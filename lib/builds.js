// Build, JobSpec and Schedule orchestration.
import { rid, now, PlatformError, fail, evalExpr } from './util.js';
import { store } from './store.js';
import { runIncrementalTransform } from './incremental.js';
import { runPipeline } from './pipelines.js';

export function registerPipelineSpec({ name, pipelineRid }) {
  const pipeline = store.state.pipelines.find((p) => p.rid === pipelineRid);
  if (!pipeline) throw new PlatformError('NOT_FOUND', `Pipeline '${pipelineRid}' not found`);
  return store.mutate((s) => {
    if (s.jobSpecs.some((spec) => spec.name === name)) throw new PlatformError('ALREADY_EXISTS', `JobSpec '${name}' already exists`);
    const inputs = pipeline.steps.filter((step) => step.op === 'select').map((step) => step.dataset);
    const spec = { rid: rid('spec'), kind: 'pipeline', name, pipelineRid, inputs: [...new Set(inputs)], output: pipeline.outputDataset, configure: { profile: pipeline.profile, backend: 'spark', lightweight: false } };
    s.jobSpecs.push(spec);
    return spec;
  });
}

export function defineJobSpec(spec) { return spec; }

export function defineSchedule({ name, targets, trigger, maxRetries = 3, buildType = 'connecting' }) {
  if (!name || !targets?.length) throw new PlatformError('BAD_SCHEDULE', 'schedule needs a name and target JobSpecs');
  const t = trigger ?? { kind: 'any', components: [{ type: 'time', cron: '*/5 * * * *' }] };
  if (!['any', 'all', 'advanced'].includes(t.kind)) throw new PlatformError('BAD_SCHEDULE', `unknown trigger composition '${t.kind}'`);
  if (!t.components?.length) throw new PlatformError('BAD_SCHEDULE', 'schedule trigger needs components');
  if (t.kind === 'advanced' && !t.expression) throw new PlatformError('BAD_SCHEDULE', 'advanced trigger needs an expression');
  for (const target of targets) if (!store.state.jobSpecs.some((spec) => spec.name === target)) throw new PlatformError('NOT_FOUND', `Target JobSpec '${target}' not found`);
  return store.mutate((s) => {
    if (s.schedules.some((schedule) => schedule.name === name)) throw new PlatformError('ALREADY_EXISTS', `Schedule '${name}' already exists`);
    const schedule = { rid: rid('sched'), name, targets: [...targets], trigger: t, buildType, maxRetries, state: 'idle', lastRun: null, pendingReRun: false, runs: 0, createdAt: now() };
    s.schedules.push(schedule);
    return schedule;
  });
}

export function evaluateTrigger(trigger, context = {}) {
  const component = (item) => item.type === 'event'
    ? Boolean((context.updatedDatasets ?? []).includes(item.dataset))
    : item.type === 'time' ? Boolean(context.timeDue ?? true) : false;
  const components = trigger.components ?? [];
  if (trigger.kind === 'any') return components.some(component);
  if (trigger.kind === 'all') return components.every(component);
  if (trigger.kind === 'advanced') {
    const scope = {};
    components.forEach((item, i) => { scope[String.fromCharCode(65 + i)] = component(item); });
    try { return Boolean(evalExpr(trigger.expression, scope)); } catch { return false; }
  }
  return false;
}

function dependencyOrder(specs, allSpecs = specs) {
  const producerOf = new Map();
  for (const spec of allSpecs) if (spec.output) producerOf.set(spec.output, spec);
  const byName = new Map(allSpecs.map((spec) => [spec.name, spec]));
  const deps = (spec) => [...new Set((spec.inputs ?? []).map((input) => producerOf.get(input)).filter(Boolean).map((producer) => producer.name))];
  const ordered = [], seen = new Set(), stack = new Set();
  const visit = (spec) => {
    if (seen.has(spec.name)) return;
    if (stack.has(spec.name)) throw new PlatformError('CYCLE', `dependency cycle at '${spec.name}'`);
    stack.add(spec.name);
    for (const dependency of deps(spec)) visit(byName.get(dependency));
    stack.delete(spec.name);
    seen.add(spec.name);
    ordered.push(spec);
  };
  for (const spec of specs) visit(spec);
  return { order: ordered, depsOf: Object.fromEntries(specs.map((spec) => [spec.name, deps(spec)])) };
}

function runJob(spec, build, attempt = 1) {
  const job = { rid: rid('job'), specName: spec.name, kind: spec.kind, buildRid: build.rid, attempt, status: 'running', startedAt: now(), retriesAllowed: build.maxRetries };
  build.jobs.push(job);
  try {
    let result;
    if (spec.kind === 'incremental_transform') result = runIncrementalTransform(spec.name, { force: build.type === 'force' });
    else if (spec.kind === 'pipeline') result = runPipeline(spec.pipelineRid, {});
    else throw new PlatformError('BAD_JOB_SPEC', `Unsupported JobSpec kind '${spec.kind}'`);
    if (!result?.ok) throw Object.assign(new Error(result?.error?.message ?? 'job failed'), { code: result?.error?.code });
    if (spec._testFailOnce && attempt === 1) throw Object.assign(new Error('simulated transient failure'), { retryable: true });
    job.status = 'success';
    job.finishedAt = now();
    job.output = { rowsOut: result.rowsOut ?? 0, outputTxRid: result.outputTxRid ?? result.txRid ?? null };
    return { ok: true, job };
  } catch (error) {
    if (error.retryable && attempt <= build.maxRetries) {
      job.status = 'retrying';
      job.note = `retryable failure: ${error.message}; relaunching attempt ${attempt + 1}`;
      return runJob(spec, build, attempt + 1);
    }
    job.status = 'failed';
    job.error = error.message;
    return { ok: false, job };
  }
}

export function runBuild({ targets, type = 'connecting', reason, maxRetries = 3 } = {}) {
  if (!targets?.length) return fail('BAD_BUILD', 'build needs target JobSpecs');
  if (!['connecting', 'force'].includes(type)) return fail('BAD_BUILD', `unknown build type '${type}'`);
  if (!Number.isInteger(maxRetries) || maxRetries < 0) return fail('BAD_BUILD', 'maxRetries must be a nonnegative integer');
  const allSpecs = store.state.jobSpecs;
  const targetSpecs = targets.map((name) => allSpecs.find((spec) => spec.name === name));
  if (targetSpecs.some((spec) => !spec)) return fail('NOT_FOUND', 'unknown target JobSpec');
  const producerOf = new Map(allSpecs.filter((spec) => spec.output).map((spec) => [spec.output, spec]));
  const selected = new Map(targetSpecs.map((spec) => [spec.name, spec]));
  const addUpstream = (spec) => {
    for (const input of spec.inputs ?? []) {
      const producer = producerOf.get(input);
      if (producer && !selected.has(producer.name)) {
        selected.set(producer.name, producer);
        addUpstream(producer);
      }
    }
  };
  // A connecting build resolves and runs its required upstream producers too;
  // force carries an explicit snapshot flag to every incremental job.
  for (const target of targetSpecs) addUpstream(target);
  let graph;
  try {    graph = dependencyOrder([...selected.values()], allSpecs); }
  catch (error) { return fail(error.code ?? 'CYCLE', error.message); }

  const consumed = new Set(graph.order.flatMap((spec) => spec.inputs ?? []));
  const conflicts = store.state.builds.filter((b) => b.status === 'running').flatMap((b) => b.jobs
    .filter((job) => job.status === 'running')
    .map((job) => ({ job, spec: allSpecs.find((candidate) => candidate.name === job.specName) }))
    .filter(({ spec }) => spec?.output && consumed.has(spec.output))
    .map(({ job, spec }) => ({ build: b.rid, job: job.specName, output: spec.output })));
  if (conflicts.length) return fail('BUILD_CONFLICT', `concurrent build(s) are rewriting required input datasets: ${conflicts.map((c) => `${c.job} -> ${c.output}`).join(', ')}`, { conflicts });

  const build = { rid: rid('build'), ts: now(), type, reason: reason ?? null, status: 'running', targets: [...targets], jobs: [], failures: [], maxRetries,
    resolution: { order: graph.order.map((spec) => spec.name), depsOf: graph.depsOf, conflictsChecked: true } };
  store.state.builds.push(build);
  const jobsByName = new Map();
  for (const spec of graph.order) {
    const failedDeps = (graph.depsOf[spec.name] ?? []).filter((name) => ['failed', 'terminated'].includes(jobsByName.get(name)?.status));
    if (failedDeps.length) {
      const job = { rid: rid('job'), specName: spec.name, kind: spec.kind, buildRid: build.rid, attempt: 0, status: 'terminated', note: `depends on failed job(s): ${failedDeps.join(', ')}` };
      build.jobs.push(job);
      jobsByName.set(spec.name, job);
      build.failures.push({ job: spec.name, error: job.note });
      continue;
    }
    const result = runJob(spec, build, 1);
    jobsByName.set(spec.name, result.job);
    if (!result.ok) build.failures.push({ job: spec.name, error: result.job.error });
  }
  build.status = build.failures.length ? 'completed_with_failures' : 'success';
  build.finishedAt = now();
  return { ok: true, buildRid: build.rid, status: build.status, type, jobs: build.jobs.map((job) => ({ spec: job.specName, status: job.status, attempt: job.attempt, note: job.note ?? null, error: job.error ?? null })), resolution: build.resolution };
}

export function triggerSchedule(name, context = {}) {
  const schedule = store.state.schedules.find((x) => x.name === name);
  if (!schedule) return fail('NOT_FOUND', `Schedule '${name}' not found`);
  if (!evaluateTrigger(schedule.trigger, context)) return { ok: true, fired: false, note: 'trigger condition not satisfied' };
  if (schedule.state === 'running') {
    schedule.pendingReRun = true;
    return { ok: true, fired: true, coalesced: true, note: 'one pending rerun already coalesced' };
  }
  const executeOnce = (reason) => {
    schedule.state = 'running';
    const result = runBuild({ targets: schedule.targets, type: schedule.buildType ?? 'connecting', reason, maxRetries: schedule.maxRetries });
    schedule.runs += 1;
    schedule.lastRun = now();
    schedule.state = 'idle';
    return result;
  };
  const first = executeOnce(`schedule ${name}`);
  let pending = null;
  // Drain at most one coalesced run. Requests produced while this run is
  // synchronous are consumed without creating an unbounded retry loop.
  if (schedule.pendingReRun) {
    schedule.pendingReRun = false;
    pending = executeOnce(`schedule ${name} (coalesced rerun)`);
    if (schedule.pendingReRun) schedule.pendingReRun = false;
  }
  return { ok: true, fired: true, coalesced: false, build: first, pendingReRun: pending };
}

export function listOrchestration() {
  const state = store.state;
  return {
    jobSpecs: state.jobSpecs.map(({ _testFailOnce, ...spec }) => ({ ...spec })),
    builds: state.builds.map((build) => ({ rid: build.rid, ts: build.ts, type: build.type, status: build.status, reason: build.reason, resolution: build.resolution, jobs: build.jobs.map((job) => ({ spec: job.specName, status: job.status, attempt: job.attempt, note: job.note ?? null, error: job.error ?? null })) })),
    schedules: state.schedules.map(({ rid: _rid, ...schedule }) => schedule),
  };
}
