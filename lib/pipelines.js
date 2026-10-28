// ─────────────────────────────────────────────────────────────────────────────
// Pipeline engine — declarative pipeline DSL (Pipeline Builder equivalent).
//
// Features mirrored from the research:
//  - Steps: select, filter, join, aggregate, rename, derive (computed column)
//  - Column-level provenance tracked automatically per step
//  - Spark profiles: named, discretized tiers across five families; tuning
//    discipline enforced (one variable at a time); auto-tuner only activates
//    when the job shape was stable across the last N runs
//  - Restricted views cannot feed batch transforms (documented tension)
// ─────────────────────────────────────────────────────────────────────────────
import { rid, now, fail, PlatformError, evalExpr } from './util.js';
import { store } from './store.js';
import { requireDataset, commitTransaction, branchTipRows } from './datasets.js';
import { assertNotRestrictedForBatch, computeInheritedMarkings } from './security.js';

// ── Spark profile tiers (Palantir-style named families) ──────────────────────
export const PROFILE_FAMILIES = {
  driverMemory: { key: 'driverMemory', tiers: { SMALL: '3g', MEDIUM: '6g', LARGE: '13g', EXTRA_LARGE: '27g' } },
  executorCores: { key: 'executorCores', tiers: { EXTRA_SMALL: 1, SMALL: 2, MEDIUM: 4, LARGE: 6, EXTRA_LARGE: 8 } },
  executorMemory: {
    key: 'executorMemory',
    tiers: { EXTRA_SMALL: '3g', SMALL: '6g', MEDIUM: '13g', LARGE: '27g' },
    overhead: { EXTRA_SMALL: '768m', SMALL: '1536m', MEDIUM: '2g', LARGE: '3g' },
  },
  executorMemoryOverhead: { key: 'executorMemoryOverhead', tiers: { SMALL: '1g', MEDIUM: '2g', LARGE: '4g', EXTRA_LARGE: '8g' } },
  numExecutors: { key: 'numExecutors', tiers: { ONE: 1, TWO: 2, FOUR: 4 } },
};

export const DEFAULT_PROFILE = {
  driverMemory: 'SMALL',
  executorCores: 'SMALL',
  executorMemory: 'EXTRA_SMALL',
  executorMemoryOverhead: 'SMALL',
  numExecutors: 'ONE',
};

export function resolveProfile(p) {
  return {
    'spark.driver.memory': PROFILE_FAMILIES.driverMemory.tiers[p.driverMemory],
    'spark.executor.cores': PROFILE_FAMILIES.executorCores.tiers[p.executorCores],
    'spark.executor.memory': PROFILE_FAMILIES.executorMemory.tiers[p.executorMemory],
    'spark.executor.memoryOverhead': PROFILE_FAMILIES.executorMemoryOverhead.tiers[p.executorMemoryOverhead],
    'spark.executor.instances': PROFILE_FAMILIES.numExecutors.tiers[p.numExecutors],
  };
}

export function createPipeline({ name, steps, outputDataset }) {
  for (const st of steps ?? []) validateStep(st);
  return store.mutate((s) => {
    const p = {
      rid: rid('pipe'),
      name,
      steps: steps ?? [],
      outputDataset: outputDataset ?? null,
      profile: { ...DEFAULT_PROFILE },
      profileHistory: [],
      createdAt: now(),
    };
    s.pipelines.push(p);
    return p;
  });
}

function validateStep(st) {
  const needs = { select: ['dataset'], select_columns: ['columns'], filter: ['expr'], join: ['otherDataset', 'on'], aggregate: ['groupBy', 'measures'], derive: ['name', 'expr'], rename: ['from', 'to'] };
  const req = needs[st.op];
  if (!req) throw new PlatformError('BAD_STEP', `Unknown step op '${st.op}'`);
  for (const k of req) if (st[k] === undefined) throw new PlatformError('BAD_STEP', `Step '${st.op}' missing '${k}'`);
}

/**
 * Run a pipeline: pull branch-tip rows from each input dataset, apply steps,
 * commit the result as a transaction on the output dataset, and record
 * column-level provenance for every output column.
 */
export function runPipeline(pipelineRid, { branch = 'master', usage, user } = {}) {
  const p = store.state.pipelines.find((x) => x.rid === pipelineRid);
  if (!p) throw new PlatformError('NOT_FOUND', `Pipeline '${pipelineRid}' not found`);

  const steps = p.steps;
  const first = steps[0];
  if (!first || first.op !== 'select' || !first.dataset) {
    throw new PlatformError('BAD_PIPELINE', 'Pipeline must start with a select step naming an input dataset');
  }

  // A pipeline may read its initial selection and every join input. Check the
  // full dependency set before reading any raw rows or executing any step.
  const dependencies = new Map();
  const registerBatchInput = (name) => {
    const dataset = requireDataset(name);
    const guard = assertNotRestrictedForBatch(dataset);
    if (!guard.ok) throw new PlatformError(guard.error.code, guard.error.message, guard.error.details);
    dependencies.set(name, dataset);
  };
  for (let index = 0; index < steps.length; index++) {
    const step = steps[index];
    if (step.op === 'select') {
      if (index !== 0) throw new PlatformError('BAD_PIPELINE', 'select is only valid as step 0');
      registerBatchInput(step.dataset);
    } else if (step.op === 'join') registerBatchInput(step.otherDataset);
  }
  const inputDs = dependencies.get(first.dataset);

  // Resolve input rows through the security engine? No — batch compute runs
  // against the *dataset*, not the user's view of it. Foundry's answer is to
  // disallow restricted views entirely (checked above), so raw rows are OK.
  let rows = structuredClone(branchTipRows(inputDs, branch));
  const provenance = []; // [{ outputColumn, steps: [...] }]
  const track = (col, stepIdx, detail) => provenance.push({ column: col, step: stepIdx, detail });

  let schemaOut = inputDs.schema.map((c) => ({ ...c }));

  for (let i = 0; i < steps.length; i++) {
    const st = steps[i];
    switch (st.op) {
      case 'select': {
        const src = requireDataset(st.dataset);
        if (i > 0) throw new PlatformError('BAD_PIPELINE', 'select is only valid as step 0');
        for (const c of schemaOut) track(c.name, i, `source column from ${src.name}`);
        break;
      }
      case 'filter':
        rows = rows.filter((r) => { try { return Boolean(evalExpr(st.expr, r)); } catch { return false; } });
        break;
      case 'select_columns':
        schemaOut = st.columns.map((c) => schemaOut.find((x) => x.name === c)).filter(Boolean);
        rows = rows.map((r) => Object.fromEntries(st.columns.filter((c) => c in r).map((c) => [c, r[c]])));
        for (const c of st.columns) track(c, i, 'column kept by select_columns');
        break;
      case 'rename':
        rows = rows.map((r) => { const { [st.from]: v, ...rest } = r; return { ...rest, [st.to]: v }; });
        schemaOut = schemaOut.map((c) => (c.name === st.from ? { ...c, name: st.to } : c));
        track(st.to, i, `renamed from '${st.from}'`);
        break;
      case 'derive': {
        const sample = rows[0] ?? {};
        const evalOne = (r) => { try { return evalExpr(st.expr, r); } catch { return null; } };
        const sampleValue = evalOne(sample);
        const t = typeof sampleValue === 'number' ? 'number' : typeof sampleValue === 'boolean' ? 'boolean' : 'string';
        rows = rows.map((r) => ({ ...r, [st.name]: evalOne(r) }));
        schemaOut.push({ name: st.name, type: t });
        track(st.name, i, `derived via \`${st.expr}\``);
        break;
      }
      case 'join': {
        const other = dependencies.get(st.otherDataset);
        rows = joinRows(rows, structuredClone(branchTipRows(other, branch)), st.on, st.how ?? 'inner');
        const otherCols = other.schema.map((c) => ({ ...c, name: c.name }));
        for (const c of otherCols) {
          if (!schemaOut.some((x) => x.name === c.name)) schemaOut.push(c);
          track(c.name, i, `joined from ${other.name} on '${st.on}'`);
        }
        break;
      }
      case 'aggregate': {
        const groups = new Map();
        for (const r of rows) {
          const key = JSON.stringify(st.groupBy.map((g) => r[g]));
          if (!groups.has(key)) groups.set(key, { group: Object.fromEntries(st.groupBy.map((g) => [g, r[g]])), rows: [] });
          groups.get(key).rows.push(r);
        }
        rows = [...groups.values()].map(({ group, rows: rs }) => {
          const out = { ...group };
          for (const m of st.measures) {
            out[m.as] = computeMeasure(rs, m);
          }
          return out;
        });
        schemaOut = [
          ...st.groupBy.map((g) => schemaOut.find((c) => c.name === g)).filter(Boolean),
          ...st.measures.map((m) => ({ name: m.as, type: 'number' })),
        ];
        for (const m of st.measures) track(m.as, i, `${m.fn}(${m.column ?? '*'}) grouped by ${st.groupBy.join(', ')}`);
        break;
      }
      default:
        throw new PlatformError('BAD_STEP', `Unknown op ${st.op}`);
    }
  }

  // Output dataset: create if needed; inherit markings from inputs.
  const outName = steps.find((s) => s.op === 'write')?.dataset ?? p.outputDataset;
  if (!outName) throw new PlatformError('BAD_PIPELINE', 'Pipeline needs an output: a write step or outputDataset');
  const inputRids = [...new Set([...dependencies.values()].map((dataset) => dataset.rid))];
  let outDs = store.state.datasets.find((d) => d.name === outName);
  if (!outDs) {
    outDs = store.mutate((s) => {
      const ds = {
        rid: rid('ds'), name: outName, kind: 'dataset',
        markings: computeInheritedMarkings({ markings: [], localMarkings: [], inputs: inputRids }, s.datasets),
        localMarkings: [],
        schema: schemaOut,
        branches: { master: { transactions: [] } },
        inputs: inputRids,
        createdAt: now(),
      };
      s.datasets.push(ds);
      return ds;
    });
  }

  const tx = commitTransaction(outDs.rid, branch, { rows, note: `pipeline ${p.name} run`, op: 'SNAPSHOT' });

  // Marking propagation is recomputed on each run.
  store.mutate((s) => {
    if (outDs.localMarkings === undefined) {
      const isAlreadyDerived = (outDs.inputs ?? []).some((rid) => s.datasets.some((dataset) => dataset.rid === rid));
      outDs.localMarkings = isAlreadyDerived ? [] : [...(outDs.markings ?? [])];
    }
    outDs.inputs = inputRids;
    outDs.markings = computeInheritedMarkings({ ...outDs, localMarkings: outDs.localMarkings, inputs: inputRids }, s.datasets);
    outDs.schema = schemaOut;
  });

  // Spark profile: explicit profile (if provided) vs auto-tuner decision.
  const profileResult = decideProfile(p, usage);

  store.mutate((s) => {
    s.pipelineRuns.push({
      rid: rid('run'),
      ts: now(),
      pipelineRid: p.rid,
      pipeline: p.name,
      branch,
      input: inputDs.name,
      output: outDs.name,
      rowsIn: branchTipRows(inputDs, branch).length,
      rowsOut: rows.length,
      profile: profileResult.profile,
      profileSource: profileResult.source,
      profileNote: profileResult.note,
      usage: profileResult.usage,
      txRid: tx.txRid,
      provenance,
    });
  });

  return {
    ok: true,
    output: { rid: outDs.rid, name: outDs.name },
    rowsOut: rows.length,
    txRid: tx.txRid,
    provenance,
    profile: profileResult,
  };
}

function computeMeasure(rs, m) {
  const vals = m.column ? rs.map((r) => Number(r[m.column])).filter((v) => !Number.isNaN(v)) : rs.map(() => 1);
  switch (m.fn) {
    case 'count': return rs.length;
    case 'sum': return vals.reduce((a, b) => a + b, 0);
    case 'avg': return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : 0;
    case 'min': return vals.length ? Math.min(...vals) : 0;
    case 'max': return vals.length ? Math.max(...vals) : 0;
    default: throw new PlatformError('BAD_STEP', `Unknown measure fn '${m.fn}'`);
  }
}

function joinRows(left, right, on, how) {
  const index = new Map();
  for (const r of right) {
    const k = JSON.stringify(r[on]);
    if (!index.has(k)) index.set(k, []);
    index.get(k).push(r);
  }
  const out = [];
  for (const l of left) {
    const matches = index.get(JSON.stringify(l[on])) ?? [];
    if (matches.length === 0 && how === 'left') { out.push({ ...l }); continue; }
    for (const r of matches) out.push({ ...l, ...r });
  }
  return out;
}

// ── Spark profile decision: explicit bumps + historical auto-tuner ──────────
function decideProfile(p, usage) {
  // usage: { driverMemoryGb, executorMemoryGb, executorCores, numExecutors }
  const profile = { ...p.profile };
  let source = 'explicit';
  let note = 'using configured profile';

  const history = store.state.pipelineRuns
    .filter((r) => r.pipelineRid === p.rid)
    .slice(-5);

  // Auto-tuner precondition: last 5 runs must share the identical profile
  // shape AND report usage — never optimize against a drifting job.
  const stable =
    history.length === 5 &&
    history.every((h) => JSON.stringify(h.profile) === JSON.stringify(p.profile)) &&
    history.every((h) => h.usage);

  if (usage && stable) {
    // Per-dimension: max observed across last 5 runs, capped at the request.
    const observed = {};
    for (const dim of ['driverMemoryGb', 'executorMemoryGb', 'executorCores', 'numExecutors']) {
      observed[dim] = Math.max(...history.map((h) => Number(h.usage?.[dim] ?? 0)));
    }
    const requested = {
      driverMemoryGb: gb(PROFILE_FAMILIES.driverMemory.tiers[profile.driverMemory]),
      executorMemoryGb: gb(PROFILE_FAMILIES.executorMemory.tiers[profile.executorMemory]),
      executorCores: PROFILE_FAMILIES.executorCores.tiers[profile.executorCores],
      numExecutors: PROFILE_FAMILIES.numExecutors.tiers[profile.numExecutors],
    };
    const bumps = [];
    const tryBump = (family, dim) => {
      const tiers = Object.keys(family.tiers);
      const idx = tiers.indexOf(profile[family.key]);
      if (idx >= 0 && idx < tiers.length - 1 && observed[dim] > requested[dim]) {
        profile[family.key] = tiers[idx + 1];
        bumps.push(`${family.key} -> ${profile[family.key]}`);
      }
    };
    tryBump(PROFILE_FAMILIES.driverMemory, 'driverMemoryGb');
    tryBump(PROFILE_FAMILIES.executorMemory, 'executorMemoryGb');
    tryBump(PROFILE_FAMILIES.executorCores, 'executorCores');
    tryBump(PROFILE_FAMILIES.numExecutors, 'numExecutors');
    if (bumps.length) {
      source = 'auto-tuner';
      note = `observed maxima over last 5 stable runs exceeded request: ${bumps.join('; ')}`;
    } else {
      note = 'observed usage within request across last 5 stable runs — no change';
    }
  }

  return {
    profile,
    resolved: resolveProfile(profile),
    source,
    note,
    usage: usage ?? null,
  };
}

function gb(str) {
  const m = /(\d+)g/.exec(str ?? '');
  return m ? Number(m[1]) : 0;
}

/** One-variable-at-a-time discipline: reject multi-family changes per edit. */
export function updateProfile(pipelineRid, patch) {
  const p = store.state.pipelines.find((x) => x.rid === pipelineRid);
  if (!p) throw new PlatformError('NOT_FOUND', 'Pipeline not found');
  const changedFamilies = Object.keys(patch ?? {});
  if (changedFamilies.length !== 1) {
    return fail(
      'PROFILE_ONE_VARIABLE',
      `Spark profiles are tuned one family at a time (tried changing: ${changedFamilies.join(', ')}). ` +
      `Adjust executor memory, run the job, then tune the next dimension.`
    );
  }
  return store.mutate(() => {
    p.profileHistory.push({ ts: now(), from: { ...p.profile }, to: { ...p.profile, ...patch } });
    p.profile = { ...p.profile, ...patch };
    return { ok: true, profile: p.profile, resolved: resolveProfile(p.profile) };
  });
}
