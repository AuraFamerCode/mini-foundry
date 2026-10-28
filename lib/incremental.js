// ─────────────────────────────────────────────────────────────────────────────
// Incremental transforms: explicit input deltas, watermarks, semantic version
// invalidation, require-incremental gating, and key-correct output upserts.
// ─────────────────────────────────────────────────────────────────────────────
import { rid, now, PlatformError, fail, evalExpr } from './util.js';
import { store } from './store.js';
import { createDataset, commitTransaction, branchTransactionsSince } from './datasets.js';
import { assertNotRestrictedForBatch } from './security.js';

export const READ_MODES = ['added', 'removed', 'modified', 'current', 'previous'];

export function defineIncrementalTransform(spec) {
  if (!spec?.name) throw new PlatformError('BAD_TRANSFORM', 'transform needs a name');
  if (!Array.isArray(spec.inputs) || !spec.inputs.length || spec.inputs.some((name) => typeof name !== 'string' || !name)) {
    throw new PlatformError('BAD_TRANSFORM', 'transform needs one or more input dataset names');
  }
  if (new Set(spec.inputs).size !== spec.inputs.length) throw new PlatformError('BAD_TRANSFORM', 'transform inputs must be unique');
  if (!spec.output) throw new PlatformError('BAD_TRANSFORM', 'transform needs an output dataset');
  if (spec.inputs.includes(spec.output)) throw new PlatformError('BAD_TRANSFORM', 'transform output cannot also be an input');
  if (typeof spec.key !== 'string' || !spec.key) throw new PlatformError('BAD_TRANSFORM', 'incremental transforms require a stable key to support correct updates and deletions');
  if (new Set(spec.snapshotInputs ?? []).size !== (spec.snapshotInputs ?? []).length) throw new PlatformError('BAD_TRANSFORM', 'snapshotInputs must be unique');
  if ((spec.snapshotInputs ?? []).some((name) => !spec.inputs.includes(name))) throw new PlatformError('BAD_TRANSFORM', 'snapshotInputs must be selected from the declared inputs');
  if (!Number.isInteger(spec.semanticVersion ?? 1) || (spec.semanticVersion ?? 1) < 1) throw new PlatformError('BAD_TRANSFORM', 'semanticVersion must be a positive integer');
  return store.mutate((s) => {
    if (s.jobSpecs.some((j) => j.name === spec.name)) throw new PlatformError('ALREADY_EXISTS', `Transform '${spec.name}' already exists`);
    const jobSpec = {
      rid: rid('spec'), kind: 'incremental_transform', name: spec.name,
      inputs: [...spec.inputs], output: spec.output, key: spec.key,
      snapshotInputs: [...(spec.snapshotInputs ?? [])],
      semanticVersion: spec.semanticVersion ?? 1,
      requireIncremental: spec.requireIncremental ?? false,
      strictAppend: spec.strictAppend ?? false,
      v2Semantics: spec.v2Semantics ?? false,
      allowRetention: spec.allowRetention ?? false,
      expression: spec.expression ?? null,
      configure: {
        profile: spec.profile ?? null,
        allowedRunDuration: spec.allowedRunDuration ?? null,
        checkpointOutputs: spec.checkpointOutputs ?? false,
        backend: spec.backend ?? 'spark',
        lightweight: spec.lightweight ?? false,
      },
      createdAt: now(),
    };
    s.jobSpecs.push(jobSpec);
    return jobSpec;
  });
}

export function configureJobSpec(specName, patch) {
  const spec = store.state.jobSpecs.find((j) => j.name === specName);
  if (!spec) return fail('NOT_FOUND', `JobSpec '${specName}' not found`);
  if (patch.allowedRunDuration !== undefined && patch.allowedRunDuration !== null && spec.kind === 'incremental_transform') {
    return fail('CONFIGURE_INVALID', 'Do not use allowed_run_duration for incremental transforms: snapshots can run much longer than incremental deltas.');
  }
  if (patch.checkpointOutputs && spec.kind === 'incremental_transform') {
    return fail('CONFIGURE_INVALID', 'checkpoint_outputs conflicts with the transactional incremental-dataset model.');
  }
  const allowed = new Set(['profile', 'backend', 'lightweight', 'allowedRunDuration', 'checkpointOutputs']);
  const unknown = Object.keys(patch).find((key) => !allowed.has(key));
  if (unknown) return fail('CONFIGURE_INVALID', `Unknown JobSpec configuration '${unknown}'`);
  return store.mutate(() => { Object.assign(spec.configure, patch); return { ok: true, configure: spec.configure }; });
}

function rowKey(row, key, datasetName) {
  const value = row?.[key];
  if (value === undefined || value === null || value === '') throw new PlatformError('INCREMENTAL_KEY', `Row in '${datasetName}' has no value for configured incremental key '${key}'`);
  return String(value);
}

function rowMap(rows, key, datasetName) {
  const out = new Map();
  rows.forEach((row, i) => {
    const k = rowKey(row, key, datasetName);
    if (out.has(k)) throw new PlatformError('INCREMENTAL_DUPLICATE_KEY', `Dataset '${datasetName}' contains duplicate incremental key '${k}' (row ${i + 1})`);
    out.set(k, row);
  });
  return out;
}

function computeSlices(ds, lastTxRid, key, datasetName) {
  const txs = ds.branches.master.transactions;
  const cutIdx = lastTxRid ? txs.findIndex((t) => t.txRid === lastTxRid) : -1;
  const baseline = cutIdx >= 0 ? txs[cutIdx] : null;
  const previous = baseline?.rows ?? [];
  const current = txs.at(-1)?.rows ?? [];
  const prevByKey = rowMap(previous, key, datasetName);
  const currByKey = rowMap(current, key, datasetName);
  const added = [], removed = [], modified = [];
  for (const [k, row] of currByKey) {
    if (!prevByKey.has(k)) added.push(row);
    else if (JSON.stringify(prevByKey.get(k)) !== JSON.stringify(row)) modified.push(row);
  }
  for (const [k, row] of prevByKey) if (!currByKey.has(k)) removed.push(row);
  return { added, removed, modified, current, previous, changed: Boolean(baseline) };
}

function schemaForInputs(inputDatasets) {
  const canonical = (schema) => [...(schema ?? [])].map((column) => ({ name: column.name, type: column.type ?? 'string' })).sort((a, b) => a.name.localeCompare(b.name));
  const expected = canonical(inputDatasets[0]?.schema);
  for (const ds of inputDatasets.slice(1)) {
    if (JSON.stringify(canonical(ds.schema)) !== JSON.stringify(expected)) {
      throw new PlatformError('BAD_TRANSFORM_SCHEMA', `Multi-input transform inputs must have compatible schemas; '${inputDatasets[0].name}' and '${ds.name}' differ`);
    }
  }
  return inputDatasets[0]?.schema ?? [];
}

function applyTransform(spec, reads, mode) {
  const selected = [];
  if (mode === 'snapshot') {
    for (const name of spec.inputs) selected.push(...reads[name].current);
  } else {
    for (const name of spec.inputs) {
      if (spec.snapshotInputs.includes(name)) continue;
      selected.push(...reads[name].added, ...reads[name].modified);
    }
  }
  const rows = spec.expression
    ? selected.filter((row) => { try { return Boolean(evalExpr(spec.expression, row)); } catch { return false; } })
    : selected;
  return { rows };
}

function readSliceCounts(reads) {
  return Object.fromEntries(Object.entries(reads).map(([name, slice]) => [name, {
    added: slice.added.length, removed: slice.removed.length,
    modified: slice.modified.length, current: slice.current.length,
  }]));
}

export function runIncrementalTransform(specName, { force = false } = {}) {
  const spec = store.state.jobSpecs.find((j) => j.name === specName);
  if (!spec) return fail('NOT_FOUND', `JobSpec '${specName}' not found`);
  if (spec.kind !== 'incremental_transform') return fail('BAD_TRANSFORM', `'${specName}' is not an incremental transform`);

  const output = store.state.datasets.find((d) => d.name === spec.output);
  const successfulRun = [...store.state.incrementalRuns].reverse().find((r) => r.specName === specName && r.status === 'success');
  const priorWatermarks = store.state.incrementalInputWatermarks[specName] ?? successfulRun?.inputWatermarks ?? {};
  const semanticChanged = Boolean(successfulRun && successfulRun.semanticVersion !== spec.semanticVersion);
  const neverBuilt = !successfulRun || !output || !output.branches.master.transactions.length;
  const outputTip = output?.branches.master.transactions.at(-1);
  const outputAdvanced = Boolean(successfulRun && outputTip?.txRid !== successfulRun.outputTxRid);
  if (outputAdvanced && !force && !semanticChanged) return fail('OUTPUT_CONFLICT', `Output '${spec.output}' changed outside '${specName}' after its last successful run; refusing to append to a diverged output.`);
  if (!successfulRun && outputTip?.rows?.length && !force) return fail('OUTPUT_CONFLICT', `Output '${spec.output}' already contains unowned rows; use an explicit force build to replace it.`);

  let mode = 'incremental';
  const reasons = [];
  if (neverBuilt) { mode = 'snapshot'; reasons.push('output has never been built'); }
  if (semanticChanged) { mode = 'snapshot'; reasons.push(`semantic version changed (${successfulRun.semanticVersion} -> ${spec.semanticVersion})`); }
  if (force) { mode = 'snapshot'; reasons.push('snapshot explicitly requested'); }
  if (spec.requireIncremental && !neverBuilt && (force || semanticChanged)) {
    return fail('REQUIRE_INCREMENTAL', `Transform '${specName}' requires incremental execution and refuses a snapshot rebuild`, { force, semanticChanged, priorSemanticVersion: successfulRun?.semanticVersion, semanticVersion: spec.semanticVersion });
  }

  const inputDatasets = [];
  for (const name of spec.inputs) {
    const ds = store.state.datasets.find((d) => d.name === name);
    if (!ds) return fail('NOT_FOUND', `Input dataset '${name}' not found`);
    const batchCheck = assertNotRestrictedForBatch(ds);
    if (!batchCheck.ok) return batchCheck;
    if (!ds.branches?.master) return fail('BAD_TRANSFORM_INPUT', `Input '${name}' has no master branch`);
    inputDatasets.push(ds);
  }
  let outputSchema;
  try { outputSchema = schemaForInputs(inputDatasets); }
  catch (error) { return fail(error.code ?? 'BAD_TRANSFORM_SCHEMA', error.message); }
  if (!outputSchema.some((column) => column.name === spec.key)) {
    return fail('BAD_TRANSFORM_SCHEMA', `Transform key '${spec.key}' is not declared in its input schema`);
  }

  const reads = {};
  const watermarks = {};
  for (const ds of inputDatasets) {
    const snapshotInput = spec.snapshotInputs.includes(ds.name);
    const previousWatermark = priorWatermarks[ds.name] ?? null;
    reads[ds.name] = snapshotInput
      ? { added: [], removed: [], modified: [], current: ds.branches.master.transactions.at(-1)?.rows ?? [], previous: [], changed: true }
      : computeSlices(ds, previousWatermark, spec.key, ds.name);
    watermarks[ds.name] = ds.branches.master.transactions.at(-1)?.txRid ?? null;
  }

  const changedInputs = Object.entries(reads).filter(([name, slice]) =>
    !spec.snapshotInputs.includes(name) && (slice.added.length || slice.modified.length || slice.removed.length));
  const snapshotAdvanced = spec.snapshotInputs.filter((name) => watermarks[name] !== priorWatermarks[name]);
  const sourceAdvanced = Object.entries(watermarks).some(([name, txRid]) => txRid !== priorWatermarks[name]);
  const canSnapshot = neverBuilt || semanticChanged || force;
  if (mode === 'incremental' && snapshotAdvanced.length) {
    return fail('SNAPSHOT_INPUT_CHANGED', `A snapshot-only input advanced; '${specName}' needs a snapshot build to consume it.`, { inputs: snapshotAdvanced });
  }
  if (mode === 'incremental' && !changedInputs.length && !sourceAdvanced) {
    const skipped = {
      rid: rid('inc'), ts: now(), specName, mode: 'incremental', reasons: ['inputs unchanged'], status: 'skipped',
      semanticVersion: spec.semanticVersion, requireIncremental: spec.requireIncremental,
      strictAppend: spec.strictAppend, v2Semantics: spec.v2Semantics,
      snapshotInputs: [...spec.snapshotInputs], readSlices: readSliceCounts(reads),
      inputWatermarks: watermarks, outputTxRid: successfulRun?.outputTxRid ?? null,
      outputTxType: 'NONE', rowsOut: 0,
    };
    store.mutate((s) => {
      s.incrementalRuns.push(skipped);
      s.incrementalInputWatermarks[specName] = watermarks;
    });
    return { ok: true, ...skipped };
  }
  if (!canSnapshot && changedInputs.some(([, slice]) => slice.removed.length) && !spec.allowRetention) {
    return fail('RETENTION_REQUIRED', `Input removals require allowRetention or an explicitly managed deletion path; '${specName}' will not silently retain stale output rows.`);
  }
  if (mode === 'incremental' && spec.strictAppend) {
    for (const ds of inputDatasets) {
      if (spec.snapshotInputs.includes(ds.name) || watermarks[ds.name] === priorWatermarks[ds.name]) continue;
      const transactions = branchTransactionsSince(ds, 'master', priorWatermarks[ds.name]);
      if (!transactions || transactions.some((transaction) => transaction.op !== 'APPEND')) {
        return fail('STRICT_APPEND_VIOLATION', `Strict-append transform '${specName}' requires every advanced input transaction to be APPEND`, { input: ds.name });
      }
      const slice = reads[ds.name];
      if (slice.modified.length || slice.removed.length) {
        return fail('STRICT_APPEND_VIOLATION', `Strict-append input '${ds.name}' modified or removed existing keys`, { input: ds.name, modified: slice.modified.length, removed: slice.removed.length });
      }
    }
  }

  let transformed;
  try {
    transformed = applyTransform(spec, reads, mode);
    // Keyed outputs cannot represent two current rows with the same identity,
    // including overlaps across inputs, so reject rather than duplicate.
    rowMap(transformed.rows, spec.key, spec.output);
  } catch (error) { return fail(error.code ?? 'TRANSFORM_FAILED', error.message); }

  let outputRows;
  if (mode === 'snapshot') {
    outputRows = transformed.rows;
  } else {
    const previousOutput = output?.branches.master.transactions.at(-1)?.rows ?? [];
    let outByKey;
    try { outByKey = rowMap(previousOutput, spec.key, spec.output); }
    catch (error) { return fail(error.code ?? 'OUTPUT_SCHEMA', error.message); }
    const changedKeys = new Set();
    for (const [inputName, slice] of Object.entries(reads)) {
      if (spec.snapshotInputs.includes(inputName)) continue;
      for (const row of [...slice.added, ...slice.modified, ...slice.removed]) changedKeys.add(rowKey(row, spec.key, inputName));
    }
    for (const key of changedKeys) {
      const candidates = [];
      for (const inputName of spec.inputs) {
        const current = reads[inputName].current;
        candidates.push(...current.filter((row) => rowKey(row, spec.key, inputName) === key));
      }
      if (candidates.length > 1) return fail('INCREMENTAL_DUPLICATE_KEY', `Incremental key '${key}' is present in more than one current input for '${specName}'`);
      const candidate = candidates[0];
      let keep = Boolean(candidate);
      if (keep && spec.expression) {
        try { keep = Boolean(evalExpr(spec.expression, candidate)); } catch { keep = false; }
      }
      if (keep) outByKey.set(key, candidate);
      else outByKey.delete(key);
    }
    outputRows = [...outByKey.values()];
  }
  try { rowMap(outputRows, spec.key, spec.output); }
  catch (error) { return fail(error.code ?? 'OUTPUT_SCHEMA', error.message); }

  if (output && ['restricted_view', 'writeback'].includes(output.kind)) return fail('BAD_TRANSFORM_OUTPUT', `Transform '${specName}' cannot write into protected '${output.kind}' dataset '${output.name}'`);
  const inheritedMarkings = [...new Set(inputDatasets.flatMap((ds) => ds.markings ?? []))];
  let outputDataset = output;
  if (!outputDataset) {
    outputDataset = createDataset({ name: spec.output, schema: outputSchema, markings: inheritedMarkings, primaryKey: spec.key });
  } else {
    const actualSchema = outputDataset.schema ?? [];
    const canonical = (schema) => [...schema].map((column) => ({ name: column.name, type: column.type ?? 'string' })).sort((a, b) => a.name.localeCompare(b.name));
    if (!actualSchema.length && !outputDataset.branches.master.transactions.length) outputDataset.schema = structuredClone(outputSchema);
    else if (JSON.stringify(canonical(actualSchema)) !== JSON.stringify(canonical(outputSchema))) {
      return fail('BAD_TRANSFORM_SCHEMA', `Output dataset '${spec.output}' schema does not match the transform input schema`);
    }
    if (outputDataset.primaryKeyColumn && outputDataset.primaryKeyColumn !== spec.key) {
      return fail('BAD_TRANSFORM_SCHEMA', `Output dataset '${spec.output}' declares primary key '${outputDataset.primaryKeyColumn}', expected '${spec.key}'`);
    }
    outputDataset.primaryKeyColumn = spec.key;
    outputDataset.markings = [...new Set([...(outputDataset.markings ?? []), ...inheritedMarkings])];
  }

  const inputIncremental = mode === 'incremental' && changedInputs.length > 0;
  const strictAppendDelta = mode === 'incremental' && spec.strictAppend && inputIncremental;
  const txType = mode === 'snapshot' || !inputIncremental ? 'SNAPSHOT' : (spec.strictAppend ? 'APPEND' : 'UPDATE');
  if (strictAppendDelta) {
    const existing = output?.branches.master.transactions.at(-1)?.rows ?? [];
    const existingKeys = rowMap(existing, spec.key, spec.output);
    const collision = transformed.rows.find((row) => existingKeys.has(rowKey(row, spec.key, spec.output)));
    if (collision) return fail('STRICT_APPEND_VIOLATION', `Strict-append transform '${specName}' emitted an existing output key '${rowKey(collision, spec.key, spec.output)}'`);
  }
  const transactionRows = strictAppendDelta ? transformed.rows : outputRows;
  const transaction = commitTransaction(outputDataset.rid, 'master', {
    op: txType, rows: transactionRows,
    note: `${specName} ${mode} (${txType})`,
  });
  const run = {
    rid: rid('inc'), ts: now(), specName, mode, reasons, status: 'success',
    semanticVersion: spec.semanticVersion, requireIncremental: spec.requireIncremental,
    strictAppend: spec.strictAppend, v2Semantics: spec.v2Semantics,
    snapshotInputs: [...spec.snapshotInputs], readSlices: readSliceCounts(reads),
    inputReadModes: Object.fromEntries(spec.inputs.map((name) => [name, spec.snapshotInputs.includes(name) ? 'current' : mode === 'snapshot' ? 'current' : 'added+modified+removed'])),
    engine: spec.configure.lightweight ? 'lightweight (single-node)' : 'spark (distributed)',
    outputTxType: txType, rowsOut: transformed.rows.length,
    outputTxRid: transaction.txRid, inputWatermarks: watermarks,
  };
  store.mutate((s) => {
    s.incrementalRuns.push(run);
    s.incrementalInputWatermarks[specName] = watermarks;
  });
  return { ok: true, ...run };
}
