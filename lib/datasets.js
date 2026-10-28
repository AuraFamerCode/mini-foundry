// ─────────────────────────────────────────────────────────────────────────────
// Dataset engine — "Git for data".
// A transaction is the base primitive: an atomic change to a dataset's
// contents on a branch. Branches, builds, and rollback are policies layered
// over this one primitive (exactly as Foundry describes transactions).
// ─────────────────────────────────────────────────────────────────────────────
import { rid, now, fail, PlatformError, evalExpr } from './util.js';
import { store } from './store.js';
import { canReadDataset, applyReadPolicies, isRestrictedView, datasetMarkings } from './security.js';

export function createDataset({ name, schema, markings, kind = 'dataset', primaryKey, primaryKeyColumn }) {
  const columns = schema ?? [];
  const keyColumn = primaryKeyColumn ?? primaryKey ?? null;
  if (keyColumn !== null && (typeof keyColumn !== 'string' || !columns.some((column) => column.name === keyColumn))) {
    throw new PlatformError('BAD_DATASET_KEY', `Primary-key column '${keyColumn}' is not declared on dataset '${name}'`);
  }
  return store.mutate((s) => {
    if (s.datasets.some((d) => d.name === name)) {
      throw new PlatformError('ALREADY_EXISTS', `Dataset '${name}' already exists`);
    }
    const localMarkings = [...(markings ?? [])];
    const ds = {
      rid: rid('ds'),
      name,
      kind, // 'dataset' | 'restricted_view' | 'stream' | 'iceberg_table' | 'virtual_table'
      // Keep locally assigned classifications separate from dynamic inheritance.
      markings: localMarkings,
      localMarkings: [...localMarkings],
      primaryKeyColumn: keyColumn,
      schema: columns, // [{ name, type }]
      branches: { master: { transactions: [] } }, // branch -> { transactions }
      inputs: [],
      createdAt: now(),
    };
    s.datasets.push(ds);
    return ds;
  });
}

export function getDataset(ridOrName) {
  const s = store.state;
  return s.datasets.find((d) => d.rid === ridOrName || d.name === ridOrName);
}

export function requireDataset(ridOrName) {
  const ds = getDataset(ridOrName);
  if (!ds) throw new PlatformError('NOT_FOUND', `Dataset '${ridOrName}' not found`);
  return ds;
}

/** Never fork dependencies implicitly: builds never create branches. */
export function createBranch(dsRid, branch, from = 'master') {
  const ds = requireDataset(dsRid);
  return store.mutate(() => {
    if (ds.branches[branch]) throw new PlatformError('BRANCH_EXISTS', `Branch '${branch}' already exists on ${ds.name}`);
    if (!ds.branches[from]) throw new PlatformError('NOT_FOUND', `Source branch '${from}' does not exist`);
    // Branch ancestry is recorded for build-compatibility checks.
    ds.branches[branch] = {
      transactions: structuredClone(ds.branches[from].transactions),
      forkedFrom: from,
      createdAt: now(),
    };
    return { ok: true, branch };
  });
}

function keyedRows(rows, keyColumn, datasetName) {
  const result = new Map();
  for (const [index, row] of rows.entries()) {
    const value = row?.[keyColumn];
    if (value === undefined || value === null || value === '') {
      throw new PlatformError('BAD_TRANSACTION_KEY', `Row ${index + 1} in '${datasetName}' is missing key '${keyColumn}'`);
    }
    const key = String(value);
    if (result.has(key)) {
      throw new PlatformError('DUPLICATE_DATASET_KEY', `Transaction for '${datasetName}' contains duplicate key '${key}'`);
    }
    result.set(key, row);
  }
  return result;
}

function rowsEqual(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function snapshotChangeSet(parentRows, nextRows, keyColumn, datasetName) {
  const before = keyedRows(parentRows, keyColumn, datasetName);
  const after = keyedRows(nextRows, keyColumn, datasetName);
  const upserts = [];
  const deletes = [];
  for (const [key, row] of after) {
    if (!before.has(key) || !rowsEqual(before.get(key), row)) upserts.push(structuredClone(row));
  }
  for (const key of before.keys()) if (!after.has(key)) deletes.push(key);
  return { keyColumn, upserts, deletes };
}

/**
 * Commit a materialized branch version. APPEND accepts only newly arrived rows
 * and merges keyed datasets by their declared primary key; unkeyed datasets
 * concatenate. SNAPSHOT/UPDATE rows are complete replacements, while DELETE
 * removes the supplied keyed rows. Every transaction retains its resulting
 * immutable tip plus a compact keyed change set for downstream incremental work.
 */
export function commitTransaction(dsRid, branch, tx) {
  const ds = requireDataset(dsRid);
  return store.mutate(() => {
    if (!ds.branches[branch]) throw new PlatformError('NOT_FOUND', `Branch '${branch}' does not exist on ${ds.name}`);
    if (!tx || typeof tx !== 'object' || Array.isArray(tx)) throw new PlatformError('BAD_TRANSACTION', 'transaction must be an object');
    const b = ds.branches[branch];
    const parentRows = b.transactions.length ? b.transactions[b.transactions.length - 1].rows : [];
    const op = tx.op ?? 'SNAPSHOT';
    if (!['SNAPSHOT', 'APPEND', 'UPDATE', 'DELETE'].includes(op)) {
      throw new PlatformError('BAD_TRANSACTION', `Unsupported transaction operation '${op}'`);
    }
    const inputRows = tx.rows ?? [];
    if (!Array.isArray(inputRows)) throw new PlatformError('BAD_TRANSACTION', 'transaction rows must be an array');
    const rows = structuredClone(inputRows);
    const requestedKey = tx.primaryKeyColumn ?? tx.primaryKey ?? tx.key;
    const keyColumn = requestedKey ?? ds.primaryKeyColumn ?? null;
    if (requestedKey && ds.primaryKeyColumn && requestedKey !== ds.primaryKeyColumn) {
      throw new PlatformError('BAD_DATASET_KEY', `Dataset '${ds.name}' already uses primary-key column '${ds.primaryKeyColumn}'`);
    }
    if (keyColumn !== null && (typeof keyColumn !== 'string' || !(ds.schema ?? []).some((column) => column.name === keyColumn))) {
      throw new PlatformError('BAD_DATASET_KEY', `Primary-key column '${keyColumn}' is not declared on dataset '${ds.name}'`);
    }

    let nextRows;
    let changeSet = null;
    if (op === 'APPEND' && keyColumn) {
      const materialized = keyedRows(parentRows, keyColumn, ds.name);
      const incoming = keyedRows(rows, keyColumn, ds.name);
      const upserts = [];
      for (const [key, row] of incoming) {
        if (!materialized.has(key) || !rowsEqual(materialized.get(key), row)) upserts.push(structuredClone(row));
        materialized.set(key, row);
      }
      nextRows = [...materialized.values()];
      changeSet = { keyColumn, upserts, deletes: [] };
    } else if (op === 'APPEND') {
      nextRows = [...structuredClone(parentRows), ...rows];
    } else if (op === 'DELETE') {
      if (!keyColumn) throw new PlatformError('BAD_TRANSACTION_KEY', `DELETE on '${ds.name}' requires a declared primary-key column`);
      const materialized = keyedRows(parentRows, keyColumn, ds.name);
      const requestedDeletes = keyedRows(rows, keyColumn, ds.name);
      const deletes = [];
      for (const key of requestedDeletes.keys()) if (materialized.delete(key)) deletes.push(key);
      nextRows = [...materialized.values()];
      changeSet = { keyColumn, upserts: [], deletes };
    } else {
      nextRows = rows;
      if (keyColumn) changeSet = snapshotChangeSet(parentRows, nextRows, keyColumn, ds.name);
    }

    if (requestedKey) ds.primaryKeyColumn = requestedKey;
    const transaction = {
      txRid: rid('tx'),
      ts: now(),
      op,
      rows: nextRows,
      parentRows,
      ...(op === 'APPEND' ? { deltaRows: rows } : {}),
      ...(changeSet ? { changeSet } : {}),
      note: tx.note,
    };
    b.transactions.push(transaction);
    return { ok: true, txRid: transaction.txRid, txCount: b.transactions.length, rowCount: nextRows.length, inputRowCount: rows.length };
  });
}

/** Coalesce only transaction deltas newer than an index watermark. */
export function branchTransactionsSince(ds, branch, afterTxRid) {
  const transactions = ds.branches[branch]?.transactions;
  if (!transactions || !afterTxRid) return null;
  const baseline = transactions.findIndex((transaction) => transaction.txRid === afterTxRid);
  return baseline < 0 ? null : transactions.slice(baseline + 1);
}

/** Coalesce only transaction deltas newer than an index watermark. */
export function branchChangesSince(ds, branch, lastTxRid, keyColumn) {
  const transactions = branchTransactionsSince(ds, branch, lastTxRid);
  if (!transactions) return null;
  const upserts = new Map();
  const deletes = new Set();
  for (const transaction of transactions) {
    const changeSet = transaction.changeSet;
    if (!changeSet || changeSet.keyColumn !== keyColumn) return null;
    for (const key of changeSet.deletes) {
      upserts.delete(key);
      deletes.add(key);
    }
    for (const row of changeSet.upserts) {
      const key = String(row[keyColumn]);
      deletes.delete(key);
      upserts.set(key, row);
    }
  }
  return { upserts: [...upserts.values()], deletes: [...deletes] };
}

/** Merge child branch back into its base (data promotion). */
export function mergeBranch(dsRid, branch, into = 'master') {
  const ds = requireDataset(dsRid);
  return store.mutate(() => {
    const child = ds.branches[branch];
    const target = ds.branches[into];
    if (!child || !target) throw new PlatformError('NOT_FOUND', 'Branch not found');
    if (child.forkedFrom !== into) {
      throw new PlatformError('BRANCH_ANCESTRY', `Branch '${branch}' is not based on '${into}'`);
    }
    const tipRows = child.transactions.length
      ? child.transactions[child.transactions.length - 1].rows
      : [];
    const res = commitTransaction(dsRid, into, { op: 'SNAPSHOT', rows: tipRows, note: `merge from ${branch}` });
    return res;
  });
}

export function branchTipRows(ds, branch = 'master') {
  const b = ds.branches[branch];
  if (!b || b.transactions.length === 0) return [];
  return b.transactions[b.transactions.length - 1].rows;
}

/**
 * Read path: permission check -> row policies -> property masking.
 * The same function serves every consumer (API, Workshop, analytics, agents).
 */
export function readDataset(ridOrName, user, { branch = 'master', resolveView = true } = {}) {
  let ds = requireDataset(ridOrName);
  if (isRestrictedView(ds) && resolveView) {
    // Restricted view = source rows filtered by the view's policy expression.
    const src = requireDataset(ds.viewOf);
    const virtualSource = { ...src, markings: datasetMarkings(ds) };
    const perm = canReadDataset(user, virtualSource);
    if (!perm.ok) return perm;
    const srcRows = branchTipRows(src, branch);
    const { rows } = applyReadPolicies(user, src, srcRows);
    return { ok: true, rows: applyViewPolicy(ds, rows, user) };
  }
  const perm = canReadDataset(user, { ...ds, markings: datasetMarkings(ds) });
  if (!perm.ok) return perm;
  const rows = branchTipRows(ds, branch);
  return { ok: true, ...applyReadPolicies(user, ds, rows) };
}

function applyViewPolicy(viewDs, rows, user) {
  if (!viewDs.policy) return rows;
  return rows.filter((row) => {
    try {
      // Policy expressions reference user attributes, e.g.
      // `user.attributes.care_center == care_center`
      return Boolean(evalExpr(viewDs.policy, { ...row, user }));
    } catch {
      return false;
    }
  });
}

export function listDatasets() {
  return store.state.datasets.map((d) => ({
    rid: d.rid,
    name: d.name,
    kind: d.kind,
    markings: datasetMarkings(d),
    schema: d.schema,
    branches: Object.keys(d.branches),
    inputs: d.inputs,
    txCount: Object.fromEntries(
      Object.entries(d.branches).map(([b, v]) => [b, v.transactions.length])
    ),
  }));
}

/** Row-level edits written via Actions land as Funnel-owned edit datasets. */
export function recordEditLog(objectTypeName, edits) {
  return store.mutate((s) => {
    const name = `edits__${objectTypeName}`;
    let ds = s.datasets.find((d) => d.name === name);
    if (!ds) {
      ds = {
        rid: rid('ds'),
        name,
        kind: 'dataset',
        markings: [],
        schema: [
          { name: 'objectPk', type: 'string' },
          { name: 'op', type: 'string' },
          { name: 'patch', type: 'json' },
          { name: 'ts', type: 'timestamp' },
          { name: 'author', type: 'string' },
        ],
        branches: { master: { transactions: [] } },
        inputs: [],
        createdAt: now(),
        managedBy: 'funnel',
      };
      s.datasets.push(ds);
    }
    commitTransaction(ds.rid, 'master', { op: 'APPEND', rows: edits, note: 'user edits flush' });
    return ds;
  });
}

export { fail };
