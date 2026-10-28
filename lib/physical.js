// ─────────────────────────────────────────────────────────────────────────────
// Physical object-type model — the enforcement layer where silent corruption
// would otherwise live.
//
// Mirrored constraints:
//   - Primary key and title key are distinct, both explicit. Title is purely
//     display and independent of identity ("Melissa Chang" vs employee ID).
//   - Changing the primary key is destructive by design: every previously
//     applied Action edit was indexed against the old identity scheme and
//     cannot be safely re-keyed. The change forces deletion of the edit log,
//     and the purge is returned to the caller.
//   - The dataset<->object-type schema contract is validated, not loosely
//     coerced: a backing-column type change against a mapped property throws
//     a typed error (Foundry: OntologyMetadata:IncompatibleFoundryFieldSchema
//     ForPropertyType).
//   - One object type is backed by exactly one dataset. Wide "god tables" must
//     be normalized upstream in the pipeline layer; the ontology refuses to
//     paper over a bad physical schema.
//   - Writeback is explicit: edits do not flow back into the backing dataset;
//     they persist durably into a dedicated Funnel-managed writeback dataset.
// ─────────────────────────────────────────────────────────────────────────────
import { rid, now, PlatformError, fail } from './util.js';
import { store } from './store.js';
import { createDataset, requireDataset } from './datasets.js';

const TYPE_COMPAT = {
  string: ['string'],
  number: ['number', 'integer'],
  integer: ['integer', 'number'],
  boolean: ['boolean'],
  array: ['array'],
  media_reference: ['media_reference'],
  date: ['date'],
  timestamp: ['timestamp'],
  json: ['json'],
};

/** Dataset column type vs declared property base type — validated, not coerced. */
export function checkFieldSchemaCompat(ot, ds) {
  for (const p of ot.properties) {
    if (p.backingColumn === null) continue; // action-only / derived property
    const col = (ds.schema ?? []).find((c) => c.name === (p.backingColumn ?? p.name));
    if (!col) {
      return { prop: p.name, code: 'OntologyMetadata:MissingBackingColumn', message: `Property '${p.name}' maps to backing column '${p.backingColumn ?? p.name}' which does not exist on dataset '${ds.name}'` };
    }
    const allowed = TYPE_COMPAT[p.type ?? 'string'] ?? [p.type ?? 'string'];
    if (!allowed.includes(col.type)) {
      return {
        prop: p.name,
        code: 'OntologyMetadata:IncompatibleFoundryFieldSchemaForPropertyType',
        message: `Backing column '${col.name}' is type '${col.type}' but property '${p.name}' is declared '${p.type ?? 'string'}' on '${ot.name}' — the schema contract is validated, not loosely coerced`,
      };
    }
  }
  return null;
}

/**
 * Enforce/refresh the schema contract for an object type against its backing
 * dataset. Returns a typed failure when the contract is violated.
 */
export function validateSchemaContract(otName) {
  const ot = store.state.objectTypes.find((o) => o.name === otName);
  if (!ot) return fail('NOT_FOUND', `Object type '${otName}' not found`);
  if (!ot.backing?.datasetRid) return { ok: true, objectType: otName, validated: true, backing: 'action-populated' };
  const ds = store.state.datasets.find((d) => d.rid === ot.backing.datasetRid);
  if (!ds) return fail('NOT_FOUND', `Backing dataset for '${otName}' no longer exists`);
  const problem = checkFieldSchemaCompat(ot, ds);
  if (problem) return fail(problem.code, problem.message, { property: problem.prop });
  const rows = ds.branches.master.transactions.at(-1)?.rows ?? [];
  const pkProblem = checkPrimaryKeyRows(ot, rows);
  if (pkProblem) return fail(pkProblem.code, pkProblem.message, { property: pkProblem.property, value: pkProblem.value, row: pkProblem.row });
  return { ok: true, objectType: otName, validated: true };
}

/** Validate that every physical row has a nonempty, unique object identity. */
export function checkPrimaryKeyRows(ot, rows) {
  const pkProperty = ot.properties.find((p) => p.name === ot.primaryKey);
  if (!pkProperty || pkProperty.backingColumn === null) {
    return { code: 'BAD_OBJECT_TYPE', message: `Primary key '${ot.primaryKey}' must map to a backing column` , property: ot.primaryKey };
  }
  const column = pkProperty.backingColumn ?? pkProperty.name;
  const seen = new Map();
  for (let i = 0; i < rows.length; i++) {
    const value = rows[i]?.[column];
    if (value === undefined || value === null || value === '') {
      return { code: 'OntologyMetadata:MissingPrimaryKeyValue', message: `Backing row ${i + 1} has no value for primary-key column '${column}' on '${ot.name}'`, property: ot.primaryKey, row: i };
    }
    const key = String(value);
    if (seen.has(key)) {
      return { code: 'OntologyMetadata:DuplicatePrimaryKey', message: `Backing rows ${seen.get(key) + 1} and ${i + 1} share primary-key value '${key}' for '${ot.name}'`, property: ot.primaryKey, value: key, row: i };
    }
    seen.set(key, i);
  }
  return null;
}

/** Normalize an object-type definition (title key defaulting + validation). */
export function normalizeObjectTypeDef(def) {
  const ot = { ...def };
  // Both identity and display keys are deliberate schema choices; never infer
  // one from the other at this boundary.
  if (!ot.primaryKey) throw new PlatformError('BAD_OBJECT_TYPE', 'object type needs an explicit primaryKey');
  if (!ot.titleKey) throw new PlatformError('BAD_OBJECT_TYPE', 'object type needs an explicit titleKey (display property)');
  if (!ot.properties?.some((p) => p.name === ot.titleKey)) {
    throw new PlatformError('BAD_OBJECT_TYPE', `Title key '${ot.titleKey}' is not a declared property`);
  }
  ot.titleIndependent = ot.titleKey !== ot.primaryKey;
  return ot;
}

/**
 * Destructive primary-key change: every applied Action edit was indexed
 * against the old identity scheme. Refuses to proceed without explicit
 * acknowledgment, and when acknowledged, purges the edit history (and the
 * writeback dataset's durable rows) — returning what was destroyed.
 */
export function changePrimaryKey(otName, newPk, { confirmEditLoss = false } = {}) {
  const ot = store.state.objectTypes.find((o) => o.name === otName);
  if (!ot) return fail('NOT_FOUND', `Object type '${otName}' not found`);
  if (!ot.properties.some((p) => p.name === newPk)) {
    return fail('BAD_OBJECT_TYPE', `New primary key '${newPk}' is not a declared property of '${otName}'`);
  }
  if (ot.primaryKey === newPk) return { ok: true, changed: false, note: 'primary key unchanged' };

  const editDs = store.state.datasets.find((d) => d.name === `edits__${otName}`);
  const currentRows = (dataset) => Object.values(dataset?.branches ?? {}).reduce((total, branch) => total + (branch.transactions.at(-1)?.rows.length ?? 0), 0);
  const durableEdits = currentRows(editDs);
  const writeback = getWriteback(otName);
  const writebackDs = writeback && store.state.datasets.find((d) => d.rid === writeback.datasetRid);
  const durableWritebackRows = currentRows(writebackDs);
  const pkProperty = ot.properties.find((p) => p.name === newPk);
  if (ot.backing?.datasetRid) {
    if (!pkProperty || pkProperty.backingColumn === null) {
      return fail('BAD_OBJECT_TYPE', `New primary key '${newPk}' must map to a physical column`);
    }
    const ds = store.state.datasets.find((d) => d.rid === ot.backing.datasetRid);
    if (!ds) return fail('NOT_FOUND', `Backing dataset for '${otName}' no longer exists`);
    const rows = ds.branches.master.transactions.at(-1)?.rows ?? [];
    const proposed = { ...ot, primaryKey: newPk };
    const proposedProblem = checkPrimaryKeyRows(proposed, rows);
    if (proposedProblem) return fail(proposedProblem.code, proposedProblem.message, { property: proposedProblem.property, value: proposedProblem.value, row: proposedProblem.row });
    const newPkColumn = pkProperty.backingColumn ?? pkProperty.name;
    const newPkColumnDef = ds.schema.find((column) => column.name === newPkColumn);
    const allowedKeyTypes = { string: ['string'], number: ['number', 'integer'], integer: ['integer', 'number'] };
    if (!newPkColumnDef || !(allowedKeyTypes[pkProperty.type ?? 'string'] ?? [pkProperty.type ?? 'string']).includes(newPkColumnDef.type)) {
      return fail('OntologyMetadata:IncompatibleFoundryFieldSchemaForPropertyType', `Primary key property '${newPk}' is incompatible with backing column '${newPkColumn}'`);
    }
    const schemaProblem = checkFieldSchemaCompat({ ...ot, properties: ot.properties.filter((property) => property.name !== ot.primaryKey) }, ds);
    if (schemaProblem) return fail(schemaProblem.code, schemaProblem.message, { property: schemaProblem.prop });

    // Do not silently strand physical links when the identity column changes.
    // Source datasets are immutable from the ontology layer; callers must
    // migrate the FK/join backing data first, then retry the PK change.
    const proposedKeys = new Set(rows.map((row) => String(row[newPkColumn])));
    for (const link of store.state.linkTypes.filter((item) => item.toType === otName && item.backing)) {
      const backing = link.backing;
      let references = [];
      if (backing.kind === 'foreign_key') {
        const fromType = store.state.objectTypes.find((item) => item.name === link.fromType);
        const source = fromType?.backing?.datasetRid && store.state.datasets.find((item) => item.rid === fromType.backing.datasetRid);
        const fkProperty = fromType?.properties.find((property) => property.name === backing.fkProperty);
        const fkColumn = fkProperty?.backingColumn ?? fkProperty?.name ?? backing.fkColumn;
        references = source?.branches.master.transactions.at(-1)?.rows.map((row) => row[fkColumn]).filter((value) => value !== undefined && value !== null && value !== '') ?? [];
      } else if (backing.kind === 'join_table') {
        const join = store.state.datasets.find((item) => item.name === backing.joinTableDataset);
        references = join?.branches.master.transactions.at(-1)?.rows.map((row) => row[link.fromType === otName ? backing.leftColumn : backing.rightColumn]).filter((value) => value !== undefined && value !== null && value !== '') ?? [];
      } else if (backing.kind === 'object_backed') {
        const manifestType = store.state.objectTypes.find((item) => item.name === backing.manifestObjectType);
        const manifest = manifestType?.backing?.datasetRid && store.state.datasets.find((item) => item.rid === manifestType.backing.datasetRid);
        const property = link.toType === otName ? backing.rightProperty : backing.leftProperty;
        references = manifest?.branches.master.transactions.at(-1)?.rows.map((row) => row[property]).filter((value) => value !== undefined && value !== null && value !== '') ?? [];
      }
      const dangling = references.filter((value) => !proposedKeys.has(String(value)));
      if (dangling.length) return fail('PRIMARY_KEY_REFERENCE_CONFLICT', `Cannot change '${otName}' primary key while physical link '${link.name}' contains references that do not match the proposed key values`, { link: link.name, danglingReferences: [...new Set(dangling.map(String))].slice(0, 10) });
    }
  }

  if (!confirmEditLoss) {
    return fail('PRIMARY_KEY_CHANGE_DESTRUCTIVE',
      `Changing the primary key of '${otName}' invalidates every previously applied Action edit ` +
      `(${durableEdits} durable edit(s) indexed against '${ot.primaryKey}'). They cannot be safely re-keyed. ` +
      `Re-submit with confirmEditLoss:true to acknowledge deletion of all existing edits.`,
      { durableEdits, durableWritebackRows, currentPrimaryKey: ot.primaryKey, proposedPrimaryKey: newPk });
  }

  return store.mutate((s) => {
    const localWriteback = getWriteback(otName);
    const localWritebackDs = localWriteback && s.datasets.find((d) => d.rid === localWriteback.datasetRid);
    const writebackRows = localWritebackDs?.branches.master.transactions.at(-1)?.rows.length ?? 0;
    const destroyed = { editDataset: editDs?.name ?? null, purgedEdits: durableEdits, writebackDataset: localWritebackDs?.name ?? null, purgedWritebackRows: writebackRows, indexesCleared: 0, linksCleared: 0 };
    const purgeDataset = (ds, note) => {
      if (!ds) return;
      for (const [branchName, branch] of Object.entries(ds.branches)) {
        // Explicit acknowledgment permits destruction of the incompatible
        // edit log itself; an empty tip alone would leave old patches readable
        // from transaction history and parentRows.
        branch.transactions = [{ txRid: rid('tx'), ts: now(), op: 'PURGE', rows: [], parentRows: [], note: `${note} (branch ${branchName})` }];
      }
    };
    purgeDataset(editDs, `primary key change '${ot.primaryKey}' -> '${newPk}': edit history purged`);
    purgeDataset(localWritebackDs, `primary key change '${ot.primaryKey}' -> '${newPk}': writeback history purged`);

    // PK identity changes invalidate every cached object and link touching it.
    const idx = s.indexes.find((i) => i.objectType === otName);
    if (idx) { idx.objects = {}; idx.sourceRows = {}; idx.eventTimes = {}; idx.stats.indexedObjects = 0; destroyed.indexesCleared = 1; }
    for (const linkIndex of s.indexes) {
      const before = linkIndex.links?.length ?? 0;
      linkIndex.links = (linkIndex.links ?? []).filter((row) => {
        const lt = s.linkTypes.find((l) => l.name === row.linkType);
        return !lt || (lt.fromType !== otName && lt.toType !== otName);
      });
      destroyed.linksCleared += before - linkIndex.links.length;
    }
    for (const lt of s.linkTypes) {
      if (lt.backing?.referencesPrimaryKey && lt.toType === otName) lt.backing.referencesPrimaryKey = newPk;
      if (lt.backing?.kind === 'foreign_key' && lt.fromType === otName) {
        const property = ot.properties.find((candidate) => candidate.name === lt.backing.fkProperty);
        lt.backing.fkColumn = property?.backingColumn ?? property?.name;
      }
    }
    ot.primaryKey = newPk;
    ot.titleIndependent = ot.titleKey !== newPk;
    return { ok: true, changed: true, primaryKey: newPk, destroyed };
  });
}

/**
 * One dataset per object type: the ontology refuses multi-dataset joins at
 * the type level. Normalization happens upstream in the pipeline layer; each
 * focused dataset then maps to its own object type connected via Link Types.
 */
export function assertSingleBackingDataset(otName, datasetName) {
  const ot = store.state.objectTypes.find((o) => o.name === otName);
  if (!ot) return fail('NOT_FOUND', `Object type '${otName}' not found`);
  if (ot.backing.datasetName !== datasetName) {
    return fail('ONE_DATASET_PER_OBJECT_TYPE',
      `Object type '${otName}' is already backed by '${ot.backing.datasetName}'. One object type is backed by exactly one dataset — ` +
      `normalize the wide table in the pipeline layer first, then map each focused dataset to its own object type connected via link types.`);
  }
  return { ok: true };
}

/**
 * Explicit writeback dataset: the separation between "data I ingested" and
 * "data users mutated". Edits durably persist HERE, never back into the
 * upstream source-of-truth table.
 */
export function configureWriteback(otName) {
  const ot = store.state.objectTypes.find((o) => o.name === otName);
  if (!ot) return fail('NOT_FOUND', `Object type '${otName}' not found`);
  if (!ot.backing?.datasetRid) return fail('BAD_WRITEBACK', 'action-populated object types have no ingested backing data and do not need a writeback dataset');
  return store.mutate((s) => {
    const existing = s.writebackDatasets.find((w) => w.objectType === otName);
    if (existing) return { ok: true, writeback: existing, note: 'already configured' };
    const name = `writeback__${otName}`;
    let ds = s.datasets.find((d) => d.name === name);
    if (!ds) {
      ds = createDataset({
        name,
        schema: [
          { name: 'objectPk', type: 'string' },
          { name: 'op', type: 'string' },
          { name: 'patch', type: 'json' },
          { name: 'ts', type: 'timestamp' },
          { name: 'author', type: 'string' },
          { name: 'actionType', type: 'string' },
        ],
        markings: [],
      });
      ds.managedBy = 'funnel';
      ds.kind = 'writeback';
    }
    const w = { objectType: otName, datasetRid: ds.rid, datasetName: ds.name, configuredAt: now() };
    s.writebackDatasets.push(w);
    return { ok: true, writeback: w };
  });
}

export function getWriteback(otName) {
  return store.state.writebackDatasets.find((w) => w.objectType === otName) ?? null;
}
