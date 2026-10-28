// Ontology definitions, Funnel indexing, governed object queries, and writes.
import { rid, now, PlatformError, fail, evalExpr, compileExpr } from './util.js';
import { store } from './store.js';
import { requireDataset, branchTipRows, branchChangesSince, branchTransactionsSince, recordEditLog, commitTransaction } from './datasets.js';
import { canReadDataset, applyReadPolicies, canReadProperty } from './security.js';
import { resolveLink } from './links.js';
import { getWriteback, checkFieldSchemaCompat, checkPrimaryKeyRows, validateSchemaContract } from './physical.js';

export function defineObjectType({ name, display, primaryKey, titleKey, properties, backingDataset, sync, markings = [] }) {
  if (!name || !primaryKey || !titleKey || !Array.isArray(properties)) throw new PlatformError('BAD_OBJECT_TYPE', 'object type needs name, explicit primaryKey, explicit titleKey, and a properties array');
  return store.mutate((s) => {
    if (s.objectTypes.some((o) => o.name === name)) throw new PlatformError('ALREADY_EXISTS', `Object type '${name}' already exists`);
    if (new Set(properties.map((p) => p.name)).size !== properties.length) throw new PlatformError('BAD_OBJECT_TYPE', 'property names must be unique within an object type');
    if (properties.some((p) => !p.name || typeof p.name !== 'string')) throw new PlatformError('BAD_OBJECT_TYPE', 'every property needs a nonempty string name');
    if (properties.filter((p) => p.name === primaryKey).length !== 1) throw new PlatformError('BAD_OBJECT_TYPE', `Primary key '${primaryKey}' must identify exactly one declared property`);
    if (properties.filter((p) => p.name === titleKey).length !== 1) throw new PlatformError('BAD_OBJECT_TYPE', `Title key '${titleKey}' must identify exactly one declared property`);
    const mapped = properties.map((p) => ({ ...p, backingColumn: p.backingColumn === undefined ? p.name : p.backingColumn }));
    const pkType = mapped.find((p) => p.name === primaryKey).type ?? 'string';
    if (!['string', 'number', 'integer'].includes(pkType)) throw new PlatformError('BAD_OBJECT_TYPE', `Primary key '${primaryKey}' must use a string, number, or integer property`);

    let ds = null;
    if (backingDataset != null) {
      ds = s.datasets.find((d) => d.name === backingDataset);
      if (!ds) throw new PlatformError('NOT_FOUND', `Backing dataset '${backingDataset}' not found`);
      const problem = checkFieldSchemaCompat({ name, properties: mapped }, ds);
      if (problem) throw new PlatformError(problem.code, problem.message, { property: problem.prop });
      const pkProperty = mapped.find((p) => p.name === primaryKey);
      if (pkProperty.backingColumn === null) throw new PlatformError('BAD_OBJECT_TYPE', 'a physically-backed object type primary key must map to a backing column');
      if (mapped.find((p) => p.name === titleKey).backingColumn === null) throw new PlatformError('BAD_OBJECT_TYPE', 'a physically-backed object type title key must map to a backing column');
      const pkProblem = checkPrimaryKeyRows({ name, primaryKey, properties: mapped }, branchTipRows(ds));
      if (pkProblem) throw new PlatformError(pkProblem.code, pkProblem.message, { property: pkProblem.property, value: pkProblem.value, row: pkProblem.row });
      const pkColumn = pkProperty.backingColumn ?? pkProperty.name;
      if (ds.primaryKeyColumn && ds.primaryKeyColumn !== pkColumn) throw new PlatformError('BAD_DATASET_KEY', `Dataset '${ds.name}' already declares primary-key column '${ds.primaryKeyColumn}'`);
      ds.primaryKeyColumn = pkColumn;
      for (const pending of s.propPolicies.filter((policy) => policy.datasetRid === ds.rid)) {
        const property = mapped.find((candidate) => candidate.name === pending.property || candidate.backingColumn === pending.property);
        if (!property) continue;
        const requiredMarkings = new Set([...(property.requiredMarkings ?? []), ...(property.requiredMarking ? [property.requiredMarking] : []), ...(pending.requiredMarking ? [pending.requiredMarking] : [])]);
        property.requiredMarkings = [...requiredMarkings];
        if (pending.minClearance !== undefined) property.minClearance = Math.max(property.minClearance ?? 0, pending.minClearance);
        s.propPolicies = s.propPolicies.filter((candidate) => candidate !== pending);
      }
    }
    const objectType = {
      rid: rid('ot'), name, display: display ?? name, primaryKey, titleKey,
      titleIndependent: titleKey !== primaryKey, properties: mapped,
      markings: [...new Set([...(ds?.markings ?? []), ...(markings ?? [])])],
      backing: ds
        ? { datasetRid: ds.rid, datasetName: ds.name, mode: sync?.mode ?? 'batch', partitionKey: sync?.partitionKey, eventTime: sync?.eventTime }
        : { datasetRid: null, datasetName: null, mode: 'action-populated' },
      createdAt: now(),
    };
    if (objectType.backing.mode === 'streaming') validateStreamingModel(objectType);
    s.objectTypes.push(objectType);
    return objectType;
  });
}

function validateStreamingModel(ot) {
  if (ot.properties.length > 250) throw new PlatformError('STREAM_LIMIT', `Streaming object type '${ot.name}' is capped at 250 properties`);
  const pkColumn = ot.properties.find((p) => p.name === ot.primaryKey)?.backingColumn;
  if (!ot.backing.partitionKey || ot.backing.partitionKey !== pkColumn || !ot.backing.eventTime) {
    throw new PlatformError('STREAM_ORDER', `Streaming object type '${ot.name}' must partition by its primary-key column and declare eventTime`);
  }
}

export function defineLinkType({ name, fromType, toType, cardinality = 'many-to-one' }) {
  return store.mutate((s) => {
    if (!name || typeof name !== 'string') throw new PlatformError('BAD_LINK', 'link type needs a name');
    if (s.linkTypes.some((link) => link.name === name)) throw new PlatformError('ALREADY_EXISTS', `Link type '${name}' already exists`);
    for (const type of [fromType, toType]) if (!s.objectTypes.some((o) => o.name === type)) throw new PlatformError('NOT_FOUND', `Object type '${type}' not found for link '${name}'`);
    const link = { rid: rid('lt'), name, fromType, toType, cardinality, createdAt: now() };
    s.linkTypes.push(link);
    return link;
  });
}

function materializeObject(ot, row) {
  const object = {};
  for (const property of ot.properties) {
    const column = property.backingColumn;
    if (column !== null && Object.hasOwn(row, column ?? property.name)) object[property.name] = row[column ?? property.name];
  }
  if (object[ot.primaryKey] === undefined) throw new PlatformError('OntologyMetadata:MissingPrimaryKeyValue', `Cannot materialize '${ot.name}': primary-key value '${ot.primaryKey}' is missing`);
  return object;
}

function ensureIndex(ot) {
  let idx = store.state.indexes.find((i) => i.objectType === ot.name);
  if (!idx) {
    idx = store.mutate((s) => {
      let found = s.indexes.find((i) => i.objectType === ot.name);
      if (!found) {
        found = { rid: rid('idx'), objectType: ot.name, backingDatasetRid: ot.backing.datasetRid, syncMode: ot.backing.mode, objects: {}, sourceRows: {}, objectVersions: {}, editOverlay: {}, editLogTxRid: null, primaryKey: ot.primaryKey, nextObjectVersion: 0, projectionSignature: null, links: [], lastTxRid: null, stats: { indexedObjects: 0, lastSync: null, incrementalWrites: 0 } };
        s.indexes.push(found);
      }
      return found;
    });
  }
  let migrated = false;
  if (!idx.objects) { idx.objects = dictionary(); migrated = true; }
  if (!idx.sourceRows) { idx.sourceRows = dictionary(); migrated = true; }
  if (!idx.objectVersions) { idx.objectVersions = dictionary(); migrated = true; }
  if (!idx.editOverlay) { idx.editOverlay = dictionary(); migrated = true; }
  if (idx.editLogTxRid === undefined) { idx.editLogTxRid = null; migrated = true; }
  if (idx.nextObjectVersion === undefined) { idx.nextObjectVersion = 0; migrated = true; }
  if (!idx.primaryKey) { idx.primaryKey = ot.primaryKey; migrated = true; }
  if (idx.projectionSignature === undefined) { idx.projectionSignature = null; migrated = true; }
  if (!idx.links) { idx.links = []; migrated = true; }
  if (!idx.stats) { idx.stats = { indexedObjects: Object.keys(idx.objects).length, lastSync: null, incrementalWrites: 0 }; migrated = true; }
  for (const key of Object.keys(idx.objects)) {
    if (!Object.hasOwn(idx.objectVersions, key)) { bumpObjectVersion(idx, key); migrated = true; }
    if (!Object.hasOwn(idx.sourceRows, key)) { idx.sourceRows[key] = structuredClone(idx.objects[key]); migrated = true; }
  }
  if (migrated) store.mutate(() => {});
  return idx;
}

export function getIndex(otName) { return store.state.indexes.find((idx) => idx.objectType === otName); }

function dictionary(value) {
  return Object.assign(Object.create(null), value ?? {});
}

function bumpObjectVersion(idx, key) {
  idx.nextObjectVersion = Number.isSafeInteger(idx.nextObjectVersion) ? idx.nextObjectVersion + 1 : 1;
  const version = `v${idx.nextObjectVersion.toString(36)}`;
  idx.objectVersions[key] = version;
  return version;
}

function projectedObject(ot, row) {
  for (const property of ot.properties) {
    if (property.backingColumn === null) continue;
    const column = property.backingColumn ?? property.name;
    const problem = propertyValueProblem(row[column], property);
    if (problem) throw new PlatformError('PROPERTY_TYPE_MISMATCH', `Backing value for '${ot.name}.${property.name}' is invalid: ${problem}`);
  }
  return materializeObject(ot, row);
}

function projectionSignature(ot) {
  return JSON.stringify(ot.properties.map(({ name, type, backingColumn }) => [name, type ?? 'string', backingColumn ?? name]));
}

function applyEditOverlay(idx, key) {
  const before = idx.objects[key] ? structuredClone(idx.objects[key]) : null;
  const overlay = idx.editOverlay[key];
  const source = idx.sourceRows[key];
  if (overlay?.op === 'delete') delete idx.objects[key];
  else if (overlay?.op === 'create') idx.objects[key] = structuredClone(overlay.patch);
  else if (overlay?.op === 'modify') {
    if (source) idx.objects[key] = { ...source, ...overlay.patch };
    else delete idx.objects[key];
  } else if (source) idx.objects[key] = structuredClone(source);
  else delete idx.objects[key];
  const after = idx.objects[key] ? JSON.stringify(idx.objects[key]) : null;
  const prior = before ? JSON.stringify(before) : null;
  if (after !== prior) {
    if (after === null) delete idx.objectVersions[key];
    else bumpObjectVersion(idx, key);
  }
}

function applyEditEvent(idx, edit) {
  const key = String(edit.objectPk);
  if (!key) throw new PlatformError('BAD_EDIT_LOG', 'Edit log contains an empty object primary key');
  const previous = idx.editOverlay[key];
  if (edit.op === 'delete') idx.editOverlay[key] = { op: 'delete', patch: null };
  else if (edit.op === 'create') idx.editOverlay[key] = { op: 'create', patch: { ...(edit.patch ?? {}), [idx.primaryKey]: key } };
  else if (edit.op === 'modify') {
    if (previous?.op === 'delete') throw new PlatformError('BAD_EDIT_LOG', `Edit log modifies '${key}' after deleting it`);
    if (previous?.op === 'create') idx.editOverlay[key] = { op: 'create', patch: { ...previous.patch, ...(edit.patch ?? {}) } };
    else idx.editOverlay[key] = { op: 'modify', patch: { ...(previous?.patch ?? {}), ...(edit.patch ?? {}) } };
  } else throw new PlatformError('BAD_EDIT_LOG', `Unsupported edit-log operation '${edit.op}'`);
  applyEditOverlay(idx, key);
  return key;
}

export function funnelSyncBatch(otName) {
  const ot = store.state.objectTypes.find((x) => x.name === otName);
  if (!ot) throw new PlatformError('NOT_FOUND', `Object type '${otName}' not found`);
  if (!ot.backing?.datasetRid) return fail('BAD_SYNC', `'${otName}' is action-populated and has no backing dataset`);
  if (ot.backing.mode === 'streaming') throw new PlatformError('BAD_SYNC', `'${otName}' is streaming; use funnelStreamEvent`);
  const ds = requireDataset(ot.backing.datasetRid);
  const schemaProblem = checkFieldSchemaCompat(ot, ds);
  if (schemaProblem) throw new PlatformError(schemaProblem.code, schemaProblem.message, { property: schemaProblem.prop });
  const pkColumn = ot.properties.find((p) => p.name === ot.primaryKey)?.backingColumn ?? ot.primaryKey;
  if (ds.primaryKeyColumn && ds.primaryKeyColumn !== pkColumn) throw new PlatformError('BAD_DATASET_KEY', `Object type '${otName}' expects primary-key column '${pkColumn}', but dataset '${ds.name}' declares '${ds.primaryKeyColumn}'`);
  const branch = ds.branches.master;
  const tipTxRid = branch.transactions.at(-1)?.txRid ?? null;
  const idx = ensureIndex(ot);
  const signature = projectionSignature(ot);
  const compactChanges = idx.projectionSignature === signature && idx.primaryKey === ot.primaryKey
    ? branchChangesSince(ds, 'master', idx.lastTxRid, pkColumn)
    : null;
  const incremental = idx.lastTxRid !== null && compactChanges !== null;
  return store.transaction(() => {
  ds.primaryKeyColumn = pkColumn;
  let added = 0, updated = 0, removed = 0;

  if (incremental) {
    const changedKeys = new Set([...compactChanges.deletes, ...compactChanges.upserts.map((row) => String(row[pkColumn]))]);
    for (const key of compactChanges.deletes) {
      if (Object.hasOwn(idx.sourceRows, key)) removed++;
      delete idx.sourceRows[key];
    }
    for (const row of compactChanges.upserts) {
      if (String(row[pkColumn] ?? '') === '') throw new PlatformError('OntologyMetadata:MissingPrimaryKeyValue', `Incremental row in '${ds.name}' is missing key '${pkColumn}'`);
      const key = String(row[pkColumn]);
      const object = projectedObject(ot, row);
      if (Object.hasOwn(idx.sourceRows, key)) {
        if (JSON.stringify(idx.sourceRows[key]) !== JSON.stringify(object)) updated++;
      } else added++;
      idx.sourceRows[key] = object;
    }
    for (const key of changedKeys) applyEditOverlay(idx, key);
  } else {
    const rows = branchTipRows(ds, 'master');
    const pkProblem = checkPrimaryKeyRows(ot, rows);
    if (pkProblem) throw new PlatformError(pkProblem.code, pkProblem.message, { property: pkProblem.property, value: pkProblem.value, row: pkProblem.row });
    const previousRows = idx.sourceRows;
    const fresh = dictionary();
    for (const row of rows) {
      const object = projectedObject(ot, row);
      const key = String(object[ot.primaryKey]);
      if (Object.hasOwn(fresh, key)) throw new PlatformError('OntologyMetadata:DuplicatePrimaryKey', `Duplicate primary key '${key}' for '${otName}'`);
      fresh[key] = object;
    }
    for (const [key, object] of Object.entries(fresh)) {
      if (!Object.hasOwn(previousRows, key)) added++;
      else if (JSON.stringify(previousRows[key]) !== JSON.stringify(object)) updated++;
    }
    for (const key of Object.keys(previousRows)) if (!Object.hasOwn(fresh, key)) removed++;
    idx.sourceRows = fresh;
    idx.objects = dictionary(fresh);
    idx.editOverlay = dictionary();
    idx.editLogTxRid = null;
    for (const [key, object] of Object.entries(fresh)) {
      if (!Object.hasOwn(idx.objectVersions, key) || JSON.stringify(previousRows[key]) !== JSON.stringify(object)) bumpObjectVersion(idx, key);
    }
    idx.primaryKey = ot.primaryKey;
  }

  idx.primaryKey = ot.primaryKey;
  idx.projectionSignature = signature;
  idx.lastTxRid = tipTxRid;
  idx.stats.indexedObjects = Object.keys(idx.objects).length;
  idx.stats.lastSync = now();
  idx.stats.incrementalWrites += added + updated + removed;
  const replay = replayEditLog(otName);
  idx.stats.indexedObjects = Object.keys(idx.objects).length;
  return { ok: true, objectType: otName, added, updated, removed, indexed: idx.stats.indexedObjects, replayedEdits: replay.replayed, incremental: incremental && (added + updated + removed > 0) };
  });
}

function parseEventTime(value) {
  const numeric = Number(value);
  if (String(value).trim() !== '' && Number.isFinite(numeric)) return numeric;
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) throw new PlatformError('STREAM_ORDER', `Invalid eventTime value '${value}'`);
  return parsed;
}

export function funnelStreamEvent(otName, event) {
  const ot = store.state.objectTypes.find((x) => x.name === otName);
  if (!ot) throw new PlatformError('NOT_FOUND', `Object type '${otName}' not found`);
  if (!ot.backing?.datasetRid) throw new PlatformError('BAD_SYNC', `'${otName}' is action-populated and does not accept source events`);
  if (ot.backing.mode !== 'streaming') throw new PlatformError('BAD_SYNC', `'${otName}' is batch-backed; use funnelSyncBatch`);
  if (Buffer.byteLength(JSON.stringify(event)) > 1_000_000) throw new PlatformError('STREAM_LIMIT', 'Stream event exceeds the 1 MB record cap');
  const pkProperty = ot.properties.find((p) => p.name === ot.primaryKey);
  const pkColumn = pkProperty?.backingColumn ?? ot.primaryKey;
  if (!ot.backing.partitionKey || ot.backing.partitionKey !== pkColumn || !ot.backing.eventTime) throw new PlatformError('STREAM_ORDER', 'Streaming object type must partition on its PK backing column and configure eventTime');
  if (String(event[ot.backing.partitionKey]) !== String(event[pkColumn])) throw new PlatformError('STREAM_ORDER', 'Stream partition key must equal the object primary key');
  const eventTime = event[ot.backing.eventTime];
  if (eventTime === undefined || eventTime === null) throw new PlatformError('STREAM_ORDER', `Stream event missing eventTime '${ot.backing.eventTime}'`);
  const ts = parseEventTime(eventTime);
  const row = projectedObject(ot, event);
  const key = String(row[ot.primaryKey]);
  const idx = ensureIndex(ot);
  const previous = idx.sourceRows[key];
  if (previous && ts < idx.eventTimes?.[key]) return { ok: true, objectType: otName, applied: false, reason: 'late event' };
  return store.transaction(() => {
    idx.sourceRows[key] = row;
    idx.objects[key] = structuredClone(row);
    bumpObjectVersion(idx, key);
    idx.eventTimes ??= {};
    idx.eventTimes[key] = ts;
    applyEditOverlay(idx, key);
    idx.stats.indexedObjects = Object.keys(idx.objects).length;
    idx.stats.lastSync = now();
    idx.stats.incrementalWrites += 1;
    return { ok: true, objectType: otName, applied: true, pk: key };
  });
}

export function validateUserEdits(otName, edits, { plannedCreates = [] } = {}) {
  const ot = store.state.objectTypes.find((x) => x.name === otName);
  if (!ot) return fail('NOT_FOUND', `Object type '${otName}' not found`);
  if (ot.backing?.datasetRid && !getWriteback(otName)) return fail('WRITEBACK_NOT_CONFIGURED', `Object type '${otName}' requires a dedicated writeback dataset before user edits`);
  const idx = store.state.indexes.find((x) => x.objectType === otName);
  const existing = idx?.objects ?? {};
  const allowed = new Set(ot.properties.map((p) => p.name));
  const planned = new Set(plannedCreates.filter((entry) => entry?.objectType === otName).map((entry) => String(entry.pk)));
  const creates = new Set();
  const deleted = new Set();
  if (ot.backing?.datasetRid) {
    const schema = validateSchemaContract(otName);
    if (!schema.ok) return schema;
  }
  for (const edit of edits) {
    const key = String(edit.pk ?? '');
    if (!key) return fail('BAD_EDIT', `Edit for '${otName}' needs a primary key`);
    if (!['create', 'modify', 'delete'].includes(edit.op)) return fail('BAD_EDIT', `Unsupported edit operation '${edit.op}'`);
    const found = Object.hasOwn(existing, key);
    if (edit.op === 'create') {
      if (found || creates.has(key) || deleted.has(key)) return fail('ALREADY_EXISTS', `Object '${otName}/${key}' already exists or is created twice`);
      creates.add(key);
    } else if (!found && !planned.has(key)) return fail('NOT_FOUND', `Cannot ${edit.op} missing object '${otName}/${key}'`);
    if (deleted.has(key)) return fail('BAD_EDIT_ORDER', `No edit may target '${otName}/${key}' after it is deleted in the same transaction`);
    if (edit.op === 'delete') deleted.add(key);
    if (edit.op !== 'delete') {
      if (!edit.patch || typeof edit.patch !== 'object' || Array.isArray(edit.patch)) return fail('BAD_EDIT', `Edit for '${otName}/${key}' needs an object patch`);
      for (const [property, value] of Object.entries(edit.patch)) {
        const definition = ot.properties.find((candidate) => candidate.name === property);
        if (!definition) return fail('BAD_EDIT', `Property '${property}' is not declared on '${otName}'`);
        if (property === ot.primaryKey && String(value) !== key) return fail('PRIMARY_KEY_IMMUTABLE', `Edits cannot change primary key '${ot.primaryKey}'`);
        const mismatch = propertyValueProblem(value, definition);
        if (mismatch) return fail('PROPERTY_TYPE_MISMATCH', `Property '${property}' on '${otName}' expects '${definition.type ?? 'string'}': ${mismatch}`);
      }
    }
  }
  return { ok: true };
}

export function validateLinkEdits(linkName, rows, { plannedCreates = [] } = {}) {
  const link = store.state.linkTypes.find((x) => x.name === linkName);
  if (!link) return fail('NOT_FOUND', `Link type '${linkName}' not found`);
  if (link.backing?.kind === 'foreign_key') return fail('BAD_LINK', `FK link '${linkName}' is derived; update its source FK property instead`);
  if (link.backing?.kind === 'object_backed') return fail('BAD_LINK', `Object-backed link '${linkName}' is derived from its manifest objects; edit the manifest object and run a batch sync`);
  const sourceIndex = store.state.indexes.find((x) => x.objectType === link.fromType);
  const targetIndex = store.state.indexes.find((x) => x.objectType === link.toType);
  const planned = (type) => new Set(plannedCreates.filter((entry) => entry.objectType === type).map((entry) => String(entry.pk)));
  const plannedSources = planned(link.fromType), plannedTargets = planned(link.toType);
  const keyFor = (fromPk, toPk) => `${fromPk}\u0000${toPk}`;
  const edges = new Map((sourceIndex?.links ?? []).filter((edge) => edge.linkType === linkName).map((edge) => [keyFor(edge.fromPk, edge.toPk), { fromPk: edge.fromPk, toPk: edge.toPk }]));
  for (const row of rows) {
    if (row.op !== undefined && !['add', 'delete'].includes(row.op)) return fail('BAD_LINK', `Link edits for '${linkName}' support only add and delete operations`);
    const fromPk = String(row.fromPk ?? ''), toPk = String(row.toPk ?? '');
    if (!fromPk || !toPk) return fail('BAD_LINK', `Link '${linkName}' needs fromPk and toPk`);
    if (!Object.hasOwn(sourceIndex?.objects ?? {}, fromPk) && !plannedSources.has(fromPk)) return fail('NOT_FOUND', `Source '${link.fromType}/${fromPk}' not found`);
    if (!Object.hasOwn(targetIndex?.objects ?? {}, toPk) && !plannedTargets.has(toPk)) return fail('NOT_FOUND', `Target '${link.toType}/${toPk}' not found`);
    const pair = keyFor(fromPk, toPk);
    if (row.op === 'delete') edges.delete(pair);
    else edges.set(pair, { fromPk, toPk });
  }
  if (link.backing?.kind === 'join_table' && !store.state.linkWritebackDatasets?.some((x) => x.linkName === linkName)) return fail('WRITEBACK_NOT_CONFIGURED', `Join-table link '${linkName}' requires a dedicated writeback dataset`);
  const projected = [...edges.values()];
  if (link.cardinality === 'one-to-one' && (new Set(projected.map((edge) => edge.fromPk)).size !== projected.length || new Set(projected.map((edge) => edge.toPk)).size !== projected.length)) return fail('LINK_CARDINALITY', `Link '${linkName}' violates one-to-one cardinality`);
  if (link.cardinality === 'many-to-one' && new Set(projected.map((edge) => edge.fromPk)).size !== projected.length) return fail('LINK_CARDINALITY', `Link '${linkName}' allows only one target per source`);
  if (link.cardinality === 'one-to-many' && new Set(projected.map((edge) => edge.toPk)).size !== projected.length) return fail('LINK_CARDINALITY', `Link '${linkName}' allows only one source per target`);
  return { ok: true };
}

function propertyValueProblem(value, property) {
  if (value === null || value === undefined) return null;
  const type = property.type ?? 'string';
  if (type === 'string' && typeof value !== 'string') return `received ${typeof value}`;
  if (type === 'number' && (typeof value !== 'number' || !Number.isFinite(value))) return `received ${typeof value} value`;
  if (type === 'integer' && !Number.isInteger(value)) return `received ${typeof value} value`;
  if (type === 'boolean' && typeof value !== 'boolean') return `received ${typeof value}`;
  if (type === 'array' && !Array.isArray(value)) return `received ${typeof value}`;
  if (type === 'date' && (typeof value !== 'string' || !/^\\d{4}-\\d{2}-\\d{2}$/.test(value) || Number.isNaN(Date.parse(value)))) return 'expected an ISO date (YYYY-MM-DD)';
  if (type === 'timestamp' && (typeof value !== 'string' || !Number.isFinite(Date.parse(value)))) return 'expected an ISO timestamp';
  return null;
}

export function applyUserEdits(otName, edits, { actionType = null, plannedCreates = [], expectedVersions = {} } = {}) {
  const validation = validateUserEdits(otName, edits, { plannedCreates });
  if (!validation.ok) throw new PlatformError(validation.error.code, validation.error.message, validation.error.details);
  if (!Array.isArray(edits) || edits.some((edit) => edit.op !== 'create' && edit.op !== 'modify' && edit.op !== 'delete')) throw new PlatformError('BAD_EDIT', `Unsupported edit operation for '${otName}'`);
  const ot = store.state.objectTypes.find((x) => x.name === otName);
  const idx = ensureIndex(ot);
  return store.transaction(() => {
    const checked = new Set();
    for (const edit of edits) {
      if (edit.op === 'create') continue;
      const key = String(edit.pk);
      if (checked.has(key)) continue;
      checked.add(key);
      if (!Object.hasOwn(expectedVersions ?? {}, key)) throw new PlatformError('OBJECT_VERSION_REQUIRED', `Edit of '${otName}/${key}' requires the version observed during preflight`);
      if (idx.objectVersions[key] !== expectedVersions[key]) throw new PlatformError('OBJECT_VERSION_CONFLICT', `Object '${otName}/${key}' changed after preflight`, { objectType: otName, objectPk: key, expected: expectedVersions[key], actual: idx.objectVersions[key] ?? null });
    }

    const persisted = [];
    for (const edit of edits) {
      const key = String(edit.pk);
      if (edit.op === 'create') {
        const object = { ...(edit.patch ?? {}), [ot.primaryKey]: key };
        persisted.push({ objectPk: key, op: 'create', patch: object, ts: now(), actionType });
      } else if (edit.op === 'delete') {
        persisted.push({ objectPk: key, op: 'delete', patch: null, ts: now(), actionType });
      } else {
        persisted.push({ objectPk: key, op: 'modify', patch: edit.patch ?? {}, ts: now(), actionType });
      }
      applyEditEvent(idx, persisted.at(-1));
      if (!ot.backing?.datasetRid) {
        if (idx.objects[key]) idx.sourceRows[key] = structuredClone(idx.objects[key]);
        else delete idx.sourceRows[key];
      }
      idx.stats.incrementalWrites += 1;
    }
    idx.stats.lastSync = now();
    const editDataset = recordEditLog(otName, persisted);
    const tip = editDataset.branches.master.transactions.at(-1);
    idx.editLogTxRid = tip?.txRid ?? idx.editLogTxRid;
    idx.stats.editFlushTx = editDataset.branches.master.transactions.length;
    idx.stats.replayedEditCount = persisted.length;
    let writebackDataset = null;
    const writeback = getWriteback(otName);
    if (writeback) {
      const ds = store.state.datasets.find((x) => x.rid === writeback.datasetRid);
      if (ds) {
        const rows = persisted.map((entry) => ({ ...entry, author: actionType ?? 'direct' }));
        commitTransaction(ds.rid, 'master', { op: 'APPEND', rows, note: `writeback flush from ${actionType ?? 'direct edit'}` });
        writebackDataset = ds.name;
      }
    }
    idx.stats.indexedObjects = Object.keys(idx.objects).length;
    return { ok: true, applied: edits.length, editDataset: editDataset.name, writebackDataset };
  });
}

export function replayEditLog(otName) {
  const ot = store.state.objectTypes.find((x) => x.name === otName);
  if (!ot) throw new PlatformError('NOT_FOUND', `Object type '${otName}' not found`);
  const ds = store.state.datasets.find((x) => x.name === `edits__${otName}`);
  if (!ds) return { ok: true, replayed: 0 };
  const idx = ensureIndex(ot);
  const transactions = ds.branches.master.transactions;
  if (!transactions.length) return { ok: true, replayed: 0 };
  const afterWatermark = idx.editLogTxRid
    ? branchTransactionsSince(ds, 'master', idx.editLogTxRid)
    : null;
  let replayed = 0;
  const resetOverlay = () => {
    idx.editOverlay = dictionary();
    idx.objects = dictionary(idx.sourceRows);
  };
  const replayRows = (rows) => {
    for (const edit of rows) {
      const key = applyEditEvent(idx, edit);
      if (!ot.backing?.datasetRid) {
        if (idx.objects[key]) idx.sourceRows[key] = structuredClone(idx.objects[key]);
        else delete idx.sourceRows[key];
      }
      replayed++;
    }
  };
  if (!idx.editLogTxRid || afterWatermark === null) {
    resetOverlay();
    const tip = transactions.at(-1);
    replayRows(tip.op === 'PURGE' ? [] : (tip.rows ?? []));
    idx.editLogTxRid = tip.txRid;
  } else {
    for (const transaction of afterWatermark) {
      if (transaction.op === 'PURGE' || transaction.op === 'SNAPSHOT' || transaction.op === 'UPDATE') resetOverlay();
      if (transaction.deltaRows) replayRows(transaction.deltaRows);
      else if (transaction.op === 'PURGE') replayRows([]);
      else {
        resetOverlay();
        replayRows(transaction.rows ?? []);
      }
      idx.editLogTxRid = transaction.txRid;
    }
  }
  idx.stats.lastSync = now();
  idx.stats.indexedObjects = Object.keys(idx.objects).length;
  idx.stats.replayedEditCount = replayed;
  return { ok: true, replayed };
}

function compileWhere(where) {
  if (typeof where === 'string') {
    const expression = compileExpr(where);
    return (object) => Boolean(expression.evaluate(object));
  }
  if (!where || typeof where !== 'object' || Array.isArray(where)) {
    throw new PlatformError('BAD_QUERY', 'where must be an expression string or an equality object');
  }
  const reserved = new Set(['__proto__', 'prototype', 'constructor', '__mfExpected']);
  const clauses = Object.entries(where).map(([key, expected]) => {
    const segments = key.split('.');
    if (!segments.every((segment) => /^[A-Za-z_][A-Za-z0-9_]*$/.test(segment) && !reserved.has(segment))) {
      throw new PlatformError('BAD_QUERY', `Invalid property path '${key}' in where clause`);
    }
    return { expected, expression: compileExpr(`${key} == __mfExpected`) };
  });
  return (object) => clauses.every(({ expected, expression }) => {
    const scope = Object.assign(Object.create(null), object, { __mfExpected: expected });
    return Boolean(expression.evaluate(scope));
  });
}

export function maskProperties(user, ot, object) {
  const out = { ...object };
  for (const property of ot.properties) {
    if (!canReadProperty(user, property, ot.name).ok) out[property.name] = '[REDACTED]';
  }
  return out;
}

export function queryObjects(otName, user, { where, search, objectPk, objectPks, includeVersions = false } = {}) {
  const s = store.state;
  const ot = s.objectTypes.find((x) => x.name === otName);
  if (!ot) return fail('NOT_FOUND', `Object type '${otName}' not found`);
  const ds = ot.backing?.datasetRid ? s.datasets.find((x) => x.rid === ot.backing.datasetRid) : null;
  if (ot.backing?.datasetRid && !ds) return fail('NOT_FOUND', `Backing dataset for '${otName}' not found`);
  const effectiveDataset = ds ?? { rid: ot.rid, name: ot.name, markings: [] };
  const perm = canReadDataset(user, { ...effectiveDataset, markings: [...new Set([...(effectiveDataset.markings ?? []), ...(ot.markings ?? [])])] });
  if (!perm.ok) return perm;
  const idx = s.indexes.find((x) => x.objectType === otName);
  let rows = Object.values(idx?.objects ?? {});
  const policyDataset = { ...effectiveDataset, markings: [...new Set([...(effectiveDataset.markings ?? []), ...(ot.markings ?? [])])] };
  rows = applyReadPolicies(user, policyDataset, rows.map((o) => ({ ...o }))).rows;
  const pkProperty = ot.properties.find((p) => p.name === ot.primaryKey);
  const pkName = pkProperty?.name ?? ot.primaryKey;
  if (objectPk !== undefined) rows = rows.filter((o) => String(o[pkName]) === String(objectPk));
  if (objectPks !== undefined) {
    const wanted = new Set(objectPks.map(String));
    rows = rows.filter((o) => wanted.has(String(o[pkName])));
  }
  rows = rows.map((o) => maskProperties(user, ot, o));
  if (search) {
    const q = String(search).toLowerCase();
    rows = rows.filter((o) => Object.values(o).some((v) => String(v ?? '').toLowerCase().includes(q)));
  }
  if (where !== undefined) {
    let predicate;
    try { predicate = compileWhere(where); }
    catch (error) { return fail(error.code ?? 'BAD_QUERY', error.message); }
    rows = rows.filter((object) => { try { return predicate(object); } catch { return false; } });
  }
  const objectVersions = includeVersions
    ? Object.fromEntries(rows.map((object) => [String(object[pkName]), idx?.objectVersions?.[String(object[pkName])] ?? null]))
    : undefined;
  return { ok: true, objectType: otName, count: rows.length, objects: rows, totalIndexed: idx?.stats.indexedObjects ?? 0, ...(objectVersions ? { objectVersions } : {}) };
}

export function linkedObjects(otName, pk, linkName, user) {
  const link = store.state.linkTypes.find((x) => x.name === linkName);
  if (!link) return fail('NOT_FOUND', `Link type '${linkName}' not found`);
  if (link.fromType !== otName) return fail('BAD_LINK_DIRECTION', `Link '${linkName}' starts at '${link.fromType}', not '${otName}'`);
  const source = queryObjects(link.fromType, user, { objectPk: String(pk) });
  if (!source.ok) return source;
  if (!source.count) return fail('NOT_FOUND', `Source '${otName}/${pk}' is not visible to this identity`);
  const resolution = resolveLink(linkName, pk);
  if (!resolution.ok) return resolution;
  const targets = queryObjects(link.toType, user, { objectPks: resolution.pks });
  if (!targets.ok) return targets;
  return { ok: true, link: linkName, backing: resolution.backing, latencyTier: resolution.latencyTier, count: targets.count, objects: targets.objects };
}

export function upsertLinks(linkName, rows, { plannedCreates = [], author = 'direct' } = {}) {
  const validation = validateLinkEdits(linkName, rows, { plannedCreates });
  if (!validation.ok) throw new PlatformError(validation.error.code, validation.error.message, validation.error.details);
  const link = store.state.linkTypes.find((x) => x.name === linkName);
  if (rows.some((row) => row.op !== undefined && !['add', 'delete'].includes(row.op))) throw new PlatformError('BAD_LINK', `Link edits for '${linkName}' support only add and delete operations`);
  const idx = ensureIndex(store.state.objectTypes.find((x) => x.name === link.fromType));

  // Physical joins have batch visibility: edits enter an isolated writeback
  // log now, but the query index changes only when syncJoinTableLink runs.
  const writeback = store.state.linkWritebackDatasets?.find((x) => x.linkName === linkName);
  if (link.backing?.kind === 'join_table') {
    if (!writeback) throw new PlatformError('WRITEBACK_NOT_CONFIGURED', `Join-table link '${linkName}' requires a dedicated writeback dataset`);
    if (rows.length) {
      const ds = store.state.datasets.find((x) => x.rid === writeback.datasetRid);
      if (!ds) throw new PlatformError('NOT_FOUND', `Link writeback dataset for '${linkName}' not found`);
      const persisted = rows.map((row) => ({ linkName, fromPk: String(row.fromPk), toPk: String(row.toPk), op: row.op ?? 'add', ts: now(), author }));
      commitTransaction(ds.rid, 'master', { op: 'APPEND', rows: persisted, note: `link writeback ${linkName}` });
    }
    return { ok: true, links: idx.links.filter((edge) => edge.linkType === linkName).length, pending: rows.length, latencyTier: link.latencyTier };
  }

  store.transaction(() => {
    for (const row of rows) {
      const fromPk = String(row.fromPk), toPk = String(row.toPk);
      const exists = idx.links.some((edge) => edge.linkType === linkName && edge.fromPk === fromPk && edge.toPk === toPk);
      if (row.op === 'delete') idx.links = idx.links.filter((edge) => !(edge.linkType === linkName && edge.fromPk === fromPk && edge.toPk === toPk));
      else if (!exists) idx.links.push({ linkType: linkName, fromPk, toPk });
    }
    if (writeback && link.backing?.kind !== 'join_table' && rows.length) {
      const ds = store.state.datasets.find((dataset) => dataset.rid === writeback.datasetRid);
      if (!ds) throw new PlatformError('NOT_FOUND', `Link writeback dataset for '${linkName}' not found`);
      const persisted = rows.map((row) => ({ linkName, fromPk: String(row.fromPk), toPk: String(row.toPk), op: row.op ?? 'add', ts: now(), author }));
      commitTransaction(ds.rid, 'master', { op: 'APPEND', rows: persisted, note: `link writeback ${linkName}` });
    }
  });
  return { ok: true, links: idx.links.filter((edge) => edge.linkType === linkName).length, latencyTier: link.latencyTier ?? 'immediate' };
}

export function listOntology() {
  const s = store.state;
  return {
    objectTypes: s.objectTypes.map((o) => ({ ...structuredClone(o), indexed: s.indexes.find((i) => i.objectType === o.name)?.stats ?? null })),
    linkTypes: s.linkTypes.map((l) => ({ ...l, indexState: s.indexes.find((i) => i.objectType === l.fromType)?.links?.filter((x) => x.linkType === l.name).length ?? 0 })),
    interfaces: s.interfaces,
    interfaceImpls: s.interfaceImpls,
  };
}
