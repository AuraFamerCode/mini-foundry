// Link type physical storage and propagation. FK links resolve from the live
// source object; join-table/object-backed links are indexed only on sync.
import { rid, now, PlatformError, fail } from './util.js';
import { store } from './store.js';
import { createDataset, commitTransaction, getDataset, branchTipRows } from './datasets.js';

function requireUniqueName(name) {
  if (!name) throw new PlatformError('BAD_LINK', 'link type needs a name');
  if (store.state.linkTypes.some((l) => l.name === name)) throw new PlatformError('ALREADY_EXISTS', `Link type '${name}' already exists`);
}

export function defineLinkTypePhysical({ name, fromType, toType, cardinality = 'many-to-one', backing }) {
  requireUniqueName(name);
  const s = store.state;
  const fromOt = s.objectTypes.find((o) => o.name === fromType);
  const toOt = s.objectTypes.find((o) => o.name === toType);
  if (!fromOt || !toOt) throw new PlatformError('NOT_FOUND', `Both object types must exist for link '${name}'`);
  const b = backing ?? {};
  let resolved = { kind: b.kind ?? 'foreign_key' };

  if (resolved.kind === 'foreign_key') {
    if (!['one-to-one', 'many-to-one'].includes(cardinality)) throw new PlatformError('BAD_LINK', 'A foreign-key property on the source type supports many-to-one or one-to-one cardinality');
    const fk = fromOt.properties.find((p) => p.name === b.fkProperty);
    if (!fk) throw new PlatformError('BAD_LINK', `fkProperty '${b.fkProperty}' is not a property of '${fromType}'`);
    const pk = toOt.properties.find((p) => p.name === toOt.primaryKey);
    if (!pk) throw new PlatformError('BAD_LINK', `Target primary key '${toOt.primaryKey}' is not declared on '${toType}'`);
    if ((fk.type ?? 'string') !== (pk.type ?? 'string') && !(['number', 'integer'].includes(fk.type) && ['number', 'integer'].includes(pk.type))) throw new PlatformError('BAD_LINK_SCHEMA', `FK property '${b.fkProperty}' and target PK '${toOt.primaryKey}' have incompatible ontology types`);
    if (fromOt.backing.datasetRid) {
      const ds = s.datasets.find((d) => d.rid === fromOt.backing.datasetRid);
      const col = ds?.schema.find((c) => c.name === (fk.backingColumn ?? fk.name));
      if (!col || fk.backingColumn === null) throw new PlatformError('BAD_LINK', `FK property '${b.fkProperty}' must map to a backing column`);
      if (col.type !== (pk.type ?? 'string') && !(['number', 'integer'].includes(col.type) && ['number', 'integer'].includes(pk.type ?? 'string'))) throw new PlatformError('BAD_LINK_SCHEMA', `FK backing column '${col.name}' and target PK '${toOt.primaryKey}' have incompatible physical types`);
    }
    resolved = { ...resolved, fkProperty: b.fkProperty, fkColumn: fk.backingColumn ?? fk.name, referencesPrimaryKey: toOt.primaryKey };
  } else if (resolved.kind === 'join_table') {
    if (!b.joinTableDataset) throw new PlatformError('BAD_LINK', `join-table link '${name}' needs joinTableDataset`);
    if (cardinality !== 'many-to-many') throw new PlatformError('BAD_LINK', 'join-table backing requires many-to-many cardinality');
    resolved = { ...resolved, joinTableDataset: b.joinTableDataset, leftColumn: b.leftColumn ?? `${fromType.toLowerCase()}_pk`, rightColumn: b.rightColumn ?? `${toType.toLowerCase()}_pk` };
    const ds = getDataset(resolved.joinTableDataset);
    if (ds) validateJoinSchema(ds, resolved, fromOt, toOt);
  } else if (resolved.kind === 'object_backed') {
    if (!b.manifestObjectType) throw new PlatformError('BAD_LINK', `object-backed link '${name}' needs manifestObjectType`);
    const manifest = s.objectTypes.find((o) => o.name === b.manifestObjectType);
    if (!manifest) throw new PlatformError('NOT_FOUND', `Manifest object type '${b.manifestObjectType}' not found`);
    resolved = { ...resolved, manifestObjectType: b.manifestObjectType, leftProperty: b.leftProperty ?? `${fromType.toLowerCase()}_pk`, rightProperty: b.rightProperty ?? `${toType.toLowerCase()}_pk` };
    const left = manifest.properties.find((p) => p.name === resolved.leftProperty);
    const right = manifest.properties.find((p) => p.name === resolved.rightProperty);
    if (!left || !right) throw new PlatformError('BAD_LINK', `Object-backed link properties must be declared on '${manifest.name}'`);
    const fromPk = fromOt.properties.find((p) => p.name === fromOt.primaryKey);
    const toPk = toOt.properties.find((p) => p.name === toOt.primaryKey);
    if ((left.type ?? 'string') !== (fromPk.type ?? 'string') || (right.type ?? 'string') !== (toPk.type ?? 'string')) throw new PlatformError('BAD_LINK_SCHEMA', `Manifest endpoint properties on '${manifest.name}' must match the endpoint primary-key types`);
  } else throw new PlatformError('BAD_LINK', `Unknown link backing kind '${resolved.kind}'`);

  return store.mutate((state) => {
    const link = { rid: rid('lt'), name, fromType, toType, cardinality, backing: resolved, latencyTier: resolved.kind === 'foreign_key' ? 'derived-instant' : 'batch-cadence', createdAt: now() };
    state.linkTypes.push(link);
    return link;
  });
}

function validateJoinSchema(ds, backing, fromOt, toOt) {
  const endpoints = [
    [backing.leftColumn, fromOt],
    [backing.rightColumn, toOt],
  ];
  for (const [colName, objectType] of endpoints) {
    const column = ds.schema.find((candidate) => candidate.name === colName);
    if (!column) throw new PlatformError('BAD_LINK_SCHEMA', `Join dataset '${ds.name}' lacks mapped key column '${colName}'`);
    const key = objectType?.properties.find((property) => property.name === objectType.primaryKey);
    const propertyType = key?.type ?? 'string';
    if (column.type !== propertyType && !(['number', 'integer'].includes(column.type) && ['number', 'integer'].includes(propertyType))) throw new PlatformError('BAD_LINK_SCHEMA', `Join dataset '${ds.name}' column '${colName}' is '${column.type}' but '${objectType?.name}' primary key is '${propertyType}'`);
  }
}

export function generateJoinTable(linkName) {
  const lt = store.state.linkTypes.find((l) => l.name === linkName);
  if (!lt) return fail('NOT_FOUND', `Link type '${linkName}' not found`);
  if (lt.backing?.kind !== 'join_table') return fail('BAD_LINK', `'${linkName}' is not join-table backed`);
  const fromOt = store.state.objectTypes.find((o) => o.name === lt.fromType);
  const toOt = store.state.objectTypes.find((o) => o.name === lt.toType);
  let ds = getDataset(lt.backing.joinTableDataset);
  if (!ds) {
    const fromType = fromOt.properties.find((property) => property.name === fromOt.primaryKey)?.type ?? 'string';
    const toType = toOt.properties.find((property) => property.name === toOt.primaryKey)?.type ?? 'string';
    ds = createDataset({ name: lt.backing.joinTableDataset, schema: [{ name: lt.backing.leftColumn, type: fromType }, { name: lt.backing.rightColumn, type: toType }], markings: [...new Set([...(fromOt.markings ?? []), ...(toOt.markings ?? [])])] });
  }
  validateJoinSchema(ds, lt.backing, fromOt, toOt);
  return { ok: true, dataset: ds.name, schema: ds.schema, note: `pairs of ${fromOt.primaryKey} -> ${toOt.primaryKey}; visible after a Funnel batch sync` };
}

export function configureLinkWriteback(linkName) {
  const lt = store.state.linkTypes.find((l) => l.name === linkName);
  if (!lt) return fail('NOT_FOUND', `Link type '${linkName}' not found`);
  if (lt.backing?.kind !== 'join_table') return fail('BAD_LINK', 'only join-table links use a dedicated link writeback dataset');
  return store.mutate((s) => {
    let record = s.linkWritebackDatasets.find((x) => x.linkName === linkName);
    if (record) return { ok: true, writeback: record };
    const datasetName = `writeback__link__${linkName}`;
    let ds = getDataset(datasetName);
    if (!ds) {
      ds = createDataset({ name: datasetName, schema: [{ name: 'fromPk', type: 'string' }, { name: 'toPk', type: 'string' }, { name: 'op', type: 'string' }, { name: 'ts', type: 'timestamp' }, { name: 'author', type: 'string' }], markings: [] });
      ds.managedBy = 'ontology';
      ds.kind = 'writeback';
    }
    record = { linkName, datasetRid: ds.rid, datasetName, configuredAt: now() };
    s.linkWritebackDatasets.push(record);
    return { ok: true, writeback: record };
  });
}

export function syncJoinTableLink(linkName) {
  const s = store.state;
  const lt = s.linkTypes.find((l) => l.name === linkName);
  if (!lt) return fail('NOT_FOUND', `Link type '${linkName}' not found`);
  if (lt.backing?.kind === 'foreign_key') return fail('BAD_LINK', `'${linkName}' is FK-derived; it requires no sync`);
  let pairs;
  let sourceTxRid = null;
  if (lt.backing.kind === 'join_table') {
    const ds = getDataset(lt.backing.joinTableDataset);
    if (!ds) return fail('NOT_FOUND', `Join table dataset '${lt.backing.joinTableDataset}' not found`);
    const fromOt = s.objectTypes.find((o) => o.name === lt.fromType);
    const toOt = s.objectTypes.find((o) => o.name === lt.toType);
    validateJoinSchema(ds, lt.backing, fromOt, toOt);
    const tx = ds.branches.master.transactions.at(-1);
    sourceTxRid = tx?.txRid ?? null;
    const pairKey = (fromPk, toPk) => `${String(fromPk)}\u0000${String(toPk)}`;
    const visible = new Map();
    for (const row of tx?.rows ?? []) {
      const fromPk = row[lt.backing.leftColumn], toPk = row[lt.backing.rightColumn];
      if (fromPk == null || toPk == null) throw new PlatformError('BAD_LINK_SCHEMA', `Join dataset '${ds.name}' contains a row missing one of its mapped key values`);
      visible.set(pairKey(fromPk, toPk), { fromPk, toPk });
    }
    // User-managed deltas are durable immediately but remain invisible until
    // this explicit batch sync applies them over the physical join snapshot.
    const writeback = s.linkWritebackDatasets?.find((item) => item.linkName === linkName);
    const writebackDs = writeback && s.datasets.find((item) => item.rid === writeback.datasetRid);
    for (const edit of (writebackDs ? branchTipRows(writebackDs, 'master') : [])) {
      if (edit.linkName !== linkName) continue;
      const key = pairKey(edit.fromPk, edit.toPk);
      if (edit.op === 'delete') visible.delete(key);
      else visible.set(key, { fromPk: edit.fromPk, toPk: edit.toPk });
    }
    pairs = [...visible.values()];
  } else {
    const manifest = s.indexes.find((i) => i.objectType === lt.backing.manifestObjectType);
    const manifestOt = s.objectTypes.find((o) => o.name === lt.backing.manifestObjectType);
    if (!manifestOt) return fail('NOT_FOUND', `Manifest object type '${lt.backing.manifestObjectType}' not found`);
    pairs = Object.values(manifest?.objects ?? {}).map((obj) => ({ fromPk: obj[lt.backing.leftProperty], toPk: obj[lt.backing.rightProperty], manifestPk: obj[manifestOt.primaryKey] }));
    if (!manifest) return fail('NOT_FOUND', `Manifest index '${lt.backing.manifestObjectType}' missing — sync it first`);
    sourceTxRid = manifest?.lastTxRid ?? null;
  }
  const idx = s.indexes.find((i) => i.objectType === lt.fromType);
  const targetIndex = s.indexes.find((i) => i.objectType === lt.toType);
  if (!idx || !targetIndex) return fail('NOT_FOUND', `Indexes for '${lt.fromType}' and '${lt.toType}' must be synced first`);
  const knownSources = new Set(Object.keys(idx.objects ?? {}));
  const knownTargets = new Set(Object.keys(targetIndex.objects ?? {}));
  const dangling = pairs.filter((pair) => !knownSources.has(String(pair.fromPk)) || !knownTargets.has(String(pair.toPk)));
  if (dangling.length) return fail('BROKEN_LINK_REFERENCE', `Physical link '${linkName}' contains endpoints not present in the current object indexes`, { dangling: dangling.slice(0, 10) });
  return store.mutate(() => {
    idx.links ??= [];
    const before = idx.links.filter((row) => row.linkType === linkName).length;
    idx.links = idx.links.filter((row) => row.linkType !== linkName);
    for (const pair of pairs) {
      const fromPk = pair.fromPk == null ? '' : String(pair.fromPk);
      const toPk = pair.toPk == null ? '' : String(pair.toPk);
      if (fromPk && toPk) idx.links.push({ linkType: linkName, fromPk, toPk });
    }
    lt.lastSyncTxRid = sourceTxRid;
    const after = idx.links.filter((row) => row.linkType === linkName).length;
    return { ok: true, link: linkName, linksIndexed: after, before, delta: after - before, latencyTier: lt.latencyTier, sourceTxRid };
  });
}

export function resolveLink(linkName, fromPk) {
  const s = store.state;
  const lt = s.linkTypes.find((l) => l.name === linkName);
  if (!lt) return fail('NOT_FOUND', `Link type '${linkName}' not found`);
  const fromIndex = s.indexes.find((i) => i.objectType === lt.fromType);
  const fromObject = fromIndex?.objects?.[String(fromPk)];
  let pks = [];
  let tier = lt.latencyTier ?? 'batch-cadence';
  if (lt.backing?.kind === 'foreign_key') {
    const fk = fromObject?.[lt.backing.fkProperty] ?? fromObject?.[lt.backing.fkColumn];
    pks = fk !== undefined && fk !== null && fk !== '' ? [String(fk)] : [];
    tier = 'derived-instant';
  } else {
    const links = (fromIndex?.links ?? []).filter((row) => row.linkType === linkName && row.fromPk === String(fromPk));
    pks = links.map((row) => row.toPk);
    if (lt.backing?.kind === 'object_backed') tier = 'batch-cadence (entity-backed)';
  }
  return { ok: true, link: linkName, backing: lt.backing?.kind ?? 'legacy-indexed', latencyTier: tier, pks };
}
