// ─────────────────────────────────────────────────────────────────────────────
// Security engine — layered, compositional, applied identically to humans and
// AI agents (every entry point resolves to a `user` record; agents are users).
//
// Layers:
//   1. Markings  — mandatory, conjunctive: subject must hold ALL markings on
//                  the resource (dataset-level).
//   2. Clearance — CBAC-style: subjects have a numeric clearance; certain
//                  properties carry `minClearance`. Subjects below it cannot
//                  read that property (column masking).
//   3. Row policies — per-dataset expression filters evaluated at query time.
//   4. Property markings — object property -> required marking (column level).
//
// Notable behaviors mirrored from the research:
//   - Every dataset has a classification (never "unclassified by default").
//   - Propagation: derived datasets inherit the union of markings of their
//     upstream datasets (mandatory controls propagate downstream).
//   - Restricted views + batch transforms are fundamentally in tension; we
//     disallow the unsafe combination instead of pretending to solve it.
// ─────────────────────────────────────────────────────────────────────────────
import { evalExpr, fail, PlatformError } from './util.js';
import { store } from './store.js';

export function datasetMarkings(ds, datasets = store.state.datasets, active = new Set()) {
  if (!ds || active.has(ds.rid)) return [];
  active.add(ds.rid);
  const datasetIds = new Set(datasets.map((dataset) => dataset.rid));
  const hasDatasetInputs = (ds.inputs ?? []).some((input) => datasetIds.has(input));
  const local = ds.localMarkings ?? (hasDatasetInputs ? [] : ds.markings ?? []);
  const inherited = new Set(local);
  for (const input of ds.inputs ?? []) {
    const upstream = datasets.find((dataset) => dataset.rid === input);
    if (!upstream) continue;
    for (const marking of datasetMarkings(upstream, datasets, active)) inherited.add(marking);
  }
  active.delete(ds.rid);
  return [...inherited];
}

/** Derived datasets inherit the current union, without freezing it at creation. */
export function computeInheritedMarkings(ds, datasets) {
  const datasetIds = new Set(datasets.map((dataset) => dataset.rid));
  const localMarkings = ds.localMarkings ?? (
    (ds.inputs ?? []).some((input) => datasetIds.has(input)) ? [] : ds.markings ?? []
  );
  const result = new Set(localMarkings);
  for (const input of ds.inputs ?? []) {
    const upstream = datasets.find((dataset) => dataset.rid === input);
    if (upstream) for (const marking of datasetMarkings(upstream, datasets)) result.add(marking);
  }
  return [...result];
}

function mergePropertyPolicy(property, policy) {
  const markings = new Set([...(property.requiredMarkings ?? []), ...(property.requiredMarking ? [property.requiredMarking] : [])]);
  if (policy.requiredMarking) markings.add(policy.requiredMarking);
  property.requiredMarkings = [...markings];
  if (property.requiredMarkings.length === 1) property.requiredMarking = property.requiredMarkings[0];
  else if (property.requiredMarkings.length > 1) delete property.requiredMarking;
  if (policy.minClearance !== undefined) property.minClearance = Math.max(property.minClearance ?? 0, policy.minClearance);
  return property;
}

/** Move pre-ontology dataset policies into the canonical ontology property. */
export function migrateLegacyPropertyPolicies(state) {
  let migrated = false;
  const legacy = state.propPolicies ?? [];
  const pending = [];
  for (const policy of legacy) {
    const objectType = state.objectTypes.find((type) => type.backing?.datasetRid === policy.datasetRid);
    const property = objectType?.properties.find((candidate) => candidate.name === policy.property || candidate.backingColumn === policy.property);
    if (!property) { pending.push(policy); continue; }
    mergePropertyPolicy(property, policy);
    migrated = true;
  }
  if (pending.length !== legacy.length) {
    state.propPolicies = pending;
    migrated = true;
  }
  for (const bundle of state.bundles ?? []) {
    const snapshot = bundle.snapshot;
    if (!snapshot || !Array.isArray(snapshot.propPolicies) || !snapshot.propPolicies.length) continue;
    for (const policy of snapshot.propPolicies) {
      const objectType = (snapshot.objectTypes ?? []).find((type) => type.backing?.datasetRid === policy.datasetRid);
      const property = objectType?.properties.find((candidate) => candidate.name === policy.property || candidate.backingColumn === policy.property);
      if (property) mergePropertyPolicy(property, policy);
    }
    const pending = (snapshot.propPolicies ?? []).filter((policy) => {
      const objectType = (snapshot.objectTypes ?? []).find((type) => type.backing?.datasetRid === policy.datasetRid);
      return !objectType?.properties.some((candidate) => candidate.name === policy.property || candidate.backingColumn === policy.property);
    });
    if (pending.length !== snapshot.propPolicies.length) {
      snapshot.propPolicies = pending;
      migrated = true;
    }
  }
  return migrated;
}

export function listPropertyPolicies() {
  const canonical = store.state.objectTypes.flatMap((objectType) => {
    if (!objectType.backing?.datasetRid) return [];
    return objectType.properties.flatMap((property) => {
      const requiredMarkings = [...new Set([...(property.requiredMarkings ?? []), ...(property.requiredMarking ? [property.requiredMarking] : [])])];
      if (!requiredMarkings.length && property.minClearance === undefined) return [];
      return [{
        objectType: objectType.name,
        datasetRid: objectType.backing.datasetRid,
        property: property.name,
        backingColumn: property.backingColumn ?? property.name,
        requiredMarking: requiredMarkings[0] ?? null,
        requiredMarkings,
        minClearance: property.minClearance,
      }];
    });
  });
  const unresolvedLegacy = (store.state.propPolicies ?? []).map((policy) => ({
    ...policy,
    objectType: null,
    requiredMarkings: policy.requiredMarking ? [policy.requiredMarking] : [],
    legacyUnresolved: true,
  }));
  return [...canonical, ...unresolvedLegacy];
}

export function hasMarkings(user, required, markings) {
  const held = new Set(user.markings ?? []);
  const missing = (required ?? []).filter((m) => !held.has(m));
  return missing.length === 0
    ? { ok: true }
    : fail('FORBIDDEN', `Access requires markings the caller does not hold`, { missing });
}

export function canReadDataset(user, ds) {
  const required = datasetMarkings(ds);
  const chk = hasMarkings(user, required);
  const pass = chk.ok;
  const missing = chk.error?.details?.missing ?? [];
  store.mutate((s) =>
    s.auditLog.push({
      ts: new Date().toISOString(),
      actor: user.id,
      kind: user.kind ?? 'human',
      action: 'dataset:read',
      resource: ds.rid,
      required,
      decision: pass ? 'allow' : 'deny',
      reason: pass ? 'markings satisfied' : `missing markings: ${missing.join(', ')}`,
    })
  );
  return chk;
}

/** Column-level guard for a single property of an object. */
export function canReadProperty(user, propDef, objectTypeName) {
  const requiredMarkings = [...new Set([...(propDef.requiredMarkings ?? []), ...(propDef.requiredMarking ? [propDef.requiredMarking] : [])])];
  const minClearance = propDef.minClearance;
  const held = new Set(user.markings ?? []);
  const missing = requiredMarkings.filter((marking) => !held.has(marking));
  if (missing.length) {
    return { ok: false, reason: `requires marking${missing.length === 1 ? '' : 's'} '${missing.join("', '")}'` };
  }
  if (minClearance !== undefined && (user.clearance ?? 0) < minClearance) {
    return { ok: false, reason: `requires clearance >= ${minClearance} (user has ${user.clearance ?? 0})` };
  }
  return { ok: true };
}

/** Apply row policies + property masking to a list of rows. */
export function applyReadPolicies(user, ds, rows) {
  // Row-level: evaluate each policy expression with the row in scope.
  const policies = store.state.policies.filter((p) => p.datasetRid === ds.rid);
  let visible = rows;
  for (const p of policies) {
    visible = visible.filter((row) => {
      try {
        return Boolean(evalExpr(p.expr, { ...row, user: { id: user.id, kind: user.kind, attributes: user.attributes ?? {} } }));
      } catch {
        return false; // fail closed
      }
    });
  }

  // Property markings and clearance have one canonical definition on the
  // ontology type. Dataset reads resolve those same property definitions.
  const types = store.state.objectTypes.filter((type) => type.backing?.datasetRid === ds.rid);
  const propertyDefinitions = types.flatMap((type) => type.properties.map((property) => ({
    definition: property,
    column: property.backingColumn ?? property.name,
  })));
  const legacyPolicies = (store.state.propPolicies ?? []).filter((policy) => policy.datasetRid === ds.rid);
  const masked = visible.map((row) => {
    const out = { ...row };
    for (const { definition, column } of propertyDefinitions) {
      if (Object.hasOwn(out, column) && !canReadProperty(user, definition, ds.name).ok) out[column] = '[REDACTED]';
    }
    for (const policy of legacyPolicies) {
      const definition = { requiredMarking: policy.requiredMarking, minClearance: policy.minClearance };
      if (Object.hasOwn(out, policy.property) && !canReadProperty(user, definition, ds.name).ok) out[policy.property] = '[REDACTED]';
    }
    return out;
  });
  return { rows: masked, visibleCount: visible.length, totalBeforePolicies: rows.length };
}

// ── Row policies CRUD (used by API + tests) ──────────────────────────────────
export function addRowPolicy({ datasetRid, expr, note, createdBy }) {
  return store.mutate((s) => {
    const p = { id: `rp-${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`, datasetRid, expr, note, createdBy, createdAt: new Date().toISOString() };
    s.policies.push(p);
    return p;
  });
}

export function addPropPolicy({ datasetRid, property, requiredMarking, minClearance }) {
  const objectType = store.state.objectTypes.find((type) => type.backing?.datasetRid === datasetRid);
  if (!objectType) throw new PlatformError('BAD_PROPERTY_POLICY', 'Property policies must target a declared ontology property on a backing dataset');
  const definition = objectType.properties.find((candidate) => candidate.name === property || candidate.backingColumn === property);
  if (!definition) throw new PlatformError('BAD_PROPERTY_POLICY', `Property '${property}' is not declared on '${objectType.name}'`);
  if (!requiredMarking && minClearance === undefined) throw new PlatformError('BAD_PROPERTY_POLICY', 'Property policy must require a marking or clearance');
  if (requiredMarking !== undefined && requiredMarking !== null && (typeof requiredMarking !== 'string' || !requiredMarking.trim())) throw new PlatformError('BAD_PROPERTY_POLICY', 'requiredMarking must be a nonempty string');
  if (minClearance !== undefined && (!Number.isFinite(minClearance) || minClearance < 0)) throw new PlatformError('BAD_PROPERTY_POLICY', 'minClearance must be a nonnegative finite number');
  return store.mutate(() => {
    mergePropertyPolicy(definition, { requiredMarking, minClearance });
    return {
      objectType: objectType.name,
      datasetRid,
      property: definition.name,
      backingColumn: definition.backingColumn ?? definition.name,
      requiredMarking: definition.requiredMarking ?? null,
      requiredMarkings: [...(definition.requiredMarkings ?? [])],
      minClearance: definition.minClearance,
      createdAt: new Date().toISOString(),
    };
  });
}

export function userAttribute(user, name) {
  return (user.attributes ?? {})[name];
}

// ── Restricted views ─────────────────────────────────────────────────────────
export function createRestrictedView(sourceRid, userAttrExpr, name) {
  return store.mutate((s) => {
    const source = s.datasets.find((d) => d.rid === sourceRid);
    if (!source) throw new Error(`Source dataset '${sourceRid}' not found`);
    if (s.datasets.some((dataset) => dataset.name === name)) throw new PlatformError('ALREADY_EXISTS', `Dataset '${name}' already exists`);
    // Markings are resolved dynamically from the source on every read.
    const ds = {
      rid: `ds-${name.toLowerCase().replace(/[^a-z0-9]+/g, '-')}`,
      name,
      kind: 'restricted_view',
      viewOf: sourceRid,
      policy: userAttrExpr,
      markings: [],
      localMarkings: [],
      inputs: [sourceRid],
      branches: { master: { transactions: [] } },
      schema: source.schema ?? [],
    };
    s.datasets.push(ds);
    return ds;
  });
}

export function isRestrictedView(ds) {
  return ds.kind === 'restricted_view';
}

/** Foundry-style rule: batch transforms cannot read restricted views. */
export function assertNotRestrictedForBatch(ds) {
  if (isRestrictedView(ds)) {
    return fail(
      'RESTRICTED_VIEW_BATCH',
      `Dataset '${ds.name}' is a restricted view; batch transforms cannot read it. ` +
      `Row-level security and generic batch compute are fundamentally in tension, ` +
      `so the unsafe combination is disallowed rather than approximated.`
    );
  }
  return { ok: true };
}
