// ─────────────────────────────────────────────────────────────────────────────
// Apollo layer — change management for the ontology itself.
// Ontology schema changes ripple through every dependent app, so the
// definition surface (object types, link types, action types, functions,
// policies) is captured as a versioned bundle: deploy creates a new version,
// rollback restores a previous one, and validation refuses restores that
// would break referential integrity (e.g. an action type pointing at an
// object type that no longer exists in that bundle).
// ─────────────────────────────────────────────────────────────────────────────
import { rid, now, PlatformError } from './util.js';
import { store } from './store.js';

const BUNDLE_KEYS = ['objectTypes', 'linkTypes', 'functions', 'actionTypes', 'policies', 'propPolicies', 'markings', 'users', 'workshopApps'];

export function captureBundle(note = 'manual release') {
  return store.mutate((s) => {
    const version = s.bundles.length + 1;
    const snapshot = {};
    for (const k of BUNDLE_KEYS) snapshot[k] = structuredClone(s[k] ?? []);
    const bundle = {
      version,
      rid: rid('bundle'),
      ts: now(),
      note,
      counts: Object.fromEntries(Object.entries(snapshot).map(([k, v]) => [k, v.length])),
      snapshot,
    };
    s.bundles.push(bundle);
    return { ok: true, version: bundle.version, rid: bundle.rid, counts: bundle.counts, ts: bundle.ts };
  });
}

export function listBundles() {
  return store.state.bundles.map((b) => ({
    version: b.version, rid: b.rid, ts: b.ts, note: b.note, counts: b.counts,
  }));
}

/** Restore an older bundle (rollback). Validates referential integrity first. */
export function rollbackTo(version) {
  const bundle = store.state.bundles.find((b) => b.version === version);
  if (!bundle) throw new PlatformError('NOT_FOUND', `Bundle version ${version} not found`);

  const problems = validateBundle(bundle.snapshot);
  if (problems.length) {
    return { ok: false, error: { code: 'BUNDLE_INVALID', message: `Refusing rollback: ${problems.join('; ')}` } };
  }

  // Pre-rollback safety net: capture the current state as a new version.
  const preRollback = captureBundle(`pre-rollback safety capture (rolling back to v${version})`);

  return store.mutate((s) => {
    for (const k of BUNDLE_KEYS) s[k] = structuredClone(bundle.snapshot[k] ?? []);
    return { ok: true, rolledBackTo: version, safetyCaptureVersion: preRollback.version };
  });
}

/**
 * Referential integrity for a bundle: link types need their object types,
 * actions need their object types and functions, function rules must be
 * exclusive, object types need their primary key declared.
 */
export function validateBundle(snap) {
  const problems = [];
  const otNames = new Set((snap.objectTypes ?? []).map((o) => o.name));
  const fnNames = new Set((snap.functions ?? []).map((f) => f.name));
  for (const o of snap.objectTypes ?? []) {
    if (!o.primaryKey || !o.properties?.some((p) => p.name === o.primaryKey)) {
      problems.push(`object type '${o.name}' has no valid primary key`);
    }
  }
  for (const l of snap.linkTypes ?? []) {
    if (!otNames.has(l.fromType) || !otNames.has(l.toType)) {
      problems.push(`link type '${l.name}' references missing object types`);
    }
  }
  for (const a of snap.actionTypes ?? []) {
    for (const r of a.rules ?? []) {
      if (r.kind === 'create' && r.objectType && !otNames.has(r.objectType)) {
        problems.push(`action '${a.name}' creates missing object type '${r.objectType}'`);
      }
      if (r.kind === 'function' && r.function && !fnNames.has(r.function)) {
        problems.push(`action '${a.name}' references missing function '${r.function}'`);
      }
      if ((r.kind === 'function') && (a.rules ?? []).length > 1) {
        problems.push(`action '${a.name}' mixes function rule with other rules`);
      }
    }
  }
  return problems;
}

/** Simulated `foundry deploy`: bundle -> installable artifact manifest. */
export function exportBundleManifest(version) {
  const bundle = store.state.bundles.find((b) => b.version === version);
  if (!bundle) throw new PlatformError('NOT_FOUND', `Bundle version ${version} not found`);
  return {
    ok: true,
    manifest: {
      product: 'mini-foundry-ontology',
      version: bundle.version,
      exportedAt: now(),
      contents: bundle.counts,
      installHint: 'foundry deploy --bundle mini-foundry-ontology@' + bundle.version,
      snapshotSize: JSON.stringify(bundle.snapshot).length,
    },
  };
}
