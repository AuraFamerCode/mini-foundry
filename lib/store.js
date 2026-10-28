// ─────────────────────────────────────────────────────────────────────────────
// Persistent store — in-memory with JSON snapshots (debounced) to data/
// Single-threaded Node: no locking needed; everything runs inside a mutator.
// ─────────────────────────────────────────────────────────────────────────────
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { migrateLegacyPropertyPolicies } from './security.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.MINI_FOUNDRY_DATA_DIR ?? path.join(__dirname, '..', 'data');

const DEFAULTS = () => ({
  // ── Connect layer
  sources: [],          // registered systems we can pull from
  connections: [],      // configured syncs (source -> dataset), carry lineage

  // ── Pipeline layer
  datasets: [],         // dataset records { rid, name, branches: { name: { transactions, schema } } }
  pipelines: [],        // pipeline definitions { steps[] } with provenance
  pipelineRuns: [],     // run history { datasetRid, profile, usage, profileChanges[] }
  incrementalRuns: [],  // incremental-transform run history (mode, read slices, tx types)
  incrementalInputWatermarks: {}, // last successful input tx per transform and dataset

  // ── Orchestration layer
  jobSpecs: [],         // compiled computation definitions (transform logic)
  jobs: [],             // concrete execution instances within builds
  builds: [],           // build records { resolution, jobs[], failures }
  schedules: [],        // recurring execution wrappers with compositional triggers

  // ── Ontology layer
  objectTypes: [],      // { name, display, primaryKey, titleKey, properties, backing }
  linkTypes: [],        // { name, fromType, toType, cardinality, backing }
  interfaces: [],       // { name, extends, properties, linkConstraints, actionConstraints }
  interfaceImpls: [],   // { interfaceName, objectTypeName, propertyMapping }
  indexes: [],          // Funnel-maintained object stores: one per backing dataset
  writebackDatasets: [],// explicit writeback datasets (OSv1-style edit durability)
  linkWritebackDatasets: [],// dedicated writable tiers for physical join-table links

  // ── Logic layer
  functions: [],        // { name, params, code, editMode }
  functionRuns: [],     // execution history incl. dry-runs and proposed edits
  actionTypes: [],      // { name, parameters, rules, submissionCriteria, overrides, ... }
  actionSubmissions: [],// submission history (edits applied, modes used)
  webhookOutbox: [],    // simulated webhook deliveries (side-effect demo)

  // ── Security layer
  users: [],            // { id, name, kind: 'human'|'agent', attributes, clearance, markings[] }
  markings: [],         // { name, description, members[] }
  policies: [],         // row policies: { datasetRid, expr, note }
  propPolicies: [],     // deprecated migration input; canonical rules live on ObjectType properties
  auditLog: [],         // every check that touched the security engine

  // ── Applications layer
  workshopApps: [],     // no-code app configs (module list)
  savedAnalyses: [],    // named query/aggregate results

  // ── AI layer
  agents: [],           // registered agents (also appear as users with kind 'agent')
  agentRuns: [],        // ask/act audit trail
  mcpApps: [],          // OMCP applications: scoped object/action surfaces
  pmcpSessions: [],     // PMCP (schema-editing MCP) session audit
  omcpCalls: [],        // OMCP per-call audit (user/agent/object/field/action checks)

  // ── Apollo layer
  bundles: [],          // released ontology bundles { version, snapshot, note }
});

class Store {
  constructor() {
    this.state = DEFAULTS();
    this._timer = null;
    this._dirty = false;
    this._load();
  }

  _file() { return path.join(DATA_DIR, 'state.json'); }

  _load() {
    try {
      if (fs.existsSync(this._file())) {
        const raw = JSON.parse(fs.readFileSync(this._file(), 'utf8'));
        this.state = { ...DEFAULTS(), ...raw };
        if (migrateLegacyPropertyPolicies(this.state)) this._dirty = true;
      }
    } catch (e) {
      // Corrupt snapshot: keep defaults rather than crash the platform.
      console.error('[store] failed to load snapshot, starting fresh:', e.message);
    }
  }

  /** Run a mutation inside the single mutator, then schedule persistence. */
  mutate(fn) {
    const out = fn(this.state);
    this._dirty = true;
    this._scheduleFlush();
    return out;
  }

  /**
   * Execute a synchronous unit of work atomically. Nested mutators share the
   * live state; an unexpected exception restores a deep checkpoint so a
   * multi-object Action/function cannot leave partially applied writes.
   */
  transaction(fn) {
    const checkpoint = structuredClone(this.state);
    const wasDirty = this._dirty;
    try {
      const out = fn(this.state);
      this._dirty = true;
      this._scheduleFlush();
      return out;
    } catch (error) {
      this.state = checkpoint;
      this._dirty = wasDirty;
      throw error;
    }
  }

  _scheduleFlush() {
    if (this._timer) return;
    this._timer = setTimeout(() => {
      this._timer = null;
      this.flush();
    }, 300);
  }

  flush() {
    if (!this._dirty) return;
    try {
      fs.mkdirSync(DATA_DIR, { recursive: true });
      fs.writeFileSync(this._file(), JSON.stringify(this.state, null, 1));
      this._dirty = false;
    } catch (e) {
      console.error('[store] flush failed:', e.message);
    }
  }

  // Convenience finders
  find(list, rid) { return this.state[list]?.find((x) => x.rid === rid); }
  require(list, rid) {
    const found = this.find(list, rid);
    if (!found) {
      const err = new Error(`Resource '${rid}' not found in '${list}'`);
      err.code = 'NOT_FOUND';
      throw err;
    }
    return found;
  }
}

export const store = new Store();
