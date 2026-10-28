// ─────────────────────────────────────────────────────────────────────────────
// Connect layer — pluggable ingestion adapters. Every sync preserves lineage
// metadata from the first sync (source system -> dataset, row counts, ts).
// Three adapter families: CSV/file, REST API, JDBC-style table pull.
// ─────────────────────────────────────────────────────────────────────────────
import { rid, now, PlatformError } from './util.js';
import { store } from './store.js';
import { createDataset, commitTransaction, requireDataset } from './datasets.js';

// Adapter registry — the "connector framework" is just a map of typed
// adapters; new sources plug in without touching the sync machinery.
const ADAPTERS = {
  csv: {
    label: 'CSV / file',
    // csv config: { csv } — raw text; first line is header
    async fetch(cfg) {
      const text = cfg.csv ?? '';
      const lines = text.trim().split(/\r?\n/).filter((l) => l.trim().length);
      if (lines.length === 0) return { schema: [], rows: [] };
      const headers = lines[0].split(',').map((h) => h.trim());
      const rows = lines.slice(1).map((line) => {
        const cells = line.split(',').map((c) => c.trim());
        const row = {};
        headers.forEach((h, i) => (row[h] = cells[i] ?? ''));
        return row;
      });
      // Infer numeric columns from all nonempty values, then materialize those
      // cells as numbers so runtime rows honor the published connector schema.
      const schema = headers.map((h) => {
        const values = rows.map((row) => row[h]).filter((value) => value !== '');
        const numeric = values.length > 0 && values.every((value) => /^[-+]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(value) && Number.isFinite(Number(value)));
        return { name: h, type: numeric ? 'number' : 'string' };
      });
      const types = new Map(schema.map(({ name, type }) => [name, type]));
      const materialized = rows.map((row) => Object.fromEntries(headers.map((header) => {
        const value = row[header];
        return [header, types.get(header) === 'number' ? (value === '' ? null : Number(value)) : value];
      })));
      return { schema, rows: materialized };
    },
  },

  api: {
    label: 'REST API',
    // api config: { url, headers, recordPath } — recordPath like 'data.items'
    async fetch(cfg) {
      const res = await fetch(cfg.url, {
        headers: cfg.headers ?? {},
        signal: AbortSignal.timeout(10_000),
      });
      if (!res.ok) throw new PlatformError('CONNECTOR_HTTP', `API returned ${res.status}`);
      let json = await res.json();
      if (cfg.recordPath) {
        for (const seg of cfg.recordPath.split('.')) json = json?.[seg];
      }
      const rows = Array.isArray(json) ? json : [json];
      const schema = rows.length
        ? Object.entries(rows[0]).map(([k, v]) => ({
            name: k,
            type: typeof v === 'number' ? 'number' : typeof v === 'boolean' ? 'boolean' : 'string',
          }))
        : [];
      return { schema, rows };
    },
  },

  jdbc: {
    label: 'JDBC table (simulated)',
    // jdbc config: { simulate: [...] } — in this miniature, JDBC sources are
    // simulated by providing rows inline (a real build would call a driver).
    async fetch(cfg) {
      const rows = cfg.simulate ?? [];
      const schema = rows.length
        ? Object.entries(rows[0]).map(([k, v]) => ({
            name: k,
            type: typeof v === 'number' ? 'number' : 'string',
          }))
        : [];
      return { schema, rows };
    },
  },
};

export function listAdapters() {
  return Object.entries(ADAPTERS).map(([id, a]) => ({ id, label: a.label }));
}

export function registerSource({ name, adapter, config, markings }) {
  if (!ADAPTERS[adapter]) throw new PlatformError('BAD_ADAPTER', `Unknown adapter '${adapter}'`);
  return store.mutate((s) => {
    const src = { rid: rid('src'), name, adapter, config, markings: markings ?? [], createdAt: now() };
    s.sources.push(src);
    return src;
  });
}

/**
 * Run a sync: fetch via adapter, land rows as a transaction on the target
 * dataset, and record lineage from the very first sync.
 */
export async function runSync(sourceRid, { datasetName, mode = 'SNAPSHOT', branch = 'master' } = {}) {
  const source = store.state.sources.find((s) => s.rid === sourceRid);
  if (!source) throw new PlatformError('NOT_FOUND', `Source '${sourceRid}' not found`);
  const adapter = ADAPTERS[source.adapter];
  const { schema, rows } = await adapter.fetch(source.config);

  let ds;
  if (datasetName) {
    ds = getOrCreateDataset(datasetName, schema, source.markings);
  } else {
    ds = getOrCreateDataset(source.name.toLowerCase().replace(/\s+/g, '_'), schema, source.markings);
  }

  const tx = commitTransaction(ds.rid, branch, { op: mode, rows, note: `sync from source ${source.name} (${source.adapter})` });

  const lineage = {
    rid: rid('ln'),
    ts: now(),
    sourceRid: source.rid,
    sourceName: source.name,
    adapter: source.adapter,
    datasetRid: ds.rid,
    datasetName: ds.name,
    branch,
    rowCount: rows.length,
    txRid: tx.txRid,
  };
  store.mutate((s) => {
    s.connections.push(lineage);
    ds.schema = schema;
    ds.inputs = [...new Set([...(ds.inputs ?? []), source.rid])];
  });
  return { ok: true, dataset: { rid: ds.rid, name: ds.name }, rowCount: rows.length, txRid: tx.txRid, lineage };
}

function getOrCreateDataset(name, schema, markings) {
  const existing = store.state.datasets.find((d) => d.name === name);
  if (existing) return existing;
  return createDataset({ name, schema, markings });
}
