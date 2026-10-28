// ─────────────────────────────────────────────────────────────────────────────
// mini-foundry server — HTTP surface for every platform layer.
// Zero dependencies. Default port 6474.
// Acting identity travels in the `x-acting-user` header (humans and agents
// both resolve to user records — the governance layer treats them alike).
// ─────────────────────────────────────────────────────────────────────────────
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { store } from './lib/store.js';
import { fail } from './lib/util.js';

import { listAdapters, registerSource, runSync } from './lib/connectors.js';
import { createDataset, getDataset, readDataset, createBranch, mergeBranch, listDatasets } from './lib/datasets.js';
import { addRowPolicy, addPropPolicy, listPropertyPolicies } from './lib/security.js';
import { createPipeline, runPipeline, updateProfile, PROFILE_FAMILIES, DEFAULT_PROFILE } from './lib/pipelines.js';
import {
  defineObjectType, defineLinkType, funnelSyncBatch, funnelStreamEvent,
  queryObjects, linkedObjects, listOntology, upsertLinks,
} from './lib/ontology.js';
import {
  validateSchemaContract, changePrimaryKey, configureWriteback, assertSingleBackingDataset,
} from './lib/physical.js';
import {
  defineLinkTypePhysical, generateJoinTable, configureLinkWriteback, syncJoinTableLink,
} from './lib/links.js';
import { defineInterface, implementInterface, implementationsOf, queryAsInterface } from './lib/interfaces.js';
import { defineIncrementalTransform, configureJobSpec, runIncrementalTransform } from './lib/incremental.js';
import { registerPipelineSpec, defineSchedule, runBuild, triggerSchedule, listOrchestration } from './lib/builds.js';
import { registerMcpApp, omcpToolCatalog, omcpSql, omcpInvokeAction, pmcpConnect, pmcpToolCatalog, pmcpCall, listMcpApps } from './lib/mcp.js';
import { defineFunction, runFunction, listFunctions } from './lib/functions.js';
import { defineActionType, submitAction, listActionTypes, getSubmissionLog } from './lib/actions.js';
import { registerAgent, agentQuery, agentAct, mcpToolCatalog } from './lib/agent.js';
import { captureBundle, listBundles, rollbackTo, exportBundleManifest } from './lib/releases.js';
import { seedDemo } from './seed.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.join(__dirname, 'public');
const PORT = Number.isInteger(Number(process.env.PORT)) && Number(process.env.PORT) > 0 ? Number(process.env.PORT) : 6474;

const MIME = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.json': 'application/json' };

function send(res, status, obj) {
  const body = JSON.stringify(obj, null, 2);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) });
  res.end(body);
}

function actor(req) {
  const id = req.headers['x-acting-user'] || 'admin';
  const u = store.state.users.find((x) => x.id === id);
  return u ?? { id, name: id, kind: 'human', markings: [], clearance: 0, attributes: {} };
}

const routes = [];
const route = (method, pattern, handler) => routes.push({ method, pattern, handler });
const match = (pattern, url) => {
  const pp = pattern.split('/').filter(Boolean);
  const up = url.split('/').filter(Boolean);
  if (pp.length !== up.length) return null;
  const params = {};
  for (let i = 0; i < pp.length; i++) {
    if (pp[i].startsWith(':')) params[pp[i].slice(1)] = decodeURIComponent(up[i]);
    else if (pp[i] !== up[i]) return null;
  }
  return params;
};

// ── Session / users ──────────────────────────────────────────────────────────
route('GET', 'api/users', (p, q, b, user) => ({ ok: true, users: store.state.users.map(({ id, name, kind, markings, clearance, attributes }) => ({ id, name, kind, markings, clearance, attributes })) }));
route('POST', 'api/users', (p, q, b) => {
  const u = { id: b.id, name: b.name ?? b.id, kind: b.kind ?? 'human', markings: b.markings ?? [], clearance: b.clearance ?? 0, attributes: b.attributes ?? {} };
  store.mutate((s) => s.users.push(u));
  return { ok: true, user: u };
});
route('POST', 'api/markings', (p, q, b) => {
  const m = { name: b.name, description: b.description ?? '', members: b.members ?? [] };
  store.mutate((s) => s.markings.push(m));
  return { ok: true, marking: m };
});

// ── Connect ──────────────────────────────────────────────────────────────────
route('GET', 'api/connectors', () => ({ ok: true, adapters: listAdapters(), sources: store.state.sources, syncs: store.state.connections.slice(-50) }));
route('POST', 'api/sources', (p, q, b) => ({ ok: true, source: registerSource(b) }));
route('POST', 'api/sync', async (p, q, b, user) => runSync(b.sourceRid, b));

// ── Datasets ─────────────────────────────────────────────────────────────────
route('GET', 'api/datasets', () => ({ ok: true, datasets: listDatasets() }));
route('POST', 'api/datasets', (p, q, b) => ({ ok: true, dataset: createDataset(b) }));
route('POST', 'api/datasets/:name/read', (p, q, b, user) => {
  const branch = b.branch ?? 'master';
  return readDataset(p.name, user, { branch });
});
route('POST', 'api/datasets/:name/branch', (p, q, b) => ({ ok: true, result: createBranch(p.name, b.branch, b.from ?? 'master') }));
route('POST', 'api/datasets/:name/merge', (p, q, b) => ({ ok: true, result: mergeBranch(p.name, b.from, b.into ?? 'master') }));
route('POST', 'api/policies/row', (p, q, b) => ({ ok: true, policy: addRowPolicy(b) }));
route('POST', 'api/policies/prop', (p, q, b) => ({ ok: true, policy: addPropPolicy(b) }));

// ── Pipelines ────────────────────────────────────────────────────────────────
route('GET', 'api/pipelines', () => ({ ok: true, pipelines: store.state.pipelines, runs: store.state.pipelineRuns.slice(-30), families: PROFILE_FAMILIES, defaultProfile: DEFAULT_PROFILE }));
route('POST', 'api/pipelines', (p, q, b) => ({ ok: true, pipeline: createPipeline(b) }));
route('POST', 'api/pipelines/:rid/run', (p, q, b, user) => runPipeline(p.rid, { branch: b.branch ?? 'master', usage: b.usage, user }));
route('POST', 'api/pipelines/:rid/profile', (p, q, b) => updateProfile(p.rid, b));

// ── Incremental transforms & build orchestration ──────────────────────────────
route('GET', 'api/orchestration', () => ({ ok: true, ...listOrchestration(), incrementalRuns: store.state.incrementalRuns.slice(-50) }));
route('POST', 'api/job-specs/pipelines', (p, q, b) => ({ ok: true, jobSpec: registerPipelineSpec(b) }));
route('POST', 'api/incremental/transforms', (p, q, b) => ({ ok: true, transform: defineIncrementalTransform(b) }));
route('POST', 'api/incremental/:name/configure', (p, q, b) => configureJobSpec(p.name, b));
route('POST', 'api/incremental/:name/run', (p, q, b) => runIncrementalTransform(p.name, { force: b.force === true }));
route('POST', 'api/builds', (p, q, b) => runBuild(b));
route('POST', 'api/schedules', (p, q, b) => ({ ok: true, schedule: defineSchedule(b) }));
route('POST', 'api/schedules/:name/trigger', (p, q, b) => triggerSchedule(p.name, b));

// ── Ontology ─────────────────────────────────────────────────────────────────
route('GET', 'api/ontology', () => ({ ok: true, ...listOntology(), editDatasets: store.state.datasets.filter((d) => d.managedBy === 'funnel').map((d) => d.name) }));
route('POST', 'api/ontology/object-types', (p, q, b) => ({ ok: true, objectType: defineObjectType(b) }));
route('POST', 'api/ontology/link-types', (p, q, b) => ({ ok: true, linkType: defineLinkType(b) }));
route('POST', 'api/ontology/:name/index', (p, q, b) => funnelSyncBatch(p.name));
route('POST', 'api/ontology/:name/stream', (p, q, b) => funnelStreamEvent(p.name, b));
route('GET', 'api/ontology/:name/physical', (p) => {
  const objectType = store.state.objectTypes.find((item) => item.name === p.name);
  if (!objectType) return fail('NOT_FOUND', `Object type '${p.name}' not found`);
  return { ok: true, objectType, writeback: store.state.writebackDatasets.find((entry) => entry.objectType === p.name) ?? null, schemaContract: validateSchemaContract(p.name) };
});
route('POST', 'api/ontology/:name/validate-schema', (p) => validateSchemaContract(p.name));
route('POST', 'api/ontology/:name/primary-key', (p, q, b) => changePrimaryKey(p.name, b.newPrimaryKey, { confirmEditLoss: b.confirmEditLoss === true }));
route('POST', 'api/ontology/:name/writeback', (p) => configureWriteback(p.name));
route('POST', 'api/ontology/:name/assert-backing-dataset', (p, q, b) => assertSingleBackingDataset(p.name, b.datasetName));
route('POST', 'api/ontology/physical-link-types', (p, q, b) => ({ ok: true, linkType: defineLinkTypePhysical(b) }));
route('POST', 'api/links/:name/generate-table', (p) => generateJoinTable(p.name));
route('POST', 'api/links/:name/writeback', (p) => configureLinkWriteback(p.name));
route('POST', 'api/links/:name/sync', (p) => syncJoinTableLink(p.name));
route('POST', 'api/links/:name/edits', (p, q, b, user) => upsertLinks(p.name, b.rows ?? [], { plannedCreates: b.plannedCreates ?? [], author: user.id }));
route('GET', 'api/interfaces', () => ({ ok: true, interfaces: store.state.interfaces, implementations: store.state.interfaceImpls }));
route('POST', 'api/interfaces', (p, q, b) => ({ ok: true, interface: defineInterface(b) }));
route('POST', 'api/interfaces/:name/implementations', (p, q, b) => implementInterface({ interfaceName: p.name, objectTypeName: b.objectTypeName, propertyMapping: b.propertyMapping ?? {} }));
route('GET', 'api/interfaces/:name/objects', (p, q, b, user) => queryAsInterface(p.name, user, { search: q.get('search') ?? undefined }));
route('GET', 'api/interfaces/:name/implementations', (p) => ({ ok: true, interface: p.name, ...implementationsOf(p.name) }));
route('GET', 'api/objects/:type', (p, q, b, user) => queryObjects(p.type, user, { search: q.get('search'), where: b.where }));
route('GET', 'api/objects/:type/:pk/links/:linkName', (p, q, b, user) => linkedObjects(p.type, p.pk, p.linkName, user));

// ── Functions ────────────────────────────────────────────────────────────────
route('GET', 'api/functions', () => ({ ok: true, functions: listFunctions(), runs: store.state.functionRuns.slice(-20) }));
route('POST', 'api/functions', (p, q, b) => ({ ok: true, fn: defineFunction(b) }));
route('POST', 'api/functions/:name/run', (p, q, b, user) => runFunction(p.name, b.args ?? {}, { mode: b.mode ?? 'dry_run', user }));

// ── Actions ──────────────────────────────────────────────────────────────────
route('GET', 'api/actions', () => ({ ok: true, actionTypes: listActionTypes(), submissions: getSubmissionLog() }));
route('POST', 'api/actions', (p, q, b) => ({ ok: true, actionType: defineActionType(b) }));
route('POST', 'api/actions/:name/submit', (p, q, b, user) => submitAction(p.name, b.params ?? {}, user, { validateOnly: b.validateOnly, returnEdits: b.returnEdits, branch: b.branch ?? 'master' }));

// ── Agents / OMCP ────────────────────────────────────────────────────────────
route('GET', 'api/agents', () => ({ ok: true, agents: store.state.agents, runs: store.state.agentRuns.slice(-30) }));
route('POST', 'api/agents', (p, q, b) => registerAgent(b));
route('GET', 'api/agents/:id/tools', (p) => mcpToolCatalog(p.id));
route('POST', 'api/agents/:id/query', (p, q, b) => agentQuery(p.id, b.objectType, b));
route('POST', 'api/agents/:id/act', (p, q, b) => agentAct(p.id, b.action, b.params, { validateOnly: b.validateOnly, branch: b.branch ?? 'master' }));

// ── OMCP consumer and PMCP builder boundaries ─────────────────────────────────
route('GET', 'api/mcp', () => ({ ok: true, ...listMcpApps() }));
route('POST', 'api/mcp/apps', (p, q, b) => registerMcpApp(b));
route('GET', 'api/mcp/apps/:name/tools', (p, q, b, user) => omcpToolCatalog(p.name, user.id));
route('POST', 'api/mcp/apps/:name/sql', (p, q, b, user) => omcpSql(p.name, user.id, b));
route('POST', 'api/mcp/apps/:name/actions/:actionName', (p, q, b, user) => omcpInvokeAction(p.name, user.id, p.actionName, b.params ?? {}, b.options ?? {}));
route('POST', 'api/pmcp/connect', (p, q, b) => pmcpConnect(b));
route('GET', 'api/pmcp/:session/tools', (p) => pmcpToolCatalog(p.session));
route('POST', 'api/pmcp/:session/call', (p, q, b) => pmcpCall(p.session, b.tool, b.args ?? {}));

// ── Applications & analytics ─────────────────────────────────────────────────
route('GET', 'api/workshop', () => ({ ok: true, apps: store.state.workshopApps }));
route('POST', 'api/workshop', (p, q, b) => store.mutate((s) => { s.workshopApps.push({ rid: `app-${Date.now()}`, ...b, createdAt: new Date().toISOString() }); return { ok: true, apps: s.workshopApps }; }));
route('POST', 'api/analytics/query', (p, q, b, user) => {
  const ds = getDataset(b.dataset);
  if (!ds) return fail('NOT_FOUND', `Dataset '${b.dataset}' not found`);
  const r = readDataset(b.dataset, user, {});
  if (!r.ok) return r;
  const groups = new Map();
  for (const row of r.rows) {
    const key = JSON.stringify((b.groupBy ?? []).map((g) => row[g]));
    if (!groups.has(key)) groups.set(key, Object.fromEntries((b.groupBy ?? []).map((g) => [g, row[g]])));
    const g = groups.get(key);
    for (const m of b.measures ?? []) {
      const v = m.column ? Number(row[m.column]) : 1;
      g[m.as] ??= 0;
      if (m.fn === 'count') g[m.as] += 1;
      else if (!Number.isNaN(v)) g[m.as] += v;
    }
  }
  return { ok: true, dataset: b.dataset, rows: [...groups.values()], rowCount: r.rows.length };
});
route('GET', 'api/analyses', () => ({ ok: true, analyses: store.state.savedAnalyses }));
route('POST', 'api/analyses', (p, q, b) => store.mutate((s) => { s.savedAnalyses.push({ id: `an-${Date.now()}`, ...b }); return { ok: true }; }));

// ── Governance & releases ────────────────────────────────────────────────────
route('GET', 'api/governance', () => ({
  ok: true,
  markings: store.state.markings,
  users: store.state.users.map(({ id, name, kind, markings, clearance, attributes }) => ({ id, name, kind, markings, clearance, attributes })),
  rowPolicies: store.state.policies,
  propPolicies: listPropertyPolicies(),
  auditLog: store.state.auditLog.slice(-120),
  webhookOutbox: store.state.webhookOutbox.slice(-30),
}));
route('POST', 'api/releases', (p, q, b) => captureBundle(b?.note ?? 'manual release'));
route('GET', 'api/releases', () => ({ ok: true, bundles: listBundles() }));
route('POST', 'api/releases/:version/rollback', (p) => rollbackTo(Number(p.version)));
route('GET', 'api/releases/:version/manifest', (p) => exportBundleManifest(Number(p.version)));
route('GET', 'api/status', () => ({
  ok: true,
  counts: {
    datasets: store.state.datasets.length,
    objectTypes: store.state.objectTypes.length,
    indexedObjects: store.state.indexes.reduce((a, i) => a + i.stats.indexedObjects, 0),
    linkTypes: store.state.linkTypes.length,
    functions: store.state.functions.length,
    actionTypes: store.state.actionTypes.length,
    pipelines: store.state.pipelines.length,
    users: store.state.users.length,
    bundles: store.state.bundles.length,
  },
}));
route('POST', 'api/admin/reset', () => seedDemo({ force: true }));

// ── HTTP plumbing ────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const parts = url.pathname.split('/').filter(Boolean);

  if (parts[0] !== 'api') {
    // static files
    let file = parts.length ? path.join(PUBLIC_DIR, ...parts) : path.join(PUBLIC_DIR, 'index.html');
    if (!file.startsWith(PUBLIC_DIR)) { send(res, 403, fail('FORBIDDEN', 'no')); return; }
    if (!fs.existsSync(file) || fs.statSync(file).isDirectory()) file = path.join(PUBLIC_DIR, 'index.html');
    try {
      const data = fs.readFileSync(file);
      res.writeHead(200, { 'content-type': MIME[path.extname(file)] ?? 'application/octet-stream' });
      res.end(data);
    } catch {
      send(res, 404, fail('NOT_FOUND', 'file not found'));
    }
    return;
  }

  let body = {};
  if (req.method === 'POST' || req.method === 'PUT') {
    let raw = '';
    for await (const chunk of req) raw += chunk;
    try { body = raw ? JSON.parse(raw) : {}; } catch { send(res, 400, fail('BAD_JSON', 'invalid JSON body')); return; }
  }

  const user = actor(req);
  for (const r of routes) {
    if (r.method !== req.method) continue;
    const params = match(r.pattern, parts.join('/'));
    if (!params) continue;
    try {
      const out = await r.handler(params, url.searchParams, body, user);
      send(res, out && out.ok === false ? 422 : 200, out);
    } catch (e) {
      send(res, e.code === 'NOT_FOUND' ? 404 : 400, fail(e.code ?? 'ERROR', e.message));
    }
    return;
  }
  send(res, 404, fail('NOT_FOUND', `No route for ${req.method} /${parts.join('/')}`));
});

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  seedDemo({ force: false });
  server.listen(PORT, () => {
    console.log(`mini-foundry listening on http://localhost:${PORT}`);
  });
}

export { server };
