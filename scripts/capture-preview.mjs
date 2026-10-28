// Generates public/preview.html — a static, self-contained snapshot of the
// console backed by a fetch() shim serving captured API responses.
// Used when no live server can be kept running (e.g. sandboxed previews).
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

process.env.MINI_FOUNDRY_DATA_DIR = '/tmp/mf-preview-' + Date.now();
const { server } = await import('../server.js');
const { seedDemo } = await import('../seed.js');

await new Promise((r) => server.listen(0, r));
const port = server.address().port;
await seedDemo({ force: true });

const base = `http://127.0.0.1:${port}`;
const routes = {
  '/api/status': null, '/api/users': null, '/api/releases': null, '/api/governance': null,
  '/api/connectors': null, '/api/datasets': null, '/api/pipelines': null, '/api/ontology': null,
  '/api/objects/Part': null, '/api/objects/WorkOrder': null,
  '/api/objects/Part?search=': null, '/api/objects/Part?search=high': null,
  '/api/functions': null, '/api/actions': null, '/api/workshop': null, '/api/analyses': null,
  '/api/agents': null, '/api/agents/agent-atlas/tools': null,
};
for (const r of Object.keys(routes)) {
  const res = await fetch(base + r, { headers: { 'x-acting-user': 'admin' } });
  routes[r] = await res.json();
}
// Identity-sensitive reads: capture per-acting-user so the snapshot shim can
// demonstrate masking / row policies when the identity selector changes.
const identityScoped = {};
for (const actor of ['admin', 'technician', 'auditor', 'agent-atlas']) {
  for (const r of ['/api/objects/Part', '/api/objects/Part?search=high', '/api/objects/WorkOrder']) {
    const res = await fetch(base + r, { headers: { 'x-acting-user': actor } });
    identityScoped[`u:${actor} ${r}`] = await res.json();
  }
}
Object.assign(routes, identityScoped);
// A couple of representative POST results (validateOnly, agent query).
const post = async (r, body, actor = 'admin') => {
  const res = await fetch(base + r, { method: 'POST', headers: { 'content-type': 'application/json', 'x-acting-user': actor }, body: JSON.stringify(body) });
  return res.json();
};
routes['POST /api/actions/closeWorkOrder/submit'] = await post('/api/actions/closeWorkOrder/submit', { validateOnly: true, params: { workOrder: 'WO-5001', resolution: 'replaced pads', justification: 'torque verified' } });
routes['POST /api/agents/agent-atlas/query'] = await post('/api/agents/agent-atlas/query', { objectType: 'Part', search: 'high' }, 'agent-atlas');
routes['POST /api/datasets/parts_by_base/read'] = await post('/api/datasets/parts_by_base/read', {}, 'technician');
routes['POST /api/analytics/query'] = await post('/api/analytics/query', { dataset: 'parts_enriched', groupBy: ['base'], measures: [{ column: 'days_since_inspection', fn: 'avg', as: 'avg_days' }] });
routes['POST /api/functions/triagePart/run'] = await post('/api/functions/triagePart/run', { args: { partId: 'P-1004', reason: 'snapshot' }, mode: 'dry_run' });

server.close();

const read = (p) => fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'public', p), 'utf8');
const css = read('app.css');
const js = read('app.js');

const html = `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>mini-foundry — console (snapshot preview)</title>
<style>${css}</style></head>
<body>
<div style="background:#352a12;color:#d29922;font-size:12px;padding:5px 18px;border-bottom:1px solid #d29922">
  static snapshot preview — run <code style="background:none">npm start</code> and open http://localhost:6474 for the live console
</div>
<header>
  <div class="brand"><span class="logo">◈</span><div><h1>mini-foundry</h1>
  <div class="sub">connect → pipeline → ontology → applications → AI</div></div></div>
  <div class="identity"><label for="userSelect">acting identity</label>
  <select id="userSelect"></select><span id="userBadges"></span></div>
</header>
<nav id="tabs">
  <button data-tab="overview">Overview</button><button data-tab="connect">Connect</button>
  <button data-tab="datasets">Datasets</button><button data-tab="pipelines">Pipelines</button>
  <button data-tab="ontology">Ontology</button><button data-tab="objects">Objects</button>
  <button data-tab="functions">Functions</button><button data-tab="actions">Actions</button>
  <button data-tab="workshop">Workshop</button><button data-tab="analytics">Analytics</button>
  <button data-tab="agent">AI Agent</button><button data-tab="governance">Governance</button>
  <button data-tab="releases">Releases</button>
</nav>
<main id="main"></main>
<footer><span>Transactions are commits · Funnel indexes, query service serves · actions are rule sets, not RPCs · agents are users</span></footer>
<script>
window.MF_SNAPSHOT = ${JSON.stringify(routes)};
const realFetch = window.fetch.bind(window);
window.fetch = async (url, opts = {}) => {
  const u = new URL(url, location.origin);
  const actor = (opts.headers && opts.headers['x-acting-user']) || 'admin';
  const key = (opts.method && opts.method !== 'GET' ? 'POST ' : '') + u.pathname + (u.search || '');
  const find = (k) => window.MF_SNAPSHOT[k] ?? window.MF_SNAPSHOT[u.pathname + (u.search || '')] ?? window.MF_SNAPSHOT[u.pathname];
  const hit = window.MF_SNAPSHOT['u:' + actor + ' ' + u.pathname + (u.search || '')]
           ?? window.MF_SNAPSHOT['u:' + actor + ' ' + u.pathname]
           ?? find(key);
  const body = hit ?? { ok: false, error: { code: 'OFFLINE_SNAPSHOT', message: 'This static preview serves captured responses only. Run npm start for the live platform.' } };
  return new Response(JSON.stringify(body), { status: 200, headers: { 'content-type': 'application/json' } });
};
</script>
<script>${js}</script>
</body></html>`;

fs.writeFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'public', 'preview.html'), html);
console.log('preview.html written:', html.length, 'bytes');
process.exit(0);
