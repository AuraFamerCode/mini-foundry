// mini-foundry console — vanilla JS, no build step.
// Every request carries the selected acting identity via x-acting-user;
// the governance layer treats humans and agents identically.

const state = {
  user: localStorage.getItem('mf-user') || 'admin',
  tab: 'overview',
  users: [],
  lastObjectQuery: { type: 'Part', search: '' },
};

const $ = (sel) => document.querySelector(sel);
const el = (html) => { const t = document.createElement('template'); t.innerHTML = html.trim(); return t.content.firstElementChild; };
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

async function api(path, body, method) {
  const m = method ?? (body ? 'POST' : 'GET');
  const res = await fetch(path, {
    method: m,
    headers: { 'content-type': 'application/json', 'x-acting-user': state.user },
    body: body && m !== 'GET' ? JSON.stringify(body) : undefined,
  });
  return res.json();
}

function out(target, result) {
  const box = typeof target === 'string' ? $(target) : target;
  box.innerHTML = `<pre class="out">${esc(JSON.stringify(result, null, 2))}</pre>`;
  const ok = result && result.ok !== false;
  box.classList.toggle('ok', ok);
  box.classList.toggle('err', !ok);
}

function toast(msg, bad) {
  const t = el(`<div class="toast ${bad ? 'bad' : ''}">${esc(msg)}</div>`);
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 3500);
}

// ── Tabs registry ────────────────────────────────────────────────────────────
const TABS = {};
const render = { header() {}, };

// ── Tab implementations ───────────────────────────────────────────────────

TABS.overview = async (main) => {
  const st = await api('/api/status');
  const rel = await api('/api/releases');
  const go = await api('/api/governance');
  main.append(
    el(`<h2>Platform overview</h2>`),
    el(`<p class="note">A working miniature of the five-layer architecture. Switch your <b>acting identity</b> (top right) and re-open any tab: markings, clearance, row policies and property masking apply identically to humans and agents, at read time and at action time.</p>`),
    el(`<div class="grid">${[
      ['Datasets', st.counts.datasets], ['Indexed objects', st.counts.indexedObjects], ['Object types', st.counts.objectTypes],
      ['Link types', st.counts.linkTypes], ['Action types', st.counts.actionTypes], ['Functions', st.counts.functions],
      ['Pipelines', st.counts.pipelines], ['Identities', st.counts.users], ['Bundles', st.counts.bundles],
    ].map(([k, v]) => `<div class="stat"><b>${v}</b><span>${k}</span></div>`).join('')}</div>`),
  );
  const audit = go.auditLog.slice(-8).reverse().map((a) => `<tr><td>${esc(a.ts.slice(11, 19))}</td><td>${esc(a.actor)} <span class="tag">${esc(a.kind)}</span></td><td>${esc(a.action)}</td><td>${a.decision === 'allow' ? '<span class="pill allow">allow</span>' : '<span class="pill deny">deny</span>'}</td><td class="mono">${esc(a.reason ?? '')}</td></tr>`).join('');
  main.append(
    el(`<h3>Recent security decisions</h3>`),
    el(`<table><tr><th>time</th><th>actor</th><th>action</th><th>decision</th><th>reason</th></tr>${audit}</table>`),
    el(`<h3>Releases (Apollo)</h3>`),
    el(`<p class="note">${(rel.bundles ?? []).length} bundle(s) captured. Deploy and rollback live in the <b>Releases</b> tab.</p>`),
  );
};

TABS.connect = async (main) => {
  const d = await api('/api/connectors');
  main.append(el(`<h2>Connect — ingestion with lineage from the first sync</h2>`));
  const srcs = d.sources.map((s) => `<tr><td>${esc(s.name)}</td><td><span class="tag">${esc(s.adapter)}</span></td><td>${(s.markings ?? []).map((m) => `<span class="tag priv">${esc(m)}</span>`).join('') || '<span class="tag">none</span>'}</td></tr>`).join('');
  const syncs = d.syncs.slice(-8).reverse().map((s) => `<tr><td class="mono">${esc(s.ts.slice(0, 19))}</td><td>${esc(s.sourceName)} → ${esc(s.datasetName)}</td><td class="num">${s.rowCount}</td><td class="mono">${esc(s.txRid ?? '')}</td></tr>`).join('');
  main.append(
    el(`<div class="card"><h3>Registered sources</h3><table><tr><th>source</th><th>adapter</th><th>markings</th></tr>${srcs}</table></div>`),
    el(`<div class="card"><h3>Recent syncs (lineage)</h3><table><tr><th>time</th><th>source → dataset</th><th>rows</th><th>transaction</th></tr>${syncs}</table></div>`),
    el(`<div class="card"><h3>Register a new source</h3>
      <div class="row">
        <div><label>name</label><input id="srcName" value="fleet-api"></div>
        <div><label>adapter</label><select id="srcAdapter">${d.adapters.map((a) => `<option value="${a.id}">${a.label}</option>`).join('')}</select></div>
        <div><label>CSV payload (for csv adapter)</label><input id="srcCsv" value="station_id,reading\nSFO,21.4\nJFK,19.8"></div>
        <div><label>markings (comma-sep)</label><input id="srcMarks" value="SAFETY"></div>
        <div><button class="act" id="addSrc">Register</button></div>
      </div><div id="srcOut"></div></div>`),
  );
  $('#addSrc').onclick = async () => {
    const adapter = $('#srcAdapter').value;
    const config = adapter === 'csv' ? { csv: $('#srcCsv').value } : adapter === 'jdbc' ? { simulate: [{ station_id: 'SFO', reading: 21.4 }] } : { url: 'https://example.com/data.json' };
    const r = await api('/api/sources', { name: $('#srcName').value, adapter, config, markings: $('#srcMarks').value.split(',').map((s) => s.trim()).filter(Boolean) });
    out('#srcOut', r); if (r.ok) toast('Source registered');
  };
};

TABS.datasets = async (main) => {
  const d = await api('/api/datasets');
  main.append(el(`<h2>Datasets — "Git for data"</h2>`),
    el(`<p class="note">A transaction is the atomic-commit primitive; branches and builds are policies over it. Read paths apply row policies and column masking at query time. Restricted views refuse batch transforms by design.</p>`));
  const rows = d.datasets.map((x) => `<tr><td>${esc(x.name)}</td><td><span class="tag">${esc(x.kind)}</span></td><td>${(x.markings ?? []).map((m) => `<span class="tag priv">${esc(m)}</span>`).join('') || '<span class="tag">none</span>'}</td><td>${(x.branches ?? []).map((b) => `<span class="tag">${esc(b)}</span>`).join('')}</td><td class="num">${Object.values(x.txCount ?? {}).join('/')}</td></tr>`).join('');
  main.append(el(`<div class="card"><table><tr><th>dataset</th><th>kind</th><th>markings (propagated)</th><th>branches</th><th>tx counts</th></tr>${rows}</table></div>`));
  main.append(el(`<div class="card"><h3>Read as current identity</h3>
    <div class="row"><div><label>dataset</label><input id="rdName" value="parts_by_base"></div>
    <div><label>branch</label><input id="rdBranch" value="master"></div>
    <div><button class="act" id="rdGo">Read</button></div></div><div id="rdOut"></div></div>`));
  $('#rdGo').onclick = async () => out('#rdOut', await api('/api/datasets/' + encodeURIComponent($('#rdName').value) + '/read', { branch: $('#rdBranch').value || 'master' }));
};

TABS.pipelines = async (main) => {
  const d = await api('/api/pipelines');
  main.append(el(`<h2>Pipelines — declarative transforms with provenance</h2>`));
  for (const p of d.pipelines) {
    const runs = d.runs.filter((r) => r.pipelineRid === p.rid).slice(-3).reverse();
    const card = el(`<div class="card">
      <h3>${esc(p.name)} <span class="tag">${p.steps.length} steps</span></h3>
      <div class="mono">${p.steps.map((s) => esc(s.op) + (s.dataset ? ':' + esc(s.dataset) : s.expr ? ': ' + esc(s.expr) : s.name ? ':' + esc(s.name) : '')).join('  →  ')}</div>
      <p class="note">Profile: <code>${esc(JSON.stringify(p.profile))}</code> — tune one family at a time; the auto-tuner only acts after 5 stable runs with usage data.</p>
      <div class="row">
        <div><label>executor memory tier</label><select id="pm-${p.rid}">${Object.keys(d.families.executorMemory.tiers).map((t) => `<option ${p.profile.executorMemory === t ? 'selected' : ''}>${t}</option>`).join('')}</select></div>
        <div><button class="act ghost" id="pp-${p.rid}">Apply profile change</button></div>
        <div><button class="act green" id="pr-${p.rid}">Run pipeline</button></div>
      </div>
      <div id="po-${p.rid}"></div></div>`);
    main.append(card);
    $(`#pp-${p.rid}`).onclick = async () => out(`#po-${p.rid}`, await api(`/api/pipelines/${p.rid}/profile`, { executorMemory: $(`#pm-${p.rid}`).value }));
    $(`#pr-${p.rid}`).onclick = async () => out(`#po-${p.rid}`, await api(`/api/pipelines/${p.rid}/run`, { usage: { driverMemoryGb: 4, executorMemoryGb: 5, executorCores: 2, numExecutors: 1 } }));
    if (runs.length) {
      const r = runs[0];
      card.append(el(`<p class="note">Last run: ${r.rowsIn} → ${r.rowsOut} rows · profile source: <b>${esc(r.profileSource)}</b> · ${esc(r.profileNote ?? '')}</p>`));
      if (r.provenance?.length) {
        card.append(el(`<details><summary class="mono">column provenance (${r.provenance.length})</summary><table><tr><th>column</th><th>step</th><th>detail</th></tr>${r.provenance.map((pv) => `<tr><td>${esc(pv.column)}</td><td class="num">${pv.step}</td><td class="mono">${esc(pv.detail)}</td></tr>`).join('')}</table></details>`));
      }
    }
  }
};

TABS.ontology = async (main) => {
  const d = await api('/api/ontology');
  main.append(el(`<h2>Ontology — semantic + kinetic primitives</h2>`),
    el(`<p class="note">Object types are backed by live datasets and indexed by <b>Funnel</b> (separate from the query service). Streaming types carry hard caps (1MB records, 250 properties, PK partitioning). User edits from Actions hit the index immediately, then flush to Funnel-owned edit datasets.</p>`));
  for (const o of d.objectTypes) {
    const props = o.properties.map((p) => {
      const tags = [];
      if (p.name === o.primaryKey) tags.push('<span class="tag">PK</span>');
      if (p.requiredMarking) tags.push(`<span class="tag priv">${esc(p.requiredMarking)}</span>`);
      if (p.minClearance !== undefined) tags.push(`<span class="tag clear">clear≥${p.minClearance}</span>`);
      return `<tr><td>${esc(p.name)}</td><td>${esc(p.type ?? 'string')}</td><td>${tags.join('')}</td></tr>`;
    }).join('');
    main.append(el(`<div class="card"><h3>${esc(o.display)} <span class="tag">${esc(o.name)}</span> ${o.backing.mode === 'streaming' ? '<span class="tag clear">streaming</span>' : '<span class="tag">batch</span>'}</h3>
      <p class="note">backing dataset: <code>${esc(o.backing.datasetName)}</code> · indexed: ${o.indexed ? o.indexed.indexedObjects + ' objects, last sync ' + esc((o.indexed.lastSync ?? '').slice(0, 19)) : 'not indexed'}${o.indexed?.editFlushTx ? ' · edit flush txs: ' + o.indexed.editFlushTx : ''}</p>
      <table><tr><th>property</th><th>type</th><th>guards</th></tr>${props}</table>
      <div class="row"><button class="act ghost" id="reindex-${o.name}">Funnel: incremental re-index</button></div>
      <div id="ix-${o.name}"></div></div>`));
    $(`#reindex-${o.name}`)?.addEventListener('click', async () => out(`#ix-${o.name}`, await api(`/api/ontology/${o.name}/index`, {})));
  }
  main.append(el(`<h3>Link types</h3>`), el(`<table><tr><th>link</th><th>from</th><th>to</th><th>cardinality</th></tr>${d.linkTypes.map((l) => `<tr><td>${esc(l.name)}</td><td>${esc(l.fromType)}</td><td>${esc(l.toType)}</td><td>${esc(l.cardinality)}</td></tr>`).join('')}</table>`),
    el(`<h3>Funnel-owned edit datasets</h3>`), el(`<p class="note">${d.editDatasets.map((x) => `<code>${esc(x)}</code>`).join(' ') || 'none yet — apply an Action first'}</p>`));
};

TABS.objects = async (main) => {
  const d = await api('/api/ontology');
  const types = d.objectTypes.map((o) => o.name);
  const cur = state.lastObjectQuery;
  main.append(el(`<h2>Objects — governed query surface</h2>`),
    el(`<p class="note">Reads resolve against the Funnel index, never the datasets. Property masking and row policies apply to your identity at query time — switch identities and compare.</p>`),
    el(`<div class="card"><div class="row">
      <div><label>object type</label><select id="objType">${types.map((t) => `<option ${t === cur.type ? 'selected' : ''}>${t}</option>`).join('')}</select></div>
      <div><label>search</label><input id="objSearch" value="${esc(cur.search)}"></div>
      <div><button class="act" id="objGo">Query</button></div>
    </div><div id="objOut"></div></div>`));
  const runQ = async () => {
    const t = $('#objType').value, s = $('#objSearch').value;
    state.lastObjectQuery = { type: t, search: s };
    const r = await api(`/api/objects/${t}?search=` + encodeURIComponent(s));
    out('#objOut', r);
  };
  $('#objGo').onclick = runQ;
  runQ();
};

TABS.functions = async (main) => {
  const d = await api('/api/functions');
  main.append(el(`<h2>Functions — pure until applied</h2>`),
    el(`<p class="note">Edit functions are side-effect-free in dry-run mode: edits land in a container, reads inside the same run see committed data only. Apply mode writes through Funnel.</p>`));
  for (const f of d.functions) {
    main.append(el(`<div class="card"><h3>${esc(f.name)}</h3>
      <p class="note">${esc(f.description)}</p>
      <pre class="out" style="max-height:150px">${esc(f.code.trim())}</pre>
      <div class="row">
        <div><label>partId</label><input id="fa-${f.name}" value="P-1004"></div>
        <div><label>reason</label><input id="fr-${f.name}" value="dry-run test from console"></div>
        <div><button class="act ghost" id="fd-${f.name}">Dry run</button></div>
        <div><button class="act green" id="fy-${f.name}">Apply</button></div>
      </div><div id="fo-${f.name}"></div></div>`));
    $(`#fd-${f.name}`).onclick = async () => out(`#fo-${f.name}`, await api(`/api/functions/${f.name}/run`, { args: { partId: $(`#fa-${f.name}`).value, reason: $(`#fr-${f.name}`).value }, mode: 'dry_run' }));
    $(`#fy-${f.name}`).onclick = async () => out(`#fo-${f.name}`, await api(`/api/functions/${f.name}/run`, { args: { partId: $(`#fa-${f.name}`).value, reason: $(`#fr-${f.name}`).value }, mode: 'apply' }));
  }
};

TABS.actions = async (main) => {
  const d = await api('/api/actions');
  main.append(el(`<h2>Action types — governed write transactions</h2>`),
    el(`<p class="note">Four separable surfaces: parameters, rules, submission criteria, overrides. Every submission passes read-permission ∧ write-authorization ∧ criteria, with dependency-order validation before commit. <code>validateOnly</code> and <code>returnEdits</code> are mutually exclusive.</p>`));
  for (const a of d.actionTypes) {
    const params = a.parameters.map((p) => {
      const tags = [];
      if (p.required) tags.push('<span class="tag">required</span>');
      if (p.type === 'object-reference') tags.push(`<span class="tag">→ ${esc(p.objectType)}</span>`);
      if (p.source?.kind) tags.push(`<span class="tag">${esc(p.source.kind)}</span>`);
      return `<tr><td>${esc(p.name)}</td><td>${esc(p.type ?? 'string')}</td><td>${tags.join('')}</td></tr>`;
    }).join('');
    const rules = a.rules.map((r) => `<span class="tag">${esc(r.kind)}${r.function ? ':' + esc(r.function) : r.webhookName ? ':' + esc(r.webhookName) : r.objectType ? ':' + esc(r.objectType) : ''}</span>`).join(' ');
    const crit = (a.submissionCriteria ?? []).map((c) => `<div class="mono">✓ ${esc(c.expr)}</div>`).join('') || '<span class="tag">no criteria</span>';
    const ovr = (a.overrides ?? []).map((o) => `<div class="mono">↳ if ${esc((o.conditions ?? []).map((c) => c.expr).join(' ∧ '))} then ${(o.then ?? []).map((t) => esc(JSON.stringify(t))).join(', ')}</div>`).join('');
    main.append(el(`<div class="card"><h3>${esc(a.name)} ${a.writeAuthorization ? `<span class="tag priv">write: ${esc(a.writeAuthorization.requiredMarking)}</span>` : ''}</h3>
      <p class="note">${esc(a.description)}</p>
      <table><tr><th>parameter</th><th>type</th><th></th></tr>${params}</table>
      <p class="note">rules: ${rules}</p><p class="note">criteria:<br>${crit}</p>${ovr ? `<p class="note">overrides:<br>${ovr}</p>` : ''}
      <div class="row">
        ${(a.parameters ?? []).map((p) => `<div><label>${esc(p.name)}${p.source?.kind ? ' (auto)' : ''}</label><input id="ap-${a.name}-${p.name}" ${p.source?.kind ? 'disabled' : ''}></div>`).join('')}
        <div><label>branch</label><select id="ab-${a.name}"><option>master</option><option>sandbox</option></select></div>
        <div><button class="act ghost" id="av-${a.name}">Validate only</button></div>
        <div><button class="act green" id="as-${a.name}">Submit</button></div>
      </div><div id="ao-${a.name}"></div></div>`));
    const collect = () => Object.fromEntries((a.parameters ?? []).filter((p) => !p.source?.kind).map((p) => [p.name, $(`#ap-${a.name}-${p.name}`).value]));
    $(`#av-${a.name}`).onclick = async () => out(`#ao-${a.name}`, await api(`/api/actions/${a.name}/submit`, { validateOnly: true, branch: $(`#ab-${a.name}`).value, params: collect() }));
    $(`#as-${a.name}`).onclick = async () => out(`#ao-${a.name}`, await api(`/api/actions/${a.name}/submit`, { branch: $(`#ab-${a.name}`).value, params: collect() }));
  }
  main.append(el(`<h3>Submission log</h3>`),
    el(`<table><tr><th>time</th><th>action</th><th>mode</th><th>branch</th><th>edits applied</th></tr>${d.submissions.slice(-8).reverse().map((s) => `<tr><td class="mono">${esc(s.ts.slice(0, 19))}</td><td>${esc(s.actionType)}</td><td>${esc(s.mode)}</td><td>${esc(s.branch)}</td><td>${(s.appliedEdits ?? []).map((e) => `${esc(e.objectType)}×${e.count}`).join(', ') || '—'}</td></tr>`).join('')}</table>`));
};

TABS.workshop = async (main) => {
  const d = await api('/api/workshop');
  main.append(el(`<h2>Workshop — object-bound, no-code apps</h2>`),
    el(`<p class="note">Modules bind ontology objects, object sets and actions into an operational surface. The parts-maintenance app composes the classic pattern: watchlist + triage actions + trend analysis.</p>`));
  for (const app of d.apps) {
    main.append(el(`<div class="card"><h3>${esc(app.name)}</h3><p class="note">${esc(app.description ?? '')}</p></div>`));
    const grid = el(`<div class="two-col"></div>`);
    for (const m of app.modules ?? []) {
      const c = el(`<div class="card"><h3>${esc(m.title)} <span class="tag">${esc(m.kind)}</span></h3><div class="ws-body">loading…</div></div>`);
      grid.append(c);
      renderModule(c.querySelector('.ws-body'), m).catch((e) => { c.querySelector('.ws-body').innerHTML = `<span class="err">${esc(e.message)}</span>`; });
    }
    main.append(grid);
  }
};

async function renderModule(body, m) {
  if (m.kind === 'object-list') {
    const r = await api(`/api/objects/${m.objectType}?search=` + encodeURIComponent(m.filter ?? ''));
    if (!r.ok) { body.innerHTML = `<span class="err">${esc(r.error?.message)}</span>`; return; }
    const cols = r.objects.length ? Object.keys(r.objects[0]).slice(0, 6) : [];
    body.innerHTML = `<p class="note">${r.count} objects visible to your identity</p><table><tr>${cols.map((c) => `<th>${esc(c)}</th>`).join('')}</tr>${r.objects.slice(0, 8).map((o) => `<tr>${cols.map((c) => `<td>${o[c] === '[REDACTED]' ? '<span class="redacted">[REDACTED]</span>' : esc(o[c])}</td>`).join('')}</tr>`).join('')}</table>`;
  } else if (m.kind === 'action-button') {
    body.innerHTML = `<p class="note">${esc(m.description ?? '')}</p><div class="row"><div><label>object pk</label><input class="ab-pk" value="${esc(m.defaultPk ?? '')}"></div></div><button class="act green ab-go">${esc(m.action)}</button><div class="ab-out"></div>`;
    body.querySelector('.ab-go').onclick = async () => {
      const r = await api(`/api/actions/${m.action}/submit`, { params: { [m.param ?? 'part']: body.querySelector('.ab-pk').value, note: 'submitted from Workshop' } });
      body.querySelector('.ab-out').innerHTML = `<pre class="out">${esc(JSON.stringify(r, null, 2))}</pre>`;
    };
  } else if (m.kind === 'trend') {
    const r = await api('/api/objects/Part', {});
    if (!r.ok) { body.innerHTML = `<span class="err">${esc(r.error?.message)}</span>`; return; }
    const byBase = {};
    for (const o of r.objects) byBase[o.base] = (byBase[o.base] ?? 0) + 1;
    const max = Math.max(1, ...Object.values(byBase));
    body.innerHTML = Object.entries(byBase).map(([b, n]) => `<div class="bar-row"><span class="mono">${esc(b)}</span><div class="bar" style="width:${(n / max) * 100}%"></div><span class="mono">${n}</span></div>`).join('');
  }
}

TABS.analytics = async (main) => {
  main.append(el(`<h2>Analytics — ad-hoc query on governed data</h2>`),
    el(`<p class="note">Contour/Quiver-style top-down aggregation. Your identity's policies apply (try technician vs admin: JFK-only rows, masked emails).</p>`),
    el(`<div class="card"><div class="row">
      <div><label>dataset</label><input id="anDs" value="parts_enriched"></div>
      <div><label>group by (comma-sep)</label><input id="anGroup" value="base"></div>
      <div><label>measure column</label><input id="anCol" value="days_since_inspection"></div>
      <div><label>fn</label><select id="anFn"><option>avg</option><option>sum</option><option>count</option><option>max</option><option>min</option></select></div>
      <div><button class="act" id="anGo">Run</button></div>
    </div><div id="anOut"></div></div>`));
  $('#anGo').onclick = async () => {
    const groupBy = $('#anGroup').value.split(',').map((s) => s.trim()).filter(Boolean);
    const col = $('#anCol').value;
    const fn = $('#anFn').value;
    const r = await api('/api/analytics/query', { dataset: $('#anDs').value, groupBy, measures: [{ column: fn === 'count' ? null : col, fn, as: `${fn}_${col}` }] });
    out('#anOut', r);
  };
};

TABS.agent = async (main) => {
  const d = await api('/api/agents');
  const acts = await api('/api/actions');
  main.append(el(`<h2>AI agents — same ontology, same permissions</h2>`),
    el(`<p class="note">Agents are registered <b>users</b>. Their reads resolve through the same security engine (masking included), and their writes are Action submissions with the same validation. Grounding is live ontology data — not a flat document copy.</p>`));
  for (const ag of d.agents ?? []) {
    main.append(el(`<div class="card"><h3>${esc(ag.name)} <span class="badge agent">agent</span></h3>
      <p class="note">markings: ${(ag.markings ?? []).map((m) => `<span class="tag priv">${esc(m)}</span>`).join('')} · clearance ${ag.clearance}</p>
      <div class="row">
        <div><label>object type</label><select id="agot-${ag.id}">${(await api('/api/ontology')).objectTypes.map((o) => `<option>${o.name}</option>`).join('')}</select></div>
        <div><label>search</label><input id="agq-${ag.id}" value="high"></div>
        <div><button class="act" id="agr-${ag.id}">Query ontology</button></div>
        <div><button class="act ghost" id="agt-${ag.id}">MCP tool catalog</button></div>
      </div>
      <div class="row">
        <div><label>act via action type</label><select id="aga-${ag.id}">${acts.actionTypes.map((a) => `<option>${a.name}</option>`).join('')}</select></div>
        <div><label>target pk</label><input id="agp-${ag.id}" value="P-1005"></div>
        <div><button class="act green" id="agact-${ag.id}">Submit as agent</button></div>
      </div>
      <div id="ago-${ag.id}"></div></div>`));
    $(`#agr-${ag.id}`).onclick = async () => out(`#ago-${ag.id}`, await api(`/api/agents/${ag.id}/query`, { objectType: $(`#agot-${ag.id}`).value, search: $(`#agq-${ag.id}`).value }));
    $(`#agt-${ag.id}`).onclick = async () => out(`#ago-${ag.id}`, await api(`/api/agents/${ag.id}/tools`));
    $(`#agact-${ag.id}`).onclick = async () => out(`#ago-${ag.id}`, await api(`/api/agents/${ag.id}/act`, { action: $(`#aga-${ag.id}`).value, params: { part: $(`#agp-${ag.id}`).value, note: 'agent submission' } }));
  }
  main.append(el(`<h3>Agent activity log</h3>`),
    el(`<table><tr><th>time</th><th>agent</th><th>op</th><th>resource</th><th>decision</th><th>detail</th></tr>${(d.runs ?? []).slice(-10).reverse().map((r) => `<tr><td class="mono">${esc(r.ts.slice(11, 19))}</td><td>${esc(r.agentId)}</td><td>${esc(r.op)}</td><td>${esc(r.resource)}</td><td>${r.decision === 'allow' ? '<span class="pill allow">allow</span>' : `<span class="pill deny">deny · ${esc(r.errorCode ?? '')}</span>`}</td><td class="mono">${r.count !== undefined && r.count !== null ? r.count + ' hits' : ''}</td></tr>`).join('')}</table>`));
};

TABS.governance = async (main) => {
  const d = await api('/api/governance');
  main.append(el(`<h2>Governance — layered, compositional, uniform</h2>`),
    el(`<p class="note">Mandatory markings (conjunctive) + clearance (CBAC-style, per-property) + row policies (query-time) + property markings on the ontology. Exports leave the perimeter by design — decide that boundary deliberately.</p>`));
  main.append(el(`<h3>Markings</h3>`),
    el(`<table><tr><th>marking</th><th>description</th><th>members</th></tr>${d.markings.map((m) => `<tr><td><span class="tag priv">${esc(m.name)}</span></td><td>${esc(m.description)}</td><td>${(m.members ?? []).map((u) => `<span class="tag">${esc(u)}</span>`).join('')}</td></tr>`).join('')}</table>`));
  main.append(el(`<h3>Identities</h3>`),
    el(`<table><tr><th>id</th><th>kind</th><th>markings</th><th>clearance</th><th>attributes</th></tr>${d.users.map((u) => `<tr><td>${esc(u.id)}</td><td><span class="badge ${u.kind === 'agent' ? 'agent' : ''}">${esc(u.kind)}</span></td><td>${(u.markings ?? []).map((m) => `<span class="tag priv">${esc(m)}</span>`).join('')}</td><td class="num">${u.clearance}</td><td class="mono">${esc(JSON.stringify(u.attributes ?? {}))}</td></tr>`).join('')}</table>`));
  main.append(el(`<h3>Row policies (query-time filters)</h3>`),
    el(`<table><tr><th>dataset</th><th>expression</th></tr>${d.rowPolicies.map((p) => `<tr><td>${esc(p.datasetRid)}</td><td class="mono">${esc(p.expr)}</td></tr>`).join('')}</table>`));
  main.append(el(`<h3>Property policies (column-level)</h3>`),
    el(`<table><tr><th>dataset</th><th>property</th><th>required marking</th></tr>${d.propPolicies.map((p) => `<tr><td>${esc(p.datasetRid)}</td><td>${esc(p.property)}</td><td><span class="tag priv">${esc(p.requiredMarking)}</span></td></tr>`).join('')}</table>`));
  main.append(el(`<h3>Webhook outbox (side effects, with branch suppression)</h3>`),
    el(`<table><tr><th>time</th><th>action</th><th>webhook</th><th>branch</th></tr>${d.webhookOutbox.slice(-6).reverse().map((w) => `<tr><td class="mono">${esc(w.ts.slice(0, 19))}</td><td>${esc(w.action)}</td><td>${esc(w.webhook)}</td><td><span class="tag">${esc(w.branch)}</span></td></tr>`).join('') || '<tr><td colspan=4>empty</td></tr>'}</table>`));
};

TABS.releases = async (main) => {
  const d = await api('/api/releases');
  main.append(el(`<h2>Releases — ontology as versioned artifact</h2>`),
    el(`<p class="note">Schema, logic and governance definitions ride together in a bundle. Rollback restores a previous version and captures the current state as a safety version first. Validation refuses restores with dangling references.</p>`),
    el(`<div class="row"><div><button class="act green" id="capBundle">Capture new bundle</button></div></div><div id="relOut"></div>`));
  $('#capBundle').onclick = async () => { const r = await api('/api/releases', { note: 'captured from console' }); out('#relOut', r); if (r.ok) setTimeout(() => go(state.tab), 600); };
  main.append(el(`<table><tr><th>version</th><th>captured</th><th>note</th><th>contents</th><th></th></tr>${[...d.bundles].reverse().map((b) => `<tr><td><b>v${b.version}</b></td><td class="mono">${esc(b.ts.slice(0, 19))}</td><td>${esc(b.note)}</td><td>${Object.entries(b.counts ?? {}).filter(([, v]) => v > 0).map(([k, v]) => `<span class="tag">${k}:${v}</span>`).join('')}</td><td>
    <button class="act ghost rb" data-v="${b.version}">Rollback</button> <button class="act ghost mf" data-v="${b.version}">Manifest</button></td></tr>`).join('')}</table>`));
  for (const btn of main.querySelectorAll('.rb')) btn.onclick = async () => { out('#relOut', await api(`/api/releases/${btn.dataset.v}/rollback`, {})); toast('Rollback executed — definitions restored', false); setTimeout(() => go('releases'), 800); };
  for (const btn of main.querySelectorAll('.mf')) btn.onclick = async () => { const r = await api(`/api/releases/${btn.dataset.v}/manifest`); out('#relOut', r); };
};

// ── Boot ────────────────────────────────────────────────────────────────────
async function go(tab) {
  state.tab = tab;
  for (const b of document.querySelectorAll('nav button')) b.classList.toggle('active', b.dataset.tab === tab);
  const main = $('#main');
  main.innerHTML = '<p class="note">loading…</p>';
  try { await TABS[tab](main); }
  catch (e) { main.innerHTML = `<div class="card err">render failed: ${esc(e.message)}</div>`; }
}

async function boot() {
  const u = await api('/api/users');
  state.users = u.users ?? [];
  const sel = $('#userSelect');
  sel.innerHTML = state.users.map((x) => `<option value="${esc(x.id)}" ${x.id === state.user ? 'selected' : ''}>${esc(x.name)} (${esc(x.kind)})</option>`).join('');
  if (!state.users.some((x) => x.id === state.user)) state.user = state.users[0]?.id ?? 'admin';
  const syncBadges = () => {
    const me = state.users.find((x) => x.id === state.user);
    $('#userBadges').innerHTML = me ? `${me.kind === 'agent' ? '<span class="badge agent">agent</span>' : ''}${(me.markings ?? []).map((m) => `<span class="badge mark">${esc(m)}</span>`).join('')}<span class="badge">clearance ${me.clearance}</span>` : '';
  };
  sel.onchange = () => { state.user = sel.value; localStorage.setItem('mf-user', state.user); syncBadges(); go(state.tab); };
  syncBadges();
  for (const b of document.querySelectorAll('nav button')) b.onclick = () => go(b.dataset.tab);
  await go(state.tab);
}

boot();
