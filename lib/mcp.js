// ─────────────────────────────────────────────────────────────────────────────
// The MCP boundary — two categorically different capability grants:
//
//   OMCP (Ontology MCP) — for ontology CONSUMERS. Reads objects via a SQL
//   tool scoped to the application's exposed types, and writes data ONLY by
//   invoking predefined Action Types (each exposed action becomes its own
//   dedicated tool). Includes the "request clarification" reflexive tool.
//   Per-call enforcement is layered UNDER the scope: user/agent, object,
//   field, and action permissions are verified on every query — the MCP
//   boundary restricts in addition to, never instead of, the permission system.
//
//   PMCP (Palantir MCP) — for ontology BUILDERS. 70+-equivalent schema tools:
//   define/modify object types, link types, action types, functions. HARD
//   CONSTRAINT: it cannot write ontology DATA — structure only. Auth is
//   OAuth-style: authorization-code grant (act as a human delegate) vs
//   client-credentials grant (act as a service).
//
//   The complementary boundary is the point: an OMCP agent can mutate data but
//   never redefine what an action is allowed to do; a PMCP agent can redefine
//   structure but never touch a production row. A compromised data-writing
//   agent cannot escalate into a schema-modifying one, and vice versa.
// ─────────────────────────────────────────────────────────────────────────────
import { rid, now, fail } from './util.js';
import { store } from './store.js';
import { queryObjects, listOntology, defineObjectType, defineLinkType, maskProperties } from './ontology.js';
import { submitAction, listActionTypes, defineActionType } from './actions.js';
import { defineFunction } from './functions.js';
import { validateSchemaContract, normalizeObjectTypeDef, changePrimaryKey } from './physical.js';
import { createDataset } from './datasets.js';
import { createPipeline } from './pipelines.js';

// ── OMCP applications ────────────────────────────────────────────────────────

/**
 * Register an OMCP application: an explicit, curated exposure surface.
 * Scoping happens at configuration time — the agent builder decides which
 * object types/properties and which action types exist for the model at all.
 */
export function registerMcpApp({ name, description, objectScopes, actionTools, allowedIdentities }) {
  if (!name) return fail('BAD_APP', 'application needs a name');
  return store.mutate((s) => {
    if (s.mcpApps.some((a) => a.name === name)) return fail('ALREADY_EXISTS', `MCP application '${name}' already exists`);
    const app = {
      rid: rid('app'), name, description: description ?? '',
      objectScopes: (objectScopes ?? []).map((o) => ({
        objectType: o.objectType,
        // property allowlist: token-efficiency AND a real security boundary
        properties: o.properties ?? null, // null = all permitted by policies
      })),
      actionTools: actionTools ?? [],
      allowedIdentities: allowedIdentities ?? null, // null = any registered identity
      createdAt: now(),
    };
    s.mcpApps.push(app);
    return { ok: true, app };
  });
}

function omcpAudit(appName, identity, tool, args, decision, detail) {
  store.mutate((s) => s.omcpCalls.push({ rid: rid('omcp'), ts: now(), app: appName, identity, tool, args, decision, detail: detail ?? null }));
}

/** MCP tool catalog for an identity against an app. */
export function omcpToolCatalog(appName, identityId) {
  const app = store.state.mcpApps.find((a) => a.name === appName);
  if (!app) return fail('NOT_FOUND', `MCP application '${appName}' not found`);
  const user = store.state.users.find((u) => u.id === identityId);
  if (!user) return fail('FORBIDDEN', 'unknown identity');
  if (app.allowedIdentities && !app.allowedIdentities.includes(identityId)) {
    omcpAudit(appName, identityId, 'tools/list', {}, 'deny', 'identity not in application scope');
    return fail('FORBIDDEN', `identity '${identityId}' is not scoped to application '${appName}'`);
  }
  const onto = listOntology();
  const tools = [
    {
      name: 'request_clarification',
      description: 'Pause and ask the user for clarification instead of guessing. Reflexive, always available.',
      inputSchema: { question: 'string', missing: 'string[]?' },
    },
    {
      name: 'ontology.sql',
      description: `Read ontology data via SQL-style queries against exposed object types: ${app.objectScopes.map((o) => o.objectType).join(', ')}. Verifies user/agent, object, field, and action permissions before every query.`,
      inputSchema: { query: 'string (SELECT ... FROM <ObjectType> WHERE ...)' },
    },
    ...app.actionTools.map((actionName) => {
      const at = listActionTypes().find((a) => a.name === actionName);
      return {
        name: `action.${actionName}`,
        description: at?.description ?? `Execute predefined action ${actionName}`,
        inputSchema: Object.fromEntries((at?.parameters ?? []).filter((p) => !p.source?.kind).map((p) => [p.name, p.type ?? 'string'])),
      };
    }),
  ];
  omcpAudit(appName, identityId, 'tools/list', {}, 'allow', `${tools.length} tools`);
  return { ok: true, server: 'ontolog mcp (omcp)', app: appName, tools };
}

/**
 * A tiny SQL-ish surface: SELECT <props|*> FROM <ObjectType> [WHERE search].
 * Scope + per-call layered permission checks, then the shared query path.
 */
export function omcpSql(appName, identityId, { query }) {
  const app = store.state.mcpApps.find((a) => a.name === appName);
  if (!app) return fail('NOT_FOUND', `MCP application '${appName}' not found`);
  const user = store.state.users.find((u) => u.id === identityId);
  if (!user) return fail('FORBIDDEN', 'unknown identity');
  if (app.allowedIdentities && !app.allowedIdentities.includes(identityId)) {
    omcpAudit(appName, identityId, 'ontology.sql', { query }, 'deny', 'identity not scoped');
    return fail('FORBIDDEN', `identity '${identityId}' is not scoped to application '${appName}'`);
  }
  const m = /^\s*select\s+(.+?)\s+from\s+([A-Za-z_]\w*)\s*(?:where\s+(.+?))?\s*;?\s*$/is.exec(query ?? '');
  if (!m) {
    omcpAudit(appName, identityId, 'ontology.sql', { query }, 'deny', 'unparseable');
    return fail('BAD_QUERY', 'only SELECT <props|*> FROM <ObjectType> [WHERE <search>] is supported');
  }
  const [, cols, typeName, whereText] = m;
  const scope = app.objectScopes.find((o) => o.objectType === typeName);
  if (!scope) {
    omcpAudit(appName, identityId, 'ontology.sql', { query }, 'deny', 'object type not in application scope');
    return fail('FORBIDDEN', `object type '${typeName}' is not exposed by application '${appName}' — scope is fixed at configuration time`);
  }
  const r = queryObjects(typeName, user, { search: whereText ?? undefined });
  if (!r.ok) {
    omcpAudit(appName, identityId, 'ontology.sql', { query }, 'deny', r.error.code);
    return r;
  }
  // Field-level scope: the app's property allowlist intersects the caller's
  // policy-visible properties. Property policies already applied by the query
  // layer ([REDACTED]); scope can narrow further, never widen.
  const ot = store.state.objectTypes.find((o) => o.name === typeName);
  let objects = r.objects;
  const requested = cols.trim() === '*' ? null : cols.split(',').map((c) => c.trim().replace(/["'`]/g, ''));
  const allowed = scope.properties ?? null;
  const project = (o) => {
    const out = {};
    for (const k of Object.keys(o)) {
      if (requested && !requested.includes(k)) continue;
      if (allowed && !allowed.includes(k)) continue;
      out[k] = o[k];
    }
    return out;
  };
  objects = objects.map(project);
  omcpAudit(appName, identityId, 'ontology.sql', { query }, 'allow', `${objects.length} rows`);
  return { ok: true, server: 'omcp', objectType: typeName, count: objects.length, objects };
}

/** Invoke an exposed action tool: predefined action only, governance intact. */
export function omcpInvokeAction(appName, identityId, actionName, params, opts = {}) {
  const app = store.state.mcpApps.find((a) => a.name === appName);
  if (!app) return fail('NOT_FOUND', `MCP application '${appName}' not found`);
  const user = store.state.users.find((u) => u.id === identityId);
  if (!user) return fail('FORBIDDEN', 'unknown identity');
  if (app.allowedIdentities && !app.allowedIdentities.includes(identityId)) {
    omcpAudit(appName, identityId, `action.${actionName}`, params ?? {}, 'deny', 'identity not scoped');
    return fail('FORBIDDEN', `identity '${identityId}' is not scoped to application '${appName}'`);
  }
  if (!app.actionTools.includes(actionName)) {
    omcpAudit(appName, identityId, `action.${actionName}`, params ?? {}, 'deny', 'action not exposed as a tool');
    return fail('FORBIDDEN', `action '${actionName}' is not an exposed tool of '${appName}' — agents get predefined actions, never generic write access`);
  }
  const r = submitAction(actionName, params ?? {}, user, opts);
  omcpAudit(appName, identityId, `action.${actionName}`, params ?? {}, r.ok ? 'allow' : 'deny', r.ok ? `applied: ${(r.appliedEdits ?? []).length} edit group(s)` : r.error.code);
  return r;
}

// ── PMCP — the builder's server ─────────────────────────────────────────────

/** OAuth-style session: acting as a human delegate or as a service. */
export function pmcpConnect({ grantType, userId, serviceId, scopes }) {
  if (!['authorization_code', 'client_credentials'].includes(grantType)) {
    return fail('BAD_GRANT', 'grant_type must be authorization_code (user-level) or client_credentials (service-level)');
  }
  if (grantType === 'authorization_code' && !store.state.users.some((u) => u.id === userId)) {
    return fail('FORBIDDEN', `authorization_code grant requires an existing user; '${userId}' not found`);
  }
  const grantedScopes = [...new Set(scopes ?? ['schema:read', 'schema:write'])];
  const allowedScopes = new Set(['schema:read', 'schema:write']);
  const invalidScope = grantedScopes.find((scope) => !allowedScopes.has(scope));
  if (invalidScope) return fail('BAD_SCOPE', `Unsupported PMCP scope '${invalidScope}'`);
  const session = {
    rid: rid('pmcp'), grantType,
    identity: grantType === 'authorization_code' ? userId : (serviceId ?? 'service-client'),
    kind: grantType === 'authorization_code' ? 'user-delegate' : 'service',
    scopes: grantedScopes,
    connectedAt: now(),
    calls: [],
  };
  store.mutate((s) => s.pmcpSessions.push(session));
  return { ok: true, session: { rid: session.rid, identity: session.identity, kind: session.kind, scopes: session.scopes } };
}

const PMCP_TOOLS = [
  { name: 'ontology.define_object_type', writes: 'schema', scope: 'schema:write' },
  { name: 'ontology.change_primary_key', writes: 'schema', scope: 'schema:write' },
  { name: 'ontology.define_link_type', writes: 'schema', scope: 'schema:write' },
  { name: 'ontology.define_action_type', writes: 'schema', scope: 'schema:write' },
  { name: 'ontology.define_function', writes: 'schema', scope: 'schema:write' },
  { name: 'ontology.validate_schema_contract', writes: 'none', scope: 'schema:read' },
  { name: 'dataset.create', writes: 'schema', scope: 'schema:write' },
  { name: 'pipeline.define', writes: 'schema', scope: 'schema:write' },
];

export function pmcpToolCatalog(sessionRid) {
  const s = store.state.pmcpSessions.find((x) => x.rid === sessionRid);
  if (!s) return fail('NOT_FOUND', 'PMCP session not found');
  return {
    ok: true,
    server: 'palantir mcp (pmcp)',
    session: { identity: s.identity, kind: s.kind },
    tools: PMCP_TOOLS.filter((tool) => s.scopes.includes(tool.scope)).map((t) => ({ name: t.name, description: `Schema/structure tool — can modify ontology structure, cannot write ontology data (${t.writes}; ${t.scope})` })),
    hardConstraint: 'PMCP cannot write ontology data. It modifies structure only.',
  };
}

/**
 * Dispatch a PMCP tool call. Schema tools run for real; any attempt at data
 * mutation is refused by the structural boundary, not by policy hopes.
 */
export function pmcpCall(sessionRid, toolName, args = {}) {
  const s = store.state.pmcpSessions.find((x) => x.rid === sessionRid);
  if (!s) return fail('NOT_FOUND', 'PMCP session not found');
  const tool = PMCP_TOOLS.find((t) => t.name === toolName);
  if (!tool) {
    const DATA_WRITE_TOOL = /^(?:action\.|objects?\.(?:create|modify|delete|write)|(?:ontology\.)?(?:data|objects?)\.(?:write|create|modify|delete)|dataset\.(?:append|write|commit)|submit)/i;
    if (DATA_WRITE_TOOL.test(toolName)) return fail('PMCP_DATA_WRITE_FORBIDDEN', `PMCP cannot write ontology data — '${toolName}' is a data surface. Use an OMCP application with predefined Action tools.`);
    return fail('NOT_FOUND', `tool '${toolName}' does not exist on PMCP`);
  }
  const entry = { ts: now(), tool: toolName, args, decision: 'allow' };
  if (!s.scopes.includes(tool.scope)) {
    entry.decision = 'refused';
    entry.note = `requires scope ${tool.scope}`;
    store.mutate(() => s.calls.push(entry));
    return fail('FORBIDDEN', `PMCP session lacks required scope '${tool.scope}' for '${toolName}'`);
  }

  // The structural refusal: data-write tools simply do not exist here.
  const DATA_WRITE_TOOL = /^(?:action\.|objects?\.(?:create|modify|delete|write)|(?:ontology\.)?(?:data|objects?)\.(?:write|create|modify|delete)|dataset\.(?:append|write|commit)|submit)/i;
  if (DATA_WRITE_TOOL.test(toolName)) {
    entry.decision = 'refused';
    entry.note = 'PMCP cannot write ontology data';
    store.mutate(() => s.calls.push(entry));
    return fail('PMCP_DATA_WRITE_FORBIDDEN', `PMCP cannot write ontology data — '${toolName}' is a data surface. Use an OMCP application with predefined Action tools.`);
  }

  let result;
  try {
    switch (toolName) {
      case 'ontology.define_object_type': result = defineObjectType(normalizeOtArgs(args)); result = { ok: true, objectType: result.name }; break;
      case 'ontology.change_primary_key': result = changePrimaryKey(args.objectType, args.newPrimaryKey, { confirmEditLoss: args.confirmEditLoss === true }); break;
      case 'ontology.define_link_type': result = defineLinkType(args); result = { ok: true, linkType: result.name }; break;
      case 'ontology.define_action_type': result = defineActionType(args); result = { ok: true, actionType: result.name }; break;
      case 'ontology.define_function': result = defineFunction(args); result = { ok: true, function: result.name }; break;
      case 'ontology.validate_schema_contract': result = validateSchemaContract(args.objectType); break;
      case 'dataset.create': result = { ok: true, dataset: createDataset(args).name }; break;
      case 'pipeline.define': result = { ok: true, pipeline: createPipeline(args).name }; break;
      default:
        result = fail('NOT_IMPLEMENTED', `'${toolName}' accepted but not wired in this miniature`);
    }
  } catch (e) {
    result = fail(e.code ?? 'ERROR', e.message);
  }
  if (!result.ok) entry.decision = 'error';
  store.mutate(() => s.calls.push(entry));
  return result;
}

function normalizeOtArgs(args) {
  // PMCP-level object type definitions go through the same physical model.
  return normalizeObjectTypeDef(args);
}

export function listMcpApps() {
  const s = store.state;
  return {
    apps: s.mcpApps.map((a) => ({ ...structuredClone(a) })),
    omcpCalls: s.omcpCalls.slice(-40),
    pmcpSessions: s.pmcpSessions.map((x) => ({ rid: x.rid, grantType: x.grantType, identity: x.identity, kind: x.kind, scopes: x.scopes, connectedAt: x.connectedAt, calls: x.calls.slice(-10) })),
  };
}
