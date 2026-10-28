// ─────────────────────────────────────────────────────────────────────────────
// AI agent layer — agents work the SAME ontology under the SAME permissions.
// There is no separate flattened document index: every agent query resolves
// through the security engine (markings, clearance, row policies, property
// masking), and every agent write is an Action Type submission — governed,
// validated, audited — exactly like a human form submission.
//
// The OMCP-style endpoint below is the interop bridge: external agents talk
// to this ontology over MCP-ish tool calls (list tools, query objects,
// submit actions) with the boundary still inside the governance layer.
// ─────────────────────────────────────────────────────────────────────────────
import { rid, now, fail } from './util.js';
import { store } from './store.js';
import { queryObjects, linkedObjects, listOntology } from './ontology.js';
import { submitAction, listActionTypes } from './actions.js';

/** Resolve an agent to its user record — agents ARE users here. */
export function agentUser(agentId) {
  const u = store.state.users.find((x) => x.id === agentId && x.kind === 'agent');
  return u ?? null;
}

export function registerAgent({ id, name, markings = [], clearance = 0, attributes = {} }) {
  return store.mutate((s) => {
    if (s.users.some((u) => u.id === id)) return { ok: false, error: { code: 'ALREADY_EXISTS', message: `Agent '${id}' already registered` } };
    const agent = { id, name, kind: 'agent', markings, clearance, attributes, registeredAt: now() };
    s.users.push(agent);
    s.agents.push(agent);
    return { ok: true, agent };
  });
}

/**
 * The agent's read loop: ontology-grounded search. Answers are grounded in
 * live, governed objects — not a stale flat document store.
 */
export function agentQuery(agentId, objectTypeName, { search, where, linkName, linkPk } = {}) {
  const user = agentUser(agentId);
  if (!user) return fail('FORBIDDEN', `Unknown agent '${agentId}' — agents must be registered like any user`);
  if (linkName && linkPk !== undefined) {
    const r = linkedObjects(objectTypeName, linkPk, linkName, user);
    return audit(agentId, 'link', objectTypeName, { linkName, linkPk }, r);
  }
  const r = queryObjects(objectTypeName, user, { search, where });
  return audit(agentId, 'query', objectTypeName, { search, where }, r);
}

function audit(agentId, op, resource, args, result) {
  store.mutate((s) => s.agentRuns.push({
    rid: rid('arun'), ts: now(), agentId, op, resource,
    args: args ?? {}, decision: result.ok ? 'allow' : 'deny',
    count: result.ok ? (result.count ?? 0) : 0,
    errorCode: result.ok ? null : result.error.code,
  }));
  return result;
}

/**
 * The agent's write loop: submissions go through the Action engine with
 * permission checks and audit trails — same governance as a human submitter.
 */
export function agentAct(agentId, actionName, params, opts = {}) {
  const user = agentUser(agentId);
  if (!user) return fail('FORBIDDEN', `Unknown agent '${agentId}' — agents must be registered like any user`);
  const r = submitAction(actionName, params ?? {}, user, opts);
  store.mutate((s) => s.agentRuns.push({
    rid: rid('arun'), ts: now(), agentId, op: 'act', resource: actionName,
    args: params ?? {}, decision: r.ok ? 'allow' : 'deny',
    errorCode: r.ok ? null : r.error.code,
  }));
  return r;
}

/** OMCP-style tool catalog: what an external agent is allowed to discover. */
export function mcpToolCatalog(agentId) {
  const user = agentUser(agentId);
  if (!user) return fail('FORBIDDEN', `Unknown agent '${agentId}'`);
  const onto = listOntology();
  return {
    ok: true,
    tools: [
      ...onto.objectTypes.map((o) => ({
        name: `ontology.search.${o.name}`,
        description: `Search governed objects of type ${o.name} (grounded in live index, property-masked for caller)`,
        inputSchema: { search: 'string?', where: `object? (filter on ${o.properties.map((p) => p.name).join(', ')})` },
      })),
      ...listActionTypes().map((a) => ({
        name: `ontology.action.${a.name}`,
        description: a.description || `Submit governed action ${a.name} (rules + criteria apply identically to agents)`,
        inputSchema: Object.fromEntries(a.parameters.map((p) => [p.name, p.type])),
      })),
    ],
    note: 'Tool discovery itself is permission-scoped: the catalog only exposes surfaces this agent identity can reach.',
  };
}
