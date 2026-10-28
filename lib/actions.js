// Governed object write transactions compiled from Action Type rules.
import { rid, now, PlatformError, fail, coerce, tryExpr } from './util.js';
import { store } from './store.js';
import { applyUserEdits, upsertLinks, queryObjects, validateUserEdits, validateLinkEdits } from './ontology.js';
import { canReadDataset } from './security.js';
import { runFunction } from './functions.js';
import { resolveInterfaceParam } from './interfaces.js';

export function defineActionType({ name, description, parameters = [], rules = [], submissionCriteria = [], overrides = [], writeAuthorization, sideEffectsOnBranch = false }) {
  if (!name) throw new PlatformError('BAD_ACTION_TYPE', 'action needs a name');
  if (store.state.actionTypes.some((action) => action.name === name)) throw new PlatformError('ALREADY_EXISTS', `Action type '${name}' already exists`);
  const errors = validateDefinition({ parameters, rules, overrides });
  if (errors.length) throw new PlatformError('BAD_ACTION_TYPE', `Invalid action type '${name}': ${errors.join('; ')}`);
  return store.mutate((s) => {
    const action = { rid: rid('at'), name, description: description ?? '', parameters, rules, submissionCriteria, overrides, writeAuthorization, sideEffectsOnBranch, createdAt: now() };
    s.actionTypes.push(action);
    return action;
  });
}

function validateDefinition({ parameters, rules, overrides }) {
  const errors = [];
  const names = parameters.map((parameter) => parameter.name);
  if (new Set(names).size !== names.length) errors.push('parameter names must be unique');
  if (rules.some((rule) => rule.kind === 'function') && rules.length > 1) errors.push('a function rule is total: no other rule may be configured alongside it');
  for (const rule of rules) {
    if (rule.kind === 'function' && !rule.function) errors.push('function rule must name a function');
    if (rule.kind === 'modify' && !rule.objectParameter) errors.push('modify rule needs an objectParameter');
    if (rule.kind === 'modify' && !rule.interface && !rule.objectType && !parameters.some((p) => p.name === rule.objectParameter && p.objectType)) errors.push(`modify rule '${rule.objectParameter}' needs a concrete object type`);
    if (rule.kind === 'delete' && !rule.objectParameter) errors.push('delete rule needs an objectParameter');
    if (rule.kind === 'create' && !rule.objectType) errors.push('create rule needs an objectType');
    if (rule.kind === 'webhook' && !rule.webhookName) errors.push('webhook rule needs webhookName');
    if (rule.kind === 'link' && (!rule.linkType || !rule.fromParameter || !rule.toParameter)) errors.push('link rule needs linkType, fromParameter, toParameter');
  }
  for (const override of overrides ?? []) {
    for (const condition of override.conditions ?? []) if (condition.param && !names.includes(condition.param)) errors.push(`override condition references unknown parameter '${condition.param}'`);
    for (const target of override.then ?? []) if (target.param !== undefined && !names.includes(target.param)) errors.push(`override targets unknown parameter '${target.param}'`);
  }
  return errors;
}

export function submitAction(actionName, rawParams, user, opts = {}) {
  const { validateOnly = false, returnEdits = false, branch = 'master' } = opts;
  if (!user?.id) return fail('FORBIDDEN', 'an acting identity is required');
  if (validateOnly && returnEdits) return fail('BAD_FLAGS', 'validateOnly and returnEdits cannot be used together');
  const stored = store.state.actionTypes.find((action) => action.name === actionName);
  if (!stored) return fail('NOT_FOUND', `Action type '${actionName}' not found`);
  const action = structuredClone(stored);
  const trace = { resolves: [], overrides: [], checks: [], criteria: [], rules: [], effects: [], edits: [] };
  const note = (section, entry) => trace[section].push({ ts: now(), ...entry });
  let params;
  try { params = resolveParameters(action, rawParams ?? {}, user, note); }
  catch (error) { return fail(error.code ?? 'BAD_ARGS', error.message, { trace }); }
  applyOverrides(action, params, user, note);
  for (const parameter of action.parameters) {
    if (params.__visible__.get(parameter.name) !== false && (parameter.required || parameter.__reqOverride === true) && isEmpty(params[parameter.name])) return fail('MISSING_PARAM', `Parameter '${parameter.name}' is required`, { trace });
  }

  const auth = authorizeSubmission(action, params, user, trace, note);
  if (!auth.ok) return { ...auth, trace };
  const plan = compilePlan(action, params, user, trace, note);
  if (!plan.ok) return { ...plan, trace };
  const order = validateOrdering(plan.edits);
  note('rules', { step: 'ordering-validation', ...order });
  if (!order.ok) return fail('ORDERING', order.reason, { trace });
  const preflight = preflightEdits(plan.edits);
  if (!preflight.ok) return { ...preflight, trace };
  if (validateOnly) return { ok: true, mode: 'validateOnly', wouldPass: true, proposedEditCount: plan.edits.length, proposedEdits: plan.edits, trace };

  // All static failures are checked before effects. The transaction is an
  // unexpected-error safety net, so a failed write batch cannot leave a
  // webhook or earlier object edit committed by itself.
  let result;
  store.transaction(() => {
    runSideEffects(action, params, user, branch, note);
    const expectedVersions = mergeVersionMaps(auth.objectVersions, plan.observedVersions, opts.expectedVersions);
    const applied = commitEdits(plan.edits, note, action.name, expectedVersions);
    result = { ok: true, mode: returnEdits ? 'returnEdits' : 'apply', actionType: action.name, branch, appliedEdits: applied, ...(returnEdits ? { edits: plan.edits } : {}), trace, ts: now() };
    store.state.actionSubmissions.push(result);
  });
  return result;
}

const isEmpty = (value) => value === undefined || value === null;

function resolveParameters(action, raw, user, note) {
  const params = { __visible__: new Map(), __user: user.id };
  for (const parameter of action.parameters) {
    let value;
    switch (parameter.source?.kind) {
      case 'static': value = parameter.source.value; break;
      case 'currentUser': value = user.id; break;
      case 'currentTime': value = now(); break;
      default: value = raw[parameter.name];
    }
    if (!isEmpty(value)) value = coerce(value, parameter.type ?? 'string');
    if (parameter.type === 'object-reference' && value && typeof value === 'object') value = String(value.pk ?? '');
    params[parameter.name] = value;
    params.__visible__.set(parameter.name, true);
    note('resolves', { param: parameter.name, source: parameter.source?.kind ?? 'from-parameter', value });
  }
  return params;
}

function applyOverrides(action, params, user, note) {
  for (const override of action.overrides ?? []) {
    let matches = true;
    for (const condition of override.conditions ?? []) {
      const scope = Object.fromEntries(action.parameters.map((p) => [p.name, params[p.name]]));
      scope.user = { attributes: user.attributes ?? {} };
      const result = tryExpr(condition.expr, scope);
      if (!result.ok || !result.value) { matches = false; break; }
    }
    if (!matches) continue;
    for (const effect of override.then ?? []) {
      if (effect.visible !== undefined) params.__visible__.set(effect.param, effect.visible);
      if (effect.required !== undefined) {
        const parameter = action.parameters.find((p) => p.name === effect.param);
        if (parameter) parameter.__reqOverride = effect.required;
      }
      if (effect.default !== undefined && isEmpty(params[effect.param])) params[effect.param] = effect.default;
      note('overrides', { override: override.name ?? 'override', then: effect });
    }
  }
}

function authorizeSubmission(action, params, user, trace, note) {
  const touched = new Set();
  const objectVersions = Object.create(null);
  for (const rule of action.rules) {
    if (rule.objectType) touched.add(rule.objectType);
    if (rule.interface && rule.typeParameter) {
      const concrete = params[rule.typeParameter];
      if (!concrete) return fail('MISSING_PARAM', `concrete object type '${rule.typeParameter}' is required`, { trace });
      touched.add(concrete);
    }
    if (rule.objectParameter) {
      const parameter = action.parameters.find((p) => p.name === rule.objectParameter);
      const targetType = rule.objectType ?? parameter?.objectType;
      if (targetType) touched.add(targetType);
    }
    if (rule.kind === 'link') {
      const link = store.state.linkTypes.find((item) => item.name === rule.linkType);
      if (!link) return fail('NOT_FOUND', `Link type '${rule.linkType}' not found`, { trace });
      touched.add(link.fromType);
      touched.add(link.toType);
    }
  }
  for (const typeName of touched) {
    const type = store.state.objectTypes.find((item) => item.name === typeName);
    if (!type) return fail('NOT_FOUND', `Object type '${typeName}' not found`, { trace });
    const dataset = type.backing?.datasetRid ? store.state.datasets.find((item) => item.rid === type.backing.datasetRid) : null;
    const permission = canReadDataset(user, dataset ?? { rid: type.rid, name: type.name, markings: type.markings ?? [] });
    note('checks', { layer: 'read-permission', resource: typeName, decision: permission.ok ? 'allow' : 'deny' });
    if (!permission.ok) return { ...permission, error: { ...permission.error, message: `Submitting this action requires read access to '${typeName}': ${permission.error.message}` } };
  }
  const required = action.writeAuthorization?.requiredMarking;
  if (required && !(user.markings ?? []).includes(required)) return fail('FORBIDDEN', `Write authorization requires marking '${required}'`, { trace });
  note('checks', { layer: 'write-authorization', decision: 'allow' });
  const scope = {};
  for (const p of action.parameters) scope[p.name] = params[p.name];
  scope.user = { id: user.id, kind: user.kind, attributes: user.attributes ?? {} };
  scope.now = now();
  for (const p of action.parameters) {
    if (p.type === 'object-reference' && params[p.name] && p.objectType) {
      const query = queryObjects(p.objectType, user, { objectPk: params[p.name], includeVersions: true });
      if (!query.ok) return query;
      if (query.objectVersions) {
        objectVersions[p.objectType] ??= Object.create(null);
        Object.assign(objectVersions[p.objectType], query.objectVersions);
      }
      const object = query.objects[0];
      if (!object) return fail('NOT_FOUND', `Object '${p.objectType}/${params[p.name]}' is not visible`, { trace });
      scope[p.name] = object;
    }
  }
  for (const criterion of action.submissionCriteria ?? []) {
    const result = tryExpr(criterion.expr, scope);
    if (!result.ok || !result.value) return fail('CRITERIA_FAILED', criterion.errorMessage ?? `Submission criteria failed: ${criterion.expr}`, { trace });
    note('criteria', { expr: criterion.expr, pass: true });
  }
  note('checks', { layer: 'submission-criteria', decision: 'allow', count: action.submissionCriteria?.length ?? 0 });
  return { ok: true, objectVersions };
}

function compilePlan(action, params, user, trace, note) {
  const edits = [];
  const observedVersions = Object.create(null);
  for (const rule of action.rules) {
    switch (rule.kind) {
      case 'create': {
        const type = store.state.objectTypes.find((item) => item.name === rule.objectType);
        if (!type) return fail('NOT_FOUND', `Object type '${rule.objectType}' not found`, { trace });
        const pkName = rule.primaryKeyFrom ?? type.primaryKey;
        const pk = params[pkName];
        if (isEmpty(pk)) return fail('MISSING_PK', `Create rule needs a value for primary key '${pkName}'`, { trace });
        const patch = {};
        for (const [property, mapping] of Object.entries(rule.propertyMap ?? {})) {
          const value = resolveMapping(mapping, params);
          if (value !== undefined) patch[property] = value;
        }
        patch[type.primaryKey] = String(pk);
        edits.push({ kind: 'create', objectType: type.name, pk: String(pk), patch, rule: 'create' });
        break;
      }
      case 'modify':
      case 'delete': {
        let typeName = rule.objectType;
        if (rule.interface) {
          typeName = params[rule.typeParameter];
          const resolved = resolveInterfaceParam(rule.interface, typeName, params);
          if (!resolved.ok) return resolved;
        } else if (!typeName) {
          typeName = action.parameters.find((p) => p.name === rule.objectParameter)?.objectType;
        }
        if (!typeName) return fail('BAD_ACTION_TYPE', `Rule '${rule.kind}' must resolve a target object type`, { trace });
        const pk = params[rule.objectParameter];
        if (isEmpty(pk)) return fail('MISSING_PARAM', `Parameter '${rule.objectParameter}' must reference an object`, { trace });
        if (rule.kind === 'delete') edits.push({ kind: 'delete', objectType: typeName, pk: String(pk), rule: 'delete' });
        else {
          const mapping = rule.interface ? resolveInterfaceParam(rule.interface, typeName, params).mapping : null;
          const patch = {};
          for (const [property, valueMapping] of Object.entries(rule.propertyMap ?? {})) {
            const value = resolveMapping(valueMapping, params);
            if (value === undefined) continue;
            const propertyName = mapping ? mapping.find((m) => m.interfaceProperty === property)?.localProperty : property;
            if (!propertyName) return fail('INTERFACE_MAPPING', `Interface property '${property}' is not mapped on '${typeName}'`, { trace });
            patch[propertyName] = value;
          }
          edits.push({ kind: 'modify', objectType: typeName, pk: String(pk), patch, rule: 'modify' });
        }
        break;
      }
      case 'link': {
        const from = params[rule.fromParameter], to = params[rule.toParameter];
        if (isEmpty(from) || isEmpty(to)) return fail('MISSING_PARAM', `Link '${rule.linkType}' requires both object references`, { trace });
        edits.push({ kind: 'link', linkType: rule.linkType, fromPk: String(from), toPk: String(to), rule: 'link' });
        break;
      }
      case 'function': {
        const args = Object.fromEntries(Object.entries(rule.paramMap ?? {}).map(([name, mapping]) => [name, resolveMapping(mapping, params)]));
        const result = runFunction(rule.function, args, { mode: 'dry_run', user });
        if (!result.ok) return fail('FUNCTION_FAILED', `Function '${rule.function}' failed: ${result.error}`, { trace });
        for (const [type, versions] of Object.entries(result.observedVersions ?? {})) {
          observedVersions[type] ??= Object.create(null);
          Object.assign(observedVersions[type], versions);
        }
        for (const edit of result.proposedEdits) edits.push(edit.op === 'link'
          ? { kind: 'link', linkType: edit.type, fromPk: edit.pk, toPk: edit.patch.toPk, rule: 'function' }
          : { kind: edit.op, objectType: edit.type, pk: edit.pk, patch: edit.patch, rule: 'function' });
        note('rules', { rule: 'function', function: rule.function, proposedEdits: result.proposedEdits.length });
        break;
      }
      case 'notify':
      case 'webhook':
        break;
      default: return fail('BAD_RULE', `Unknown rule kind '${rule.kind}'`, { trace });
    }
  }
  return { ok: true, edits, observedVersions };
}

function mergeVersionMaps(...maps) {
  const result = Object.create(null);
  for (const map of maps) {
    for (const [type, versions] of Object.entries(map ?? {})) {
      result[type] ??= Object.create(null);
      Object.assign(result[type], versions ?? {});
    }
  }
  return result;
}

function resolveMapping(mapping, params) {
  if (mapping === undefined || mapping === null) return undefined;
  if (typeof mapping === 'object') {
    switch (mapping.kind) {
      case 'fromParameter': return params[mapping.param];
      case 'static': return mapping.value;
      case 'currentUser': return params.__user;
      case 'currentTime': return now();
      default: return undefined;
    }
  }
  return params[mapping];
}

function validateOrdering(edits) {
  const created = new Set();
  for (const edit of edits) {
    if (edit.kind === 'link') continue;
    const key = `${edit.objectType}:${edit.pk}`;
    if (edit.kind === 'create') {
      if (created.has(key)) return { ok: false, reason: `object created twice in one submission (${key})` };
      created.add(key);
    } else if (created.has(key)) return { ok: false, reason: `${edit.kind} cannot target an object created in the same submission` };
  }
  return { ok: true };
}

function preflightEdits(edits) {
  const grouped = new Map();
  for (const edit of edits) if (edit.kind !== 'link') {
    if (!grouped.has(edit.objectType)) grouped.set(edit.objectType, []);
    grouped.get(edit.objectType).push({ pk: edit.pk, op: edit.kind, patch: edit.patch });
  }
  const plannedCreates = edits.filter((edit) => edit.kind === 'create').map(({ objectType, pk }) => ({ objectType, pk }));
  for (const [typeName, rows] of grouped) {
    const check = validateUserEdits(typeName, rows, { plannedCreates });
    if (!check.ok) return check;
  }
  const linkGroups = new Map();
  for (const edit of edits) if (edit.kind === 'link') {
    if (!linkGroups.has(edit.linkType)) linkGroups.set(edit.linkType, []);
    linkGroups.get(edit.linkType).push({ fromPk: edit.fromPk, toPk: edit.toPk });
  }
  for (const [linkName, rows] of linkGroups) {
    const check = validateLinkEdits(linkName, rows, { plannedCreates });
    if (!check.ok) return check;
  }
  return { ok: true };
}

function runSideEffects(action, params, _user, branch, note) {
  for (const rule of action.rules) if (rule.kind === 'webhook' || rule.kind === 'notify') {
    const suppressed = branch !== 'master' && !action.sideEffectsOnBranch;
    note('effects', { kind: rule.kind === 'webhook' ? 'webhook' : 'notification', name: rule.webhookName ?? rule.to ?? 'ops', executed: !suppressed, suppressed });
    if (!suppressed && rule.kind === 'webhook') {
      const safe = Object.fromEntries(action.parameters.map((p) => [p.name, params[p.name]]));
      store.mutate((s) => s.webhookOutbox.push({ ts: now(), action: action.name, webhook: rule.webhookName, branch, params: safe }));
    }
  }
}

function commitEdits(edits, note, actionName, expectedVersions = {}) {
  const byType = new Map();
  for (const edit of edits) if (edit.kind !== 'link') {
    if (!byType.has(edit.objectType)) byType.set(edit.objectType, []);
    byType.get(edit.objectType).push({ pk: edit.pk, op: edit.kind, patch: edit.patch });
  }
  const plannedCreates = edits.filter((edit) => edit.kind === 'create').map(({ objectType, pk }) => ({ objectType, pk }));
  for (const [typeName, rows] of byType) {
    const result = applyUserEdits(typeName, rows, { actionType: actionName, plannedCreates, expectedVersions: expectedVersions[typeName] });
    note('edits', { objectType: typeName, applied: rows.length, editDataset: result.editDataset, writebackDataset: result.writebackDataset });
  }
  const linkGroups = new Map();
  for (const edit of edits) if (edit.kind === 'link') {
    if (!linkGroups.has(edit.linkType)) linkGroups.set(edit.linkType, []);
    linkGroups.get(edit.linkType).push({ fromPk: edit.fromPk, toPk: edit.toPk });
  }
  for (const [linkName, rows] of linkGroups) upsertLinks(linkName, rows, { plannedCreates, author: actionName });
  return [...byType].map(([objectType, rows]) => ({ objectType, count: rows.length }));
}

export function listActionTypes() { return store.state.actionTypes.map((action) => structuredClone(action)); }
export function getSubmissionLog() { return store.state.actionSubmissions.map((submission) => ({ ts: submission.ts, actionType: submission.actionType, mode: submission.mode, branch: submission.branch, appliedEdits: submission.appliedEdits })); }
