// Server-side edit functions: dry-runs collect a plan, apply writes only after preflight.
import { randomBytes } from 'node:crypto';
import { rid, now, PlatformError, compileExpr } from './util.js';
import { store } from './store.js';
import { applyUserEdits, upsertLinks, queryObjects, linkedObjects, validateUserEdits, validateLinkEdits } from './ontology.js';

const IDENTIFIER = /^[A-Za-z][A-Za-z0-9_]{0,63}$/;
const PARAMETER_TYPES = new Set(['string', 'number', 'integer', 'boolean', 'json']);
const RESERVED_NAMES = new Set(['__proto__', 'prototype', 'constructor']);

function badFunction(message) {
  throw new PlatformError('BAD_FUNCTION', message);
}

function isRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function safeIdentifier(value, label) {
  if (typeof value !== 'string' || !IDENTIFIER.test(value) || RESERVED_NAMES.has(value)) {
    badFunction(`${label} must be a safe identifier`);
  }
}

function validateOperand(value, label, depth = 0) {
  if (depth > 32) badFunction(`${label} exceeds the supported nesting depth`);
  if (Array.isArray(value)) {
    value.forEach((item, index) => validateOperand(item, `${label}[${index}]`, depth + 1));
    return;
  }
  if (!isRecord(value)) return;
  const keys = Object.keys(value);
  if (keys.length === 1 && (keys[0] === '$param' || keys[0] === '$ref')) {
    if (typeof value[keys[0]] !== 'string' || !value[keys[0]].split('.').every((part) => IDENTIFIER.test(part) && !RESERVED_NAMES.has(part))) {
      badFunction(`${label} has an invalid ${keys[0]} reference`);
    }
    return;
  }
  for (const [key, item] of Object.entries(value)) {
    if (RESERVED_NAMES.has(key)) badFunction(`${label} contains a reserved object key`);
    validateOperand(item, `${label}.${key}`, depth + 1);
  }
}

function validateFunctionDefinition(definition) {
  if (!definition || typeof definition !== 'object' || Array.isArray(definition)) badFunction('function definition must be an object');
  safeIdentifier(definition.name, 'function name');
  if (Object.hasOwn(definition, 'code') || Object.hasOwn(definition, 'body')) {
    throw new PlatformError('UNSAFE_FUNCTION_CODE', 'JavaScript function bodies are not supported; define a declarative steps plan instead');
  }
  if (!Array.isArray(definition.params) || !Array.isArray(definition.steps)) badFunction('function requires params and declarative steps arrays');
  if (definition.params.length > 64 || definition.steps.length > 100) badFunction('function exceeds the supported parameter or step limit');
  const params = new Set();
  for (const parameter of definition.params) {
    if (!isRecord(parameter)) badFunction('function parameters must be objects');
    safeIdentifier(parameter.name, 'parameter name');
    if (params.has(parameter.name)) badFunction(`duplicate parameter '${parameter.name}'`);
    params.add(parameter.name);
    if (parameter.type !== undefined && !PARAMETER_TYPES.has(parameter.type)) badFunction(`unsupported parameter type '${parameter.type}'`);
    if (Object.hasOwn(parameter, 'default')) validateOperand(parameter.default, `default for '${parameter.name}'`);
  }
  const variables = new Set();
  for (const [index, step] of definition.steps.entries()) {
    if (!isRecord(step) || typeof step.op !== 'string') badFunction(`step ${index + 1} must declare an operation`);
    const label = `step ${index + 1}`;
    switch (step.op) {
      case 'get':
        if (typeof step.type !== 'string' || !step.type || !Object.hasOwn(step, 'pk')) badFunction(`${label} get requires type and pk`);
        safeIdentifier(step.as, `${label} output`);
        validateOperand(step.pk, `${label} pk`);
        variables.add(step.as);
        break;
      case 'search':
        if (typeof step.type !== 'string' || !step.type) badFunction(`${label} search requires a type`);
        safeIdentifier(step.as, `${label} output`);
        if (step.predicate !== undefined) {
          if (typeof step.predicate !== 'string') badFunction(`${label} predicate must be an expression string`);
          try { compileExpr(step.predicate); } catch (error) { badFunction(`${label} has invalid predicate: ${error.message}`); }
        }
        variables.add(step.as);
        break;
      case 'require':
        if (typeof step.expression !== 'string' || !step.expression) badFunction(`${label} require needs an expression`);
        try { compileExpr(step.expression); } catch (error) { badFunction(`${label} has invalid expression: ${error.message}`); }
        if (step.message !== undefined && typeof step.message !== 'string') badFunction(`${label} message must be a string`);
        break;
      case 'generate_id':
        safeIdentifier(step.as, `${label} output`);
        if (step.prefix !== undefined && (typeof step.prefix !== 'string' || step.prefix.length > 32)) badFunction(`${label} prefix must be a string of at most 32 characters`);
        variables.add(step.as);
        break;
      case 'modify':
      case 'delete':
        if (typeof step.type !== 'string' || !step.type || !Object.hasOwn(step, 'pk')) badFunction(`${label} ${step.op} requires type and pk`);
        validateOperand(step.pk, `${label} pk`);
        if (step.op === 'modify') {
          if (!isRecord(step.patch)) badFunction(`${label} modify requires an object patch`);
          validateOperand(step.patch, `${label} patch`);
        }
        break;
      case 'create':
        if (typeof step.type !== 'string' || !step.type || !isRecord(step.values)) badFunction(`${label} create requires type and values`);
        validateOperand(step.values, `${label} values`);
        break;
      case 'link':
        if (typeof step.link !== 'string' || !Object.hasOwn(step, 'from') || !Object.hasOwn(step, 'to')) badFunction(`${label} link requires link, from, and to`);
        validateOperand(step.from, `${label} from`);
        validateOperand(step.to, `${label} to`);
        break;
      case 'log':
        if (!Array.isArray(step.values)) badFunction(`${label} log requires a values array`);
        validateOperand(step.values, `${label} log values`);
        break;
      default:
        badFunction(`${label} uses unsupported operation '${step.op}'`);
    }
  }
  validateOperand(definition.result ?? null, 'function result');
  return { ...structuredClone(definition), description: definition.description ?? '' };
}

function resolveReference(path, params, variables, label = 'reference') {
  const segments = path.split('.');
  let value = segments[0] === 'params' ? params : variables;
  if (segments[0] === 'params') segments.shift();
  for (const segment of segments) {
    if (RESERVED_NAMES.has(segment) || value === null || value === undefined || !Object.hasOwn(Object(value), segment)) {
      throw new PlatformError('FUNCTION_REFERENCE', `${label} '${path}' could not be resolved`);
    }
    value = value[segment];
  }
  return structuredClone(value);
}

function resolveOperand(value, params, variables, depth = 0) {
  if (depth > 32) throw new PlatformError('FUNCTION_REFERENCE', 'function value exceeds the supported nesting depth');
  if (Array.isArray(value)) return value.map((item) => resolveOperand(item, params, variables, depth + 1));
  if (!isRecord(value)) return value;
  if (Object.keys(value).length === 1 && Object.hasOwn(value, '$param')) return resolveReference(`params.${value.$param}`, params, variables, 'parameter reference');
  if (Object.keys(value).length === 1 && Object.hasOwn(value, '$ref')) return resolveReference(value.$ref, params, variables);
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, resolveOperand(item, params, variables, depth + 1)]));
}

function bindParameters(fn, args) {
  if (!isRecord(args)) throw new PlatformError('BAD_ARGS', 'function arguments must be an object');
  const bound = Object.create(null);
  for (const parameter of fn.params) {
    let value = args[parameter.name];
    if (value === undefined) {
      if (parameter.required) throw new PlatformError('BAD_ARGS', `Missing required parameter '${parameter.name}'`);
      value = parameter.default;
    }
    if (value !== undefined && value !== null) {
      switch (parameter.type ?? 'string') {
        case 'number': value = typeof value === 'number' ? value : Number(value); if (!Number.isFinite(value)) throw new PlatformError('BAD_ARGS', `Parameter '${parameter.name}' must be a finite number`); break;
        case 'integer': value = typeof value === 'number' ? value : Number(value); if (!Number.isSafeInteger(value)) throw new PlatformError('BAD_ARGS', `Parameter '${parameter.name}' must be a safe integer`); break;
        case 'boolean': if (value !== true && value !== false) throw new PlatformError('BAD_ARGS', `Parameter '${parameter.name}' must be a boolean`); break;
        case 'string': if (typeof value !== 'string') throw new PlatformError('BAD_ARGS', `Parameter '${parameter.name}' must be a string`); break;
        case 'json': break;
        default: throw new PlatformError('BAD_FUNCTION', `Unsupported parameter type for '${parameter.name}'`);
      }
    }
    bound[parameter.name] = structuredClone(value);
  }
  return bound;
}

function executePlan(fn, host, params, edits, logs) {
  const variables = Object.create(null);
  const editStart = edits.length;
  for (const step of fn.steps) {
    switch (step.op) {
      case 'get': variables[step.as] = host.Objects.get(step.type, resolveOperand(step.pk, params, variables)); break;
      case 'search': variables[step.as] = host.Objects.search(step.type, step.predicate); break;
      case 'require': {
        const expression = compileExpr(step.expression);
        if (!expression.evaluate({ ...params, ...variables })) throw new PlatformError('FUNCTION_PRECONDITION', step.message ?? 'Function precondition failed');
        break;
      }
      case 'generate_id': variables[step.as] = `${step.prefix ?? ''}${randomBytes(8).toString('hex')}`; break;
      case 'modify': host.modifyObject(step.type, resolveOperand(step.pk, params, variables), resolveOperand(step.patch, params, variables)); break;
      case 'create': host.createObject(step.type, resolveOperand(step.values, params, variables)); break;
      case 'delete': host.deleteObject(step.type, resolveOperand(step.pk, params, variables)); break;
      case 'link': host.addLink(step.link, resolveOperand(step.from, params, variables), resolveOperand(step.to, params, variables)); break;
      case 'log': logs.push(step.values.map((value) => String(resolveOperand(value, params, variables) ?? '')).join('')); break;
      default: throw new PlatformError('BAD_FUNCTION', `Unsupported function operation '${step.op}'`);
    }
  }
  Object.defineProperty(variables, '__changeCount', { value: edits.length - editStart, enumerable: false });
  return resolveOperand(fn.result ?? null, params, variables);
}

function mergeExpectedVersions(primary, fallback) {
  const merged = Object.create(null);
  for (const [type, versions] of Object.entries(fallback ?? {})) merged[type] = { ...versions };
  for (const [type, versions] of Object.entries(primary ?? {})) merged[type] = { ...merged[type], ...versions };
  return merged;
}

export function runFunction(name, args = {}, { mode = 'dry_run', user, expectedVersions } = {}) {
  const fn = store.state.functions.find((item) => item.name === name);
  if (!fn) throw new PlatformError('NOT_FOUND', `Function '${name}' not found`);
  if (!Array.isArray(fn.steps)) throw new PlatformError('UNSAFE_FUNCTION_DISABLED', `Function '${name}' contains a legacy JavaScript body; migrate it to declarative steps before execution`);
  if (!['dry_run', 'apply'].includes(mode)) throw new PlatformError('BAD_FUNCTION_MODE', `Unsupported function mode '${mode}'`);
  const bound = bindParameters(fn, args);
  const edits = [], appliedEdits = [], readLog = [], logs = [];
  const observedVersions = Object.create(null);
  const recordVersions = (type, result) => {
    if (!result.ok || !result.objectVersions) return;
    observedVersions[type] ??= Object.create(null);
    Object.assign(observedVersions[type], result.objectVersions);
  };
  const actingUser = user ?? { id: 'system', kind: 'service', markings: [], clearance: 0, attributes: {} };
  const host = {
    Objects: {
      get: (type, pk) => {
        const result = queryObjects(type, actingUser, { objectPk: String(pk), includeVersions: true });
        recordVersions(type, result);
        const object = result.ok ? result.objects[0] ?? null : null;
        readLog.push({ op: 'get', type, pk: String(pk), found: Boolean(object), decision: result.ok ? (object ? 'allow' : 'not-visible') : 'deny' });
        return object ? structuredClone(object) : null;
      },
      search: (type, predicateSrc) => {
        const result = queryObjects(type, actingUser, { includeVersions: true });
        let objects = result.ok ? result.objects : [];
        recordVersions(type, result);
        if (predicateSrc) {
          let predicate;
          try { predicate = compileExpr(predicateSrc); }
          catch { predicate = null; }
          objects = predicate
            ? objects.filter((object) => { try { return Boolean(predicate.evaluate(object)); } catch { return false; } })
            : [];
        }
        readLog.push({ op: 'search', type, predicate: predicateSrc ?? null, hits: objects.length, decision: result.ok ? 'allow' : 'deny' });
        return objects.map((object) => structuredClone(object));
      },
      linked: (type, pk, linkName) => {
        const result = linkedObjects(type, pk, linkName, actingUser);
        readLog.push({ op: 'linked', type, pk: String(pk), linkName, hits: result.ok ? result.count : 0, decision: result.ok ? 'allow' : 'deny' });
        return result.ok ? result.objects : [];
      },
    },
    createObject: (type, patch) => {
      const objectType = store.state.objectTypes.find((item) => item.name === type);
      if (!objectType) throw new PlatformError('NOT_FOUND', `Object type '${type}' not found`);
      const key = patch?.[objectType.primaryKey];
      if (key === undefined || key === null || key === '') throw new PlatformError('BAD_EDIT', `createObject('${type}') requires primary key '${objectType.primaryKey}'`);
      edits.push({ type, op: 'create', pk: String(key), patch: structuredClone(patch) });
    },
    modifyObject: (type, pk, patch) => edits.push({ type, op: 'modify', pk: String(pk), patch: structuredClone(patch) }),
    deleteObject: (type, pk) => edits.push({ type, op: 'delete', pk: String(pk), patch: null }),
    addLink: (linkName, fromPk, toPk) => edits.push({ type: linkName, op: 'link', pk: String(fromPk), patch: { toPk: String(toPk) } }),
    log: (...parts) => logs.push(parts.map(String).join(' ')),
    params: bound,
    user: { id: actingUser.id, kind: actingUser.kind ?? 'human' },
  };

  let result = null, error = null;
  const started = Date.now();
  try {
    result = executePlan(fn, host, bound, edits, logs);
  } catch (caught) { error = caught.message; }
  const elapsedMs = Date.now() - started;

  if (!error && mode === 'apply') {
    const grouped = new Map(), plannedCreates = edits.filter((edit) => edit.op === 'create').map(({ type, pk }) => ({ objectType: type, pk }));
    for (const edit of edits) {
      if (edit.op === 'link') continue;
      if (!grouped.has(edit.type)) grouped.set(edit.type, []);
      grouped.get(edit.type).push({ pk: edit.pk, op: edit.op, patch: edit.patch });
    }
    for (const [type, rows] of grouped) {
      const validation = validateUserEdits(type, rows, { plannedCreates });
      if (!validation.ok) error = validation.error.message;
    }
    const linkGroups = new Map();
    for (const edit of edits.filter((item) => item.op === 'link')) {
      if (!linkGroups.has(edit.type)) linkGroups.set(edit.type, []);
      linkGroups.get(edit.type).push({ fromPk: edit.pk, toPk: edit.patch.toPk });
    }
    for (const [linkName, rows] of linkGroups) {
      const validation = validateLinkEdits(linkName, rows, { plannedCreates });
      if (!validation.ok) error = validation.error.message;
    }
    if (!error) {
      const versions = mergeExpectedVersions(expectedVersions, observedVersions);
      for (const [type, rows] of grouped) {
        const write = applyUserEdits(type, rows, { actionType: name, plannedCreates, expectedVersions: versions[type] });
        appliedEdits.push({ type, count: write.applied, editDataset: write.editDataset, writebackDataset: write.writebackDataset });
      }
      for (const [linkName, rows] of linkGroups) {
        upsertLinks(linkName, rows, { plannedCreates, author: name });
        appliedEdits.push({ link: linkName, count: rows.length });
      }
    }
  }

  const response = {
    ok: !error, runId: rid('fnr'), function: name, mode, result, error, logs,
    proposedEdits: edits, appliedEdits, reads: readLog, elapsedMs,
    observedVersions,
    note: mode === 'dry_run' ? 'Dry run: zero data side effects; governed reads only.' : error ? 'No edits were applied because preflight failed.' : 'Applied after complete edit preflight.',
    ts: now(),
  };
  store.mutate((s) => s.functionRuns.push(response));
  return response;
}

export function defineFunction(definition) {
  const normalized = validateFunctionDefinition(definition);
  if (store.state.functions.some((item) => item.name === normalized.name)) throw new PlatformError('ALREADY_EXISTS', `Function '${normalized.name}' already exists`);
  return store.mutate((s) => {
    const fn = { rid: rid('fn'), ...normalized, createdAt: now() };
    s.functions.push(fn);
    return fn;
  });
}

export function listFunctions() {
  return store.state.functions.map(({ rid: resourceId, name, params, steps, result, description }) => ({ rid: resourceId, name, params, steps: steps ?? [], result: result ?? null, description }));
}
