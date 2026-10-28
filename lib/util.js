// ─────────────────────────────────────────────────────────────────────────────
// mini-foundry — shared utilities
// ─────────────────────────────────────────────────────────────────────────────
import crypto from 'node:crypto';

export const rid = (prefix) =>
  `${prefix}-${crypto.randomBytes(6).toString('hex')}`;

export const now = () => new Date().toISOString();

export const clone = (v) =>
  v === undefined ? undefined : structuredClone(v);

export const isPlainObject = (v) =>
  v !== null && typeof v === 'object' && !Array.isArray(v);

export const deepMerge = (base, patch) => {
  const out = clone(base) ?? {};
  for (const [k, v] of Object.entries(patch ?? {})) {
    if (isPlainObject(v) && isPlainObject(out[k])) out[k] = deepMerge(out[k], v);
    else out[k] = clone(v);
  }
  return out;
};

export const ok = (data, meta) => ({ ok: true, data, ...(meta ?? {}) });
export const fail = (code, message, details) => ({
  ok: false,
  error: { code, message, ...(details ? { details } : {}) },
});
export const isFail = (r) => r && r.ok === false;

/** Typed platform errors — Foundry-style constraints are enforced, typed errors. */
export class PlatformError extends Error {
  constructor(code, message, details) {
    super(message);
    this.code = code;
    this.details = details;
  }
}

// ── Expression engine ────────────────────────────────────────────────────────
// Used by row policies, submission criteria and pipeline filters.
// Grammar: and/or/not/( )/comparisons/in + literals + dotted identifiers.
// Identifiers resolve against a caller-supplied scope. No eval().

const TOKEN_RE = /\s*(>=|<=|==|!=|&&|\|\||>|<|\(|\)|\[|\]|,|!|in\b|and\b|or\b|not\b|'[^']*'|"[^"]*"|-?\d+(?:\.\d+)?|true|false|null|[A-Za-z_][\w.]*)/gi;

export function compileExpr(src) {
  const tokens = [];
  let m;
  const re = new RegExp(TOKEN_RE);
  while ((m = re.exec(src)) !== null) {
    if (m.index === re.lastIndex) re.lastIndex++;
    if (m[1]) tokens.push(m[1]);
    else if (m[0].trim()) tokens.push(m[0].trim());
  }
  let pos = 0;
  const peek = () => tokens[pos];
  const eat = (t) => {
    if (tokens[pos] === t) { pos++; return true; }
    return false;
  };
  const expect = (t) => {
    if (!eat(t)) throw new PlatformError('EXPR_PARSE', `Expected '${t}' at token ${pos} in: ${src}`);
  };

  function parseOr() {
    let left = parseAnd();
    while (peek() === 'or' || peek() === '||') { pos++; left = { op: 'or', a: left, b: parseAnd() }; }
    return left;
  }
  function parseAnd() {
    let left = parseNot();
    while (peek() === 'and' || peek() === '&&') { pos++; left = { op: 'and', a: left, b: parseNot() }; }
    return left;
  }
  function parseNot() {
    if (peek() === 'not' || peek() === '!') { pos++; return { op: 'not', a: parseNot() }; }
    return parseComparison();
  }
  function parseComparison() {
    let left = parsePrimary();
    const ops = ['==', '!=', '>=', '<=', '>', '<', 'in'];
    while (ops.includes(peek())) {
      const op = tokens[pos++];
      const right = parsePrimary();
      left = { op, a: left, b: right };
    }
    return left;
  }
  function parsePrimary() {
    if (eat('(')) { const e = parseOr(); expect(')'); return e; }
    const t = peek();
    if (t === undefined) throw new PlatformError('EXPR_PARSE', `Unexpected end of expression: ${src}`);
    pos++;
    if (t === 'true') return { lit: true };
    if (t === 'false') return { lit: false };
    if (t === 'null') return { lit: null };
    if (/^['"]/.test(t)) return { lit: t.slice(1, -1) };
    if (/^-?\d+(?:\.\d+)?$/.test(t)) return { lit: Number(t) };
    return { ref: t.split('.') };
  }

  const ast = parseOr();
  if (pos !== tokens.length) throw new PlatformError('EXPR_PARSE', `Trailing tokens at ${pos} in: ${src}`);

  return {
    ast,
    evaluate(scope) {
      const ev = (n) => {
        if (n.lit !== undefined) return n.lit;
        if (n.ref) {
          let v = scope;
          for (const seg of n.ref) {
            if (v === null || v === undefined) return undefined;
            v = v[seg];
          }
          return v;
        }
        switch (n.op) {
          case 'and': return truthy(ev(n.a)) && truthy(ev(n.b));
          case 'or': return truthy(ev(n.a)) || truthy(ev(n.b));
          case 'not': return !truthy(ev(n.a));
          case '==': return ev(n.a) === ev(n.b);
          case '!=': return ev(n.a) !== ev(n.b);
          case '>': return ev(n.a) > ev(n.b);
          case '<': return ev(n.a) < ev(n.b);
          case '>=': return ev(n.a) >= ev(n.b);
          case '<=': return ev(n.a) <= ev(n.b);
          case 'in': {
            const b = ev(n.b);
            return Array.isArray(b) ? b.includes(ev(n.a)) : String(b ?? '').includes(String(ev(n.a) ?? ''));
          }
          default: throw new PlatformError('EXPR_PARSE', `Unknown op ${n.op}`);
        }
      };
      return ev(ast);
    },
  };
}

const truthy = (v) => Boolean(v);

export function evalExpr(src, scope) {
  return compileExpr(src).evaluate(scope);
}

/** Try to evaluate; on parse error treat as failed condition with message. */
export function tryExpr(src, scope) {
  try { return { ok: true, value: evalExpr(src, scope) }; }
  catch (e) { return { ok: false, error: e.message }; }
}

/** Coerce a string form value to the declared property type. */
export function coerce(value, type) {
  if (value === '' || value === undefined || value === null) return null;
  switch (type) {
    case 'number': {
      const n = Number(value);
      if (Number.isNaN(n)) throw new PlatformError('TYPE_MISMATCH', `Value '${value}' is not a number`);
      return n;
    }
    case 'boolean':
      if (typeof value === 'boolean') return value;
      if (value === 'true') return true;
      if (value === 'false') return false;
      throw new PlatformError('TYPE_MISMATCH', `Value '${value}' is not a boolean`);
    case 'array':
      if (Array.isArray(value)) return value;
      return String(value).split(',').map((s) => s.trim()).filter(Boolean);
    default:
      return String(value);
  }
}
