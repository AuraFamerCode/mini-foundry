// ─────────────────────────────────────────────────────────────────────────────
// Interfaces — the polymorphism layer. Abstract, unmaterialized contracts that
// concrete object types implement via property MAPPING (not just a tag).
//
// Mirrored semantics:
//   - Object types are concrete (backed by datasets, instantiable); interfaces
//     are abstract (not backed by datasets, cannot be instantiated directly).
//   - An interface carries interface properties, link-type constraints, and
//     action-type constraints — a fuller contract than an OOP interface.
//   - Implementation is a mapping operation: shared properties auto-map by
//     name; otherwise a local property is wired to the interface slot.
//     Required properties MUST be mapped; optional ones may be skipped —
//     optionality is the versioning/compatibility escape hatch.
//   - Interfaces compose via extension: an extending interface inherits the
//     properties and constraints of what it extends.
//   - Interface properties are contextual: querying "as the interface" returns
//     shared names (a view over the object), never a merged flat namespace.
//   - Workflows/apps/agents built against the interface work unmodified
//     against any current or future implementing type.
// ─────────────────────────────────────────────────────────────────────────────
import { rid, now, PlatformError, fail } from './util.js';
import { store } from './store.js';
import { queryObjects } from './ontology.js';

/** Define an interface (abstract type). */
export function defineInterface({ name, extends: parents = [], properties = [], linkConstraints = [], actionConstraints = [], description }) {
  if (!name) throw new PlatformError('BAD_INTERFACE', 'interface needs a name');
  if (store.state.interfaces.some((i) => i.name === name)) {
    throw new PlatformError('ALREADY_EXISTS', `Interface '${name}' already exists`);
  }
  // Extension composition: inherit properties + constraints from parents.
  const inherited = { properties: [], linkConstraints: [], actionConstraints: [] };
  for (const parent of parents) {
    const p = store.state.interfaces.find((i) => i.name === parent);
    if (!p) throw new PlatformError('NOT_FOUND', `Interface '${parent}' not found for extension`);
    inherited.properties.push(...p.properties);
    inherited.linkConstraints.push(...p.linkConstraints);
    inherited.actionConstraints.push(...p.actionConstraints);
  }
  return store.mutate((s) => {
    const iface = {
      rid: rid('if'),
      name,
      extends: parents,
      description: description ?? '',
      properties: [...inherited.properties, ...properties],
      linkConstraints: [...inherited.linkConstraints, ...linkConstraints],
      actionConstraints: [...inherited.actionConstraints, ...actionConstraints],
      createdAt: now(),
    };
    s.interfaces.push(iface);
    return iface;
  });
}

/**
 * Implement an interface on a concrete object type via property mapping.
 * Required interface properties must be mapped (auto-mapped when a local
 * property with the same name exists); optional ones may be skipped.
 */
export function implementInterface({ interfaceName, objectTypeName, propertyMapping = {} }) {
  const iface = store.state.interfaces.find((i) => i.name === interfaceName);
  if (!iface) return fail('NOT_FOUND', `Interface '${interfaceName}' not found`);
  const ot = store.state.objectTypes.find((o) => o.name === objectTypeName);
  if (!ot) return fail('NOT_FOUND', `Object type '${objectTypeName}' not found`);

  const mapping = [];
  for (const ip of iface.properties) {
    // auto-map by name first
    const auto = ot.properties.find((p) => p.name === ip.name);
    const local = propertyMapping[ip.name] ?? auto?.name ?? null;
    if (ip.required && !local) {
      return fail('INTERFACE_MAPPING', `Interface property '${ip.name}' is required; '${objectTypeName}' must map it (auto-map needs a same-named local property)`);
    }
    if (local) {
      const lp = ot.properties.find((p) => p.name === local);
      if (!lp) return fail('INTERFACE_MAPPING', `Mapping for '${ip.name}' points at unknown local property '${local}'`);
      mapping.push({ interfaceProperty: ip.name, localProperty: local, baseType: ip.baseType ?? lp.type ?? 'string', required: Boolean(ip.required) });
    } else {
      mapping.push({ interfaceProperty: ip.name, localProperty: null, baseType: ip.baseType ?? 'string', required: false, skipped: true }); // optional, skipped
    }
  }
  // Link constraints: object must have a compatible link of that name.
  for (const lc of iface.linkConstraints ?? []) {
    const ok = store.state.linkTypes.some((l) => l.name === lc.linkName && (l.fromType === objectTypeName || l.toType === objectTypeName));
    if (!ok) {
      return fail('INTERFACE_MAPPING', `Link constraint not satisfied: '${objectTypeName}' needs link '${lc.linkName}' (from or to), which does not exist`);
    }
  }

  return store.mutate((s) => {
    s.interfaceImpls = s.interfaceImpls.filter((x) => !(x.interfaceName === interfaceName && x.objectTypeName === objectTypeName));
    const impl = { interfaceName, objectTypeName, propertyMapping: mapping, createdAt: now() };
    s.interfaceImpls.push(impl);
    return { ok: true, implementation: impl };
  });
}

/** Implementations of an interface (including via extension inheritance). */
export function implementationsOf(interfaceName) {
  const direct = store.state.interfaceImpls.filter((x) => x.interfaceName === interfaceName);
  const childIfaces = store.state.interfaces.filter((i) => i.extends?.includes(interfaceName)).map((i) => i.name);
  const viaExtension = childIfaces.flatMap((c) => store.state.interfaceImpls.filter((x) => x.interfaceName === c));
  return { direct: direct.map((d) => d.objectTypeName), viaExtension: viaExtension.map((d) => d.objectTypeName) };
}

/**
 * Query objects "as the interface": a contextual VIEW. Results carry shared
 * (interface) property names, not the concrete local names — deliberately
 * not a merged flat namespace, so interface memberships can't collide.
 */
export function queryAsInterface(interfaceName, user, { search } = {}) {
  const iface = store.state.interfaces.find((i) => i.name === interfaceName);
  if (!iface) return fail('NOT_FOUND', `Interface '${interfaceName}' not found`);
  const impls = implementationsOf(interfaceName);
  const results = [];
  for (const otName of impls.direct) {
    const impl = store.state.interfaceImpls.find((x) => x.interfaceName === interfaceName && x.objectTypeName === otName);
    const r = queryObjects(otName, user, { search });
    if (!r.ok) continue; // caller's policies may deny a concrete type entirely
    for (const o of r.objects) {
      const view = { __interface: interfaceName, __objectType: otName, __pk: String(o[store.state.objectTypes.find((x) => x.name === otName)?.primaryKey ?? 'id']) };
      for (const m of impl.propertyMapping) {
        view[m.interfaceProperty] = m.localProperty ? o[m.localProperty] : null;
      }
      results.push(view);
    }
  }
  return { ok: true, interface: interfaceName, count: results.length, objects: results };
}

/**
 * Interface-targeted action submission: the action declares an interface
 * reference parameter; the submitter passes the concrete object type at
 * submit time, and rules resolve against the mapped local properties.
 */
export function resolveInterfaceParam(interfaceName, objectTypeName, params) {
  const impl = store.state.interfaceImpls.find((x) => x.interfaceName === interfaceName && x.objectTypeName === objectTypeName);
  if (!impl) return fail('INTERFACE_MAPPING', `'${objectTypeName}' does not implement '${interfaceName}' — cannot route an interface-typed parameter there`);
  return { ok: true, mapping: impl.propertyMapping, params };
}
