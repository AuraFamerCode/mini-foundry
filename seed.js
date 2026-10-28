// ─────────────────────────────────────────────────────────────────────────────
// Demo scenario: aircraft parts maintenance workspace (the composition example
// from the research: parts needing maintenance, actions to triage them, and
// analytics on maintenance trends) — populated across every layer.
// ─────────────────────────────────────────────────────────────────────────────
import { store } from './lib/store.js';
import { registerSource, runSync } from './lib/connectors.js';
import { createDataset, commitTransaction } from './lib/datasets.js';
import { createRestrictedView } from './lib/security.js';
import { createPipeline, runPipeline } from './lib/pipelines.js';
import { defineObjectType, defineLinkType, funnelSyncBatch, upsertLinks } from './lib/ontology.js';
import { configureWriteback } from './lib/physical.js';
import { defineLinkTypePhysical, syncJoinTableLink, configureLinkWriteback } from './lib/links.js';
import { defineInterface, implementInterface } from './lib/interfaces.js';
import { defineIncrementalTransform } from './lib/incremental.js';
import { defineSchedule, registerPipelineSpec } from './lib/builds.js';
import { registerMcpApp } from './lib/mcp.js';
import { defineFunction } from './lib/functions.js';
import { defineActionType, submitAction } from './lib/actions.js';
import { registerAgent } from './lib/agent.js';
import { captureBundle } from './lib/releases.js';

const PARTS_CSV = `part_id,part_name,base,status,last_inspection,days_since_inspection,owner_email,severity
P-1001,Brake Assembly A320, JFK,needs_review,2026-05-12,138,owner.p1001@airline.example,high
P-1002,Landing Gear Strut,JFK,operational,2026-08-01,57,owner.p1002@airline.example,low
P-1003,Hydraulic Pump 737,LAX,needs_review,2026-04-02,178,owner.p1003@airline.example,high
P-1004,Cabin Pressure Valve,LAX,operational,2026-07-15,74,owner.p1004@airline.example,medium
P-1005,APU Control Unit,SFO,monitoring,2026-06-20,99,owner.p1005@airline.example,medium`;

const WORK_ORDERS = [
  { wo_id: 'WO-5001', part_id: 'P-1001', issue: 'Brake wear beyond limit', severity: 'high', due_date: '2026-10-01', status: 'open' },
  { wo_id: 'WO-5002', part_id: 'P-1003', issue: 'Pressure drop detected', severity: 'high', due_date: '2026-09-30', status: 'open' },
  { wo_id: 'WO-5003', part_id: 'P-1005', issue: 'Intermittent fault code', severity: 'medium', due_date: '2026-10-15', status: 'open' },
];

export async function seedDemo({ force = false } = {}) {
  if (!force && store.state.datasets.length > 0) return { ok: true, skipped: true };

  // ── Reset ──────────────────────────────────────────────────────────────────
  store.mutate((s) => {
    for (const k of Object.keys(s)) {
      if (Array.isArray(s[k])) s[k] = [];
    }
  });

  // ── Security: markings + users (agents are users too) ──────────────────────
  store.mutate((s) => {
    s.markings.push(
      { name: 'PII', description: 'Personally identifiable information', members: ['maintenance_lead', 'admin', 'auditor'] },
      { name: 'SAFETY', description: 'Safety-critical maintenance data', members: ['admin', 'maintenance_lead', 'technician', 'agent-atlas'] }
    );
    s.users.push(
      { id: 'admin', name: 'Platform Admin', kind: 'human', markings: ['PII', 'SAFETY'], clearance: 5, attributes: { role: 'admin', base: '*' } },
      { id: 'maintenance_lead', name: 'Dana (Maintenance Lead)', kind: 'human', markings: ['PII', 'SAFETY'], clearance: 3, attributes: { role: 'manager', base: 'JFK' } },
      { id: 'technician', name: 'Sam (Technician)', kind: 'human', markings: ['SAFETY'], clearance: 2, attributes: { role: 'tech', base: 'JFK' } },
      { id: 'auditor', name: 'Alex (Compliance Auditor)', kind: 'human', markings: ['PII'], clearance: 1, attributes: { role: 'auditor', base: '*' } }
    );
  });

  // ── Connect: sources with lineage from the first sync ──────────────────────
  const srcParts = registerSource({ name: 'parts-inventory.csv', adapter: 'csv', config: { csv: PARTS_CSV }, markings: ['SAFETY'] });
  const srcWO = registerSource({ name: 'maintenance-db (JDBC)', adapter: 'jdbc', config: { simulate: WORK_ORDERS }, markings: ['SAFETY'] });
  await runSync(srcParts.rid, { datasetName: 'parts_raw' });
  await runSync(srcWO.rid, { datasetName: 'work_orders_raw' });

  // ── Pipeline: enrich parts (filter -> join -> derive) with provenance ──────
  const pipe = createPipeline({
    name: 'enrich_parts',
    outputDataset: 'parts_enriched',
    steps: [
      { op: 'select', dataset: 'parts_raw' },
      { op: 'filter', expr: 'status != "retired"' },
      { op: 'rename', from: 'status', to: 'part_status' },
      { op: 'join', otherDataset: 'work_orders_raw', on: 'part_id', how: 'left' },
      { op: 'rename', from: 'status', to: 'wo_status' },
      { op: 'rename', from: 'part_status', to: 'status' },
      { op: 'derive', name: 'needs_action', expr: 'severity == "high" or days_since_inspection > 90' },
    ],
  });
  runPipeline(pipe.rid, { usage: { driverMemoryGb: 2, executorMemoryGb: 4, executorCores: 1, numExecutors: 1 } });

  // Restricted view: users only see rows matching their own base attribute.
  const enriched = store.state.datasets.find((dataset) => dataset.name === pipe.outputDataset);
  // Pipeline derives a boolean into the data rows; align the declared schema
  // type with that materialized output before validating the object contract.
  const needsActionColumn = enriched.schema.find((column) => column.name === 'needs_action');
  if (needsActionColumn) needsActionColumn.type = 'boolean';
  createRestrictedView(enriched.rid, 'user.attributes.base == base or user.attributes.base == "*"', 'parts_by_base');

  // ── Ontology: object types, link types, Funnel sync ────────────────────────
  defineObjectType({
    name: 'Part',
    display: 'Aircraft Part',
    primaryKey: 'part_id',
    titleKey: 'part_name',
    backingDataset: 'parts_enriched',
    sync: { mode: 'batch' },
    properties: [
      { name: 'part_id', type: 'string' },
      { name: 'part_name', type: 'string' },
      { name: 'base', type: 'string' },
      { name: 'status', type: 'string' },
      { name: 'last_inspection', type: 'string' },
      { name: 'days_since_inspection', type: 'number' },
      { name: 'owner_email', type: 'string', requiredMarking: 'PII' },
      { name: 'severity', type: 'string' },
      { name: 'wo_status', type: 'string' },
      { name: 'needs_action', type: 'boolean' },
    ],
  });
  defineObjectType({
    name: 'WorkOrder',
    display: 'Maintenance Work Order',
    primaryKey: 'wo_id',
    titleKey: 'issue',
    backingDataset: 'work_orders_raw',
    sync: { mode: 'batch' },
    properties: [
      { name: 'wo_id', type: 'string' },
      { name: 'part_id', type: 'string' },
      { name: 'issue', type: 'string' },
      { name: 'severity', type: 'string' },
      { name: 'due_date', type: 'string' },
      { name: 'status', type: 'string' },
      { name: 'resolution_note', type: 'string', backingColumn: null },
    ],
  });
  defineLinkType({ name: 'PartWorkOrders', fromType: 'Part', toType: 'WorkOrder', cardinality: 'one-to-many' });
  funnelSyncBatch('Part');
  funnelSyncBatch('WorkOrder');
  upsertLinks('PartWorkOrders', [
    { fromPk: 'P-1001', toPk: 'WO-5001' },
    { fromPk: 'P-1003', toPk: 'WO-5002' },
    { fromPk: 'P-1005', toPk: 'WO-5003' },
  ]);

  // ── Functions: dry-run edit function for triage logic ──────────────────
  defineFunction({
    name: 'triagePart',
    description: 'Escalate a part to needs_review, create a work order, and link them.',
    params: [{ name: 'partId', type: 'string', required: true }, { name: 'reason', type: 'string', required: true }],
    steps: [
      { op: 'get', type: 'Part', pk: { $param: 'partId' }, as: 'part' },
      { op: 'require', expression: 'part != null', message: 'Part not found' },
      { op: 'modify', type: 'Part', pk: { $ref: 'part.part_id' }, patch: { status: 'needs_review' } },
      { op: 'generate_id', as: 'woId', prefix: 'WO-' },
      { op: 'create', type: 'WorkOrder', values: { wo_id: { $ref: 'woId' }, part_id: { $ref: 'part.part_id' }, issue: { $param: 'reason' }, severity: 'high', due_date: '2026-12-01' } },
      { op: 'link', link: 'PartWorkOrders', from: { $ref: 'part.part_id' }, to: { $ref: 'woId' } },
      { op: 'log', values: ['triaged ', { $ref: 'part.part_id' }, ' with new work order ', { $ref: 'woId' }] },
    ],
    result: { escalated: { $ref: 'part.part_id' }, workOrder: { $ref: 'woId' } },
  });

  // ── Action types: declarative rules + criteria + overrides ───────────
  defineActionType({
    name: 'closeWorkOrder',
    description: 'Mark a work order closed and update the part status.',
    parameters: [
      { name: 'workOrder', type: 'object-reference', objectType: 'WorkOrder', required: true },
      { name: 'resolution', type: 'string', required: true },
      { name: 'justification', type: 'string', required: false },
    ],
    rules: [
      { kind: 'modify', objectParameter: 'workOrder', propertyMap: { status: { kind: 'static', value: 'CLOSED' }, resolution_note: { kind: 'fromParameter', param: 'resolution' } } },
    ],
    submissionCriteria: [
      { expr: 'resolution != ""', errorMessage: 'Provide a resolution before closing.' },
      { expr: 'workOrder.severity != "high" or justification', errorMessage: 'Closing a high-severity work order requires a written justification.' },
    ],
    overrides: [
      {
        name: 'manager justification override',
        conditions: [{ expr: 'user.attributes.role == "manager"' }],
        then: [{ param: 'justification', required: true, visible: true }],
      },
    ],
    writeAuthorization: { requiredMarking: 'SAFETY' },
  });

  defineActionType({
    name: 'escalatePart',
      description: 'Escalate a part: flip status and notify safety ops.',
      parameters: [
        { name: 'part', type: 'object-reference', objectType: 'Part', required: true },
        { name: 'note', type: 'string', required: false },
      ],
      rules: [
        { kind: 'function', function: 'triagePart', paramMap: { partId: 'part', reason: { kind: 'static', value: 'Escalated via escalatePart action' } } },
      ],
      writeAuthorization: { requiredMarking: 'SAFETY' },
    });

  defineActionType({
    name: 'notifySafetyDesk',
    description: 'Fire the safety-desk webhook (used to demo branch suppression).',
    parameters: [],
    rules: [{ kind: 'webhook', webhookName: 'safety-desk-inbox' }],
  });

  // ── Physical model: title key + explicit writeback dataset ──────────
  configureWriteback('Part');
  configureWriteback('WorkOrder'); // "data users mutated" ≠ "data I ingested"

  // ── Three link implementations over one ontology ────────────────────
  const seedDs = (name, schema, rows) => {
    createDataset({ name, schema, markings: ['SAFETY'] });
    commitTransaction(name, 'master', { rows, note: 'seed' });
  };
  seedDs('bases_raw', [{ name: 'base_id', type: 'string' }, { name: 'region', type: 'string' }], [
    { base_id: 'JFK', region: 'northeast' }, { base_id: 'LAX', region: 'west' }, { base_id: 'SFO', region: 'west' },
  ]);
  seedDs('technicians_raw', [{ name: 'tech_id', type: 'string' }, { name: 'tech_name', type: 'string' }], [
    { tech_id: 'T-1', tech_name: 'Sam' }, { tech_id: 'T-2', tech_name: 'Priya' },
  ]);
  seedDs('wo_assignments', [{ name: 'wo_pk', type: 'string' }, { name: 'tech_pk', type: 'string' }], [
    { wo_pk: 'WO-5001', tech_pk: 'T-1' }, { wo_pk: 'WO-5002', tech_pk: 'T-1' }, { wo_pk: 'WO-5002', tech_pk: 'T-2' },
  ]);
  seedDs('visits_raw', [{ name: 'visit_id', type: 'string' }, { name: 'part_pk', type: 'string' }, { name: 'tech_pk', type: 'string' }, { name: 'visit_date', type: 'string' }], [
    { visit_id: 'V-1', part_pk: 'P-1001', tech_pk: 'T-1', visit_date: '2026-09-20' },
  ]);

  defineObjectType({ name: 'Base', display: 'Maintenance Base', primaryKey: 'base_id', titleKey: 'base_id', backingDataset: 'bases_raw',
    properties: [{ name: 'base_id', type: 'string' }, { name: 'region', type: 'string' }] });
  defineObjectType({ name: 'Technician', display: 'Technician', primaryKey: 'tech_id', titleKey: 'tech_name', backingDataset: 'technicians_raw',
    properties: [{ name: 'tech_id', type: 'string' }, { name: 'tech_name', type: 'string' }] });
  defineObjectType({ name: 'MaintenanceVisit', display: 'Maintenance Visit', primaryKey: 'visit_id', titleKey: 'visit_id', backingDataset: 'visits_raw',
    properties: [{ name: 'visit_id', type: 'string' }, { name: 'part_pk', type: 'string' }, { name: 'tech_pk', type: 'string' }, { name: 'visit_date', type: 'string' }] });
  funnelSyncBatch('Base'); funnelSyncBatch('Technician'); funnelSyncBatch('MaintenanceVisit');

  // 1. FK-derived (one-to-many): WorkOrder.part_id -> Part.part_id. Resolves
  //    live from the FK property — no sync step, derived-instant.
  defineLinkTypePhysical({ name: 'WorkOrderPart', fromType: 'WorkOrder', toType: 'Part', cardinality: 'many-to-one', backing: { kind: 'foreign_key', fkProperty: 'part_id' } });
  // 2. Join-table (many-to-many): dedicated physical dataset of PK pairs;
  //    visible only after a Funnel run on that table (batch cadence).
  defineLinkTypePhysical({ name: 'WorkOrderAssignees', fromType: 'WorkOrder', toType: 'Technician', cardinality: 'many-to-many', backing: { kind: 'join_table', joinTableDataset: 'wo_assignments', leftColumn: 'wo_pk', rightColumn: 'tech_pk' } });
  syncJoinTableLink('WorkOrderAssignees');
  configureLinkWriteback('WorkOrderAssignees');
  // 3. Object-backed: the link is itself an entity (MaintenanceVisit) with
  //    its own properties and lifecycle.
  defineLinkTypePhysical({ name: 'PartVisits', fromType: 'Part', toType: 'MaintenanceVisit', cardinality: 'many-to-many', backing: { kind: 'object_backed', manifestObjectType: 'MaintenanceVisit', leftProperty: 'part_pk', rightProperty: 'visit_id' } });
  syncJoinTableLink('PartVisits');

  // ── Interfaces: one contract, three implementing shapes ────────────
  defineInterface({
    name: 'Triageable',
    description: 'Anything that can be triaged: carries status + severity and is targetable by triage actions.',
    properties: [
      { name: 'status', baseType: 'string', required: true },
      { name: 'severity', baseType: 'string', required: true },
      { name: 'triageNotes', baseType: 'string', required: false },
    ],
    actionConstraints: [{ actionName: 'closeWorkOrder' }],
  });
  implementInterface({ interfaceName: 'Triageable', objectTypeName: 'Part' });       // auto-maps status, severity
  implementInterface({ interfaceName: 'Triageable', objectTypeName: 'WorkOrder' });  // auto-maps status, severity

  // Interface-targeted action: the auto-generated "Object type" picker decides
  // the concrete type at submit time; property writes route through the
  // implementation mapping (views over the object, not merged namespaces).
  defineActionType({
    name: 'setTriageStatus',
    description: 'Set the triage status of ANY Triageable object (Part, WorkOrder, or future implementing types).',
    parameters: [
      { name: 'triageable', type: 'interface-reference', interface: 'Triageable', required: true },
      { name: 'objectType', type: 'string', required: true },
      { name: 'status', type: 'string', required: true },
    ],
    rules: [
      { kind: 'modify', interface: 'Triageable', typeParameter: 'objectType', objectParameter: 'triageable', propertyMap: { status: 'status' } },
    ],
    writeAuthorization: { requiredMarking: 'SAFETY' },
  });

  // ── Incremental pipeline + build orchestration ─────────────────────
  defineIncrementalTransform({
    name: 'partsNeedsAction',
    inputs: ['parts_enriched'],
    output: 'parts_needs_action',
    key: 'part_id',
    semanticVersion: 1,
    requireIncremental: true,
    strictAppend: true,
    expression: 'status == "needs_review" or days_since_inspection > 90',
  });
  const enrichSpec = registerPipelineSpec({ name: 'enrichPartsJob', pipelineRid: pipe.rid });
  defineSchedule({
    name: 'maintenance-refresh',
    targets: ['partsNeedsAction'],
    buildType: 'force',
    trigger: { kind: 'any', components: [{ type: 'event', dataset: 'parts_raw' }, { type: 'event', dataset: 'work_orders_raw' }] },
    maxRetries: 3,
  });
  void enrichSpec;

  // ── Agents: registered like users, same governance surface ──────────
  registerAgent({ id: 'agent-atlas', name: 'Atlas (maintenance copilot)', markings: ['SAFETY'], clearance: 2, attributes: { role: 'agent' } });

  registerMcpApp({
    name: 'maintenance-ops',
    description: 'Scoped maintenance object queries and governed triage actions.',
    objectScopes: [
      { objectType: 'Part', properties: ['part_id', 'part_name', 'base', 'status', 'severity', 'needs_action'] },
      { objectType: 'WorkOrder', properties: ['wo_id', 'part_id', 'issue', 'severity', 'status', 'due_date'] },
    ],
    actionTools: ['setTriageStatus', 'closeWorkOrder'],
    allowedIdentities: ['admin', 'maintenance_lead', 'technician', 'agent-atlas'],
  });

  // ── Smoke: one governed write through the action engine ─────────────
  submitAction('notifySafetyDesk', {}, { id: 'admin', kind: 'human', markings: ['PII', 'SAFETY'], clearance: 5, attributes: {} }, { branch: 'master' });

  // ── Apollo: capture the seeded state as v1 ──────────────────────────
  captureBundle('v1: seeded aircraft maintenance workspace');

  return { ok: true, seeded: true };
}
