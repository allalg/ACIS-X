export type EventEnvelope = {
  event_id: string
  event_type: string
  event_source: string
  event_time: string
  correlation_id: string | null
  entity_id: string
  schema_version: string
  payload: Record<string, unknown>
  metadata?: Record<string, unknown>
}

export type EventStreamStatus = 'connected' | 'reconnecting' | 'disconnected'

export const EVENT_ABBREVIATIONS: Record<string, string> = {
  'invoice.created': 'INV',
  'invoice.updated': 'INV',
  'payment.received': 'PAY',
  'payment.applied': 'PAY',
  'customer.metrics.updated': 'STATE',
  'risk.profile.updated': 'RISK',
  'risk.scored': 'RISK',
  'customer.profile.updated': 'PROF',
  'collections.action': 'COLL',
  'overdue.detected': 'OVRD',
  'external.data.signal': 'EXT',
  'time.tick': 'TICK',
  'agent.registered': 'REG',
  'agent.health': 'HEALTH',
  'self.healing.triggered': 'HEAL',
  'db.write.confirmed': 'DB',
  'memory.updated': 'MEM',
  'payment.prediction.updated': 'PRED',
  'credit.policy.updated': 'POLICY',
}
