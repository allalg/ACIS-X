export type AgentStatusType =
  | 'idle'
  | 'active'
  | 'processing'
  | 'heartbeat'
  | 'error'
  | 'healthy'
  | 'degraded'
  | 'offline'

export type AgentInfo = {
  agent_id: string
  agent_name: string
  agent_type: string
  status: string
  registered_at: string
  last_heartbeat: string
  topics: {
    consumes: string[]
    produces: string[]
  }
  capabilities: string[]
  version: string
}

export type AgentsStatusResponse = {
  agents: AgentInfo[]
}

export const BUSINESS_AGENTS = [
  'ScenarioGeneratorAgent',
  'CustomerStateAgent',
  'AggregatorAgent',
  'CustomerProfileAgent',
  'RiskScoringAgent',
  'PaymentPredictionAgent',
  'CollectionsAgent',
  'OverdueDetectionAgent',
  'CreditPolicyAgent',
  'ExternalDataAgent',
  'ExternalScrapingAgent',
  'DBAgent',
  'MemoryAgent',
  'QueryAgent',
  'TimeTickAgent',
] as const

/** Core pipeline agents displayed in the simulation canvas SVG */
export const PIPELINE_AGENTS = [
  'ScenarioGeneratorAgent',
  'CustomerStateAgent',
  'CustomerProfileAgent',
  'PaymentPredictionAgent',
  'RiskScoringAgent',
  'CreditPolicyAgent',
  'CollectionsAgent',
] as const

export const OPERATIONAL_AGENTS = [
  'MonitoringAgent',
  'SelfHealingAgent',
  'RuntimeManager',
  'PlacementEngine',
  'RegistryService',
] as const
