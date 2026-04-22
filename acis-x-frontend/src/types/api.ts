import type { AgentInfo } from './agent'
import type { Invoice, Payment } from './ledger'
import type { CustomerMetric, RiskProfile } from './metrics'

export type HealthResponse = {
  status: string
  service: string
  version: string
  timestamp: string
}

export type DashboardSummary = {
  timestamp: string
  total_customers: number
  total_invoices: number
  open_invoices: number
  overdue_invoices: number
  total_outstanding: number
  avg_delay: number
  on_time_ratio: number
  high_risk_count: number
  medium_risk_count: number
  low_risk_count: number
}

export type CustomersResponse = {
  customers: Array<{
    customer_id: string
    name: string
    credit_limit: number
    risk_score: number
    status: string
    combined_risk: number
    severity: 'low' | 'medium' | 'high' | 'critical'
    total_outstanding: number
    avg_delay: number
    on_time_ratio: number
    updated_at: string
  }>
  total: number
}

export type CustomerProfile = {
  customer_id: string
  name: string
  credit_limit: number
  risk_score: number
  total_outstanding: number
  avg_delay: number
  on_time_ratio: number
  overdue_count: number
  last_payment_date: string
  financial_risk: number
  litigation_risk: number
  combined_risk: number
  severity: 'low' | 'medium' | 'high' | 'critical'
  confidence: number
  updated_at: string
}

export type MetricsComputeResponse = {
  job_id: string
  status: 'computing' | 'ready' | 'failed'
  started_at: string
}

export type AppDataSnapshot = {
  agents: AgentInfo[]
  invoices: Invoice[]
  payments: Payment[]
  risk_profiles: RiskProfile[]
  customer_metrics: CustomerMetric[]
}
