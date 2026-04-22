export type RiskProfile = {
  customer_id: string
  company_name: string
  financial_risk: number
  litigation_risk: number
  combined_risk: number
  severity: 'low' | 'medium' | 'high' | 'critical'
  confidence: number
  updated_at: string
}

export type CustomerMetric = {
  customer_id: string
  total_outstanding: number
  avg_delay: number
  on_time_ratio: number
  last_payment_date: string
  updated_at: string
}

export type MetricsSummary = {
  total_customers: number
  high_risk_count: number
  medium_risk_count: number
  low_risk_count: number
  on_time_ratio: number
}

export type MetricsResult = {
  job_id: string
  status: 'computing' | 'ready' | 'failed'
  computed_at: string
  data: {
    risk_profiles: RiskProfile[]
    customer_metrics: CustomerMetric[]
    summary: MetricsSummary
  }
}
