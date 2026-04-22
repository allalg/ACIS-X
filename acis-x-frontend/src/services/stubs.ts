import seedrandom from 'seedrandom'
import type {
  CustomerProfile,
  CustomersResponse,
  DashboardSummary,
  HealthResponse,
  MetricsComputeResponse,
} from '../types/api'
import type { AgentsStatusResponse } from '../types/agent'
import type { EventEnvelope } from '../types/events'
import type { Invoice, InvoiceResponse, Payment, PaymentResponse } from '../types/ledger'
import type { MetricsResult, RiskProfile } from '../types/metrics'

const rng = seedrandom('acis-x-seed')

function randomBetween(min: number, max: number): number {
  return min + (max - min) * rng()
}

function randomInt(min: number, max: number): number {
  return Math.floor(randomBetween(min, max + 1))
}

function pick<T>(values: T[]): T {
  return values[randomInt(0, values.length - 1)]
}

const customerNames = [
  'Atlas Manufacturing',
  'Helios Freight',
  'Northline Textiles',
  'Meridian Medical',
  'Pioneer Components',
  'Bluegate Retail',
  'Summit Steelworks',
  'Crescent Foods',
  'Aurum Electronics',
  'Keystone Logistics',
  'Cardinal Energy',
  'Horizon Packaging',
  'Sterling Biotech',
  'Lighthouse Interiors',
  'Bridgewater Supply',
  'Redstone Mobility',
  'Quantum Agro',
  'Ridgeway Pharma',
  'Nimbus Telecom',
  'Riverview Capital',
]

const agentNames = [
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
  'MonitoringAgent',
  'SelfHealingAgent',
  'RuntimeManager',
  'PlacementEngine',
  'RegistryService',
]

const invoices: Invoice[] = Array.from({ length: 120 }).map((_, index) => {
  const customerIndex = index % customerNames.length
  const totalAmount = randomInt(2000, 180000)
  const paidAmount = randomInt(0, totalAmount)
  const status: Invoice['status'] =
    paidAmount === 0 ? 'pending' : paidAmount < totalAmount ? 'partial' : 'paid'
  const dueOffset = randomInt(-25, 25)
  const dueDate = new Date(Date.now() + dueOffset * 24 * 60 * 60 * 1000)
  return {
    invoice_id: `INV-${(index + 1).toString().padStart(5, '0')}`,
    customer_id: `CUST-${(customerIndex + 1).toString().padStart(4, '0')}`,
    customer_name: customerNames[customerIndex],
    total_amount: totalAmount,
    paid_amount: paidAmount,
    currency: 'USD',
    issued_date: new Date(Date.now() - randomInt(10, 120) * 24 * 60 * 60 * 1000).toISOString(),
    due_date: dueDate.toISOString(),
    status: dueOffset < 0 && paidAmount < totalAmount ? 'overdue' : status,
    days_overdue: dueOffset < 0 && paidAmount < totalAmount ? Math.abs(dueOffset) : 0,
    created_at: new Date(Date.now() - randomInt(1, 120) * 24 * 60 * 60 * 1000).toISOString(),
    updated_at: new Date(Date.now() - randomInt(0, 5) * 60 * 60 * 1000).toISOString(),
  }
})

const payments: Payment[] = Array.from({ length: 160 }).map((_, index) => {
  const invoice = pick(invoices)
  return {
    payment_id: `PAY-${(index + 1).toString().padStart(5, '0')}`,
    invoice_id: invoice.invoice_id,
    customer_id: invoice.customer_id,
    customer_name: invoice.customer_name,
    amount: randomInt(500, Math.max(1000, Math.floor(invoice.total_amount * 0.7))),
    currency: 'USD',
    payment_date: new Date(Date.now() - randomInt(0, 45) * 24 * 60 * 60 * 1000).toISOString(),
    payment_method: pick(['bank_transfer', 'card', 'cheque', 'other'] as const),
    status: pick(['received', 'processing', 'applied', 'failed'] as const),
    reference: `REF-${randomInt(100000, 999999)}`,
    created_at: new Date(Date.now() - randomInt(0, 2) * 24 * 60 * 60 * 1000).toISOString(),
  }
})

const riskProfiles: RiskProfile[] = customerNames.map((name, idx) => {
  const combined = randomBetween(0.15, 0.95)
  return {
    customer_id: `CUST-${(idx + 1).toString().padStart(4, '0')}`,
    company_name: name,
    financial_risk: randomBetween(0.1, 0.95),
    litigation_risk: randomBetween(0.05, 0.9),
    combined_risk: combined,
    severity: combined > 0.8 ? 'critical' : combined > 0.65 ? 'high' : combined > 0.45 ? 'medium' : 'low',
    confidence: randomBetween(0.55, 0.98),
    updated_at: new Date(Date.now() - randomInt(0, 3) * 60 * 60 * 1000).toISOString(),
  }
})

export const stubs = {
  getHealth(): HealthResponse {
    return {
      status: 'ok',
      service: 'acis-api-bff',
      version: '0.1.0',
      timestamp: new Date().toISOString(),
    }
  },

  getDashboardSummary(): DashboardSummary {
    const openInvoices = invoices.filter((invoice) => invoice.status !== 'paid').length
    const overdue = invoices.filter((invoice) => invoice.status === 'overdue')
    const totalOutstanding = invoices.reduce(
      (sum, invoice) => sum + (invoice.total_amount - invoice.paid_amount),
      0,
    )

    return {
      timestamp: new Date().toISOString(),
      total_customers: customerNames.length,
      total_invoices: invoices.length,
      open_invoices: openInvoices,
      overdue_invoices: overdue.length,
      total_outstanding: totalOutstanding,
      avg_delay: 4.2,
      on_time_ratio: 0.78,
      high_risk_count: riskProfiles.filter((profile) => profile.severity === 'high').length,
      medium_risk_count: riskProfiles.filter((profile) => profile.severity === 'medium').length,
      low_risk_count: riskProfiles.filter((profile) => profile.severity === 'low').length,
    }
  },

  getCustomers(search = ''): CustomersResponse {
    const rows = customerNames
      .map((name, index) => {
        const customerId = `CUST-${(index + 1).toString().padStart(4, '0')}`
        const profile = riskProfiles[index]
        const customerInvoices = invoices.filter((invoice) => invoice.customer_id === customerId)
        const outstanding = customerInvoices.reduce(
          (sum, invoice) => sum + (invoice.total_amount - invoice.paid_amount),
          0,
        )
        return {
          customer_id: customerId,
          name,
          credit_limit: randomInt(100000, 1500000),
          risk_score: profile.combined_risk,
          status: 'active',
          combined_risk: profile.combined_risk,
          severity: profile.severity,
          total_outstanding: outstanding,
          avg_delay: randomBetween(0.8, 12.4),
          on_time_ratio: randomBetween(0.45, 0.98),
          updated_at: new Date(Date.now() - randomInt(0, 120) * 60 * 1000).toISOString(),
        }
      })
      .filter((customer) => {
        if (!search) {
          return true
        }
        const value = search.toLowerCase()
        return (
          customer.name.toLowerCase().includes(value) ||
          customer.customer_id.toLowerCase().includes(value)
        )
      })

    return { customers: rows, total: rows.length }
  },

  getCustomerById(id: string): CustomerProfile {
    const index = Math.max(0, customerNames.findIndex((_, idx) => `CUST-${(idx + 1).toString().padStart(4, '0')}` === id))
    const profile = riskProfiles[index]
    const customerInvoices = invoices.filter((invoice) => invoice.customer_id === id)

    return {
      customer_id: id,
      name: customerNames[index],
      credit_limit: randomInt(100000, 1500000),
      risk_score: profile.combined_risk,
      total_outstanding: customerInvoices.reduce(
        (sum, invoice) => sum + (invoice.total_amount - invoice.paid_amount),
        0,
      ),
      avg_delay: randomBetween(1, 12),
      on_time_ratio: randomBetween(0.45, 0.97),
      overdue_count: customerInvoices.filter((invoice) => invoice.status === 'overdue').length,
      last_payment_date: new Date(Date.now() - randomInt(1, 30) * 24 * 60 * 60 * 1000).toISOString(),
      financial_risk: profile.financial_risk,
      litigation_risk: profile.litigation_risk,
      combined_risk: profile.combined_risk,
      severity: profile.severity,
      confidence: profile.confidence,
      updated_at: profile.updated_at,
    }
  },

  getInvoices(customerId?: string, status?: string): InvoiceResponse {
    const rows = invoices.filter((invoice) => {
      if (customerId && invoice.customer_id !== customerId) {
        return false
      }
      if (status && invoice.status !== status) {
        return false
      }
      return true
    })

    return { invoices: rows, total: rows.length }
  },

  getPayments(customerId?: string, invoiceId?: string): PaymentResponse {
    const rows = payments.filter((payment) => {
      if (customerId && payment.customer_id !== customerId) {
        return false
      }
      if (invoiceId && payment.invoice_id !== invoiceId) {
        return false
      }
      return true
    })

    return { payments: rows, total: rows.length }
  },

  getAgentsStatus(): AgentsStatusResponse {
    return {
      agents: agentNames.map((agentName, index) => ({
        agent_id: `agent-${(index + 1).toString().padStart(3, '0')}`,
        agent_name: agentName,
        agent_type: agentName.includes('Agent') ? 'business' : 'operational',
        status: pick(['healthy', 'degraded', 'healthy', 'healthy']),
        registered_at: new Date(Date.now() - randomInt(1, 240) * 60 * 1000).toISOString(),
        last_heartbeat: new Date(Date.now() - randomInt(1, 35) * 1000).toISOString(),
        topics: {
          consumes: ['acis.invoices', 'acis.payments'],
          produces: ['acis.risk'],
        },
        capabilities: ['monitoring', 'scoring'],
        version: '1.0.0',
      })),
    }
  },

  computeMetrics(): MetricsComputeResponse {
    return {
      job_id: `job-${Date.now()}`,
      status: 'computing',
      started_at: new Date().toISOString(),
    }
  },

  getMetricsResult(jobId: string): MetricsResult {
    return {
      job_id: jobId,
      status: 'ready',
      computed_at: new Date().toISOString(),
      data: {
        risk_profiles: riskProfiles,
        customer_metrics: riskProfiles.map((profile) => ({
          customer_id: profile.customer_id,
          total_outstanding: randomInt(10000, 350000),
          avg_delay: randomBetween(1, 13),
          on_time_ratio: randomBetween(0.45, 0.95),
          last_payment_date: new Date(Date.now() - randomInt(1, 35) * 24 * 60 * 60 * 1000).toISOString(),
          updated_at: new Date().toISOString(),
        })),
        summary: {
          total_customers: riskProfiles.length,
          high_risk_count: riskProfiles.filter((profile) => profile.severity === 'high').length,
          medium_risk_count: riskProfiles.filter((profile) => profile.severity === 'medium').length,
          low_risk_count: riskProfiles.filter((profile) => profile.severity === 'low').length,
          on_time_ratio: 0.76,
        },
      },
    }
  },

  createEvent(): EventEnvelope {
    const eventTypes = [
      'invoice.created',
      'payment.received',
      'customer.metrics.updated',
      'risk.profile.updated',
      'agent.health',
      'self.healing.triggered',
      'time.tick',
    ]
    const source = pick(agentNames)
    return {
      event_id: crypto.randomUUID(),
      event_type: pick(eventTypes),
      event_source: source,
      event_time: new Date().toISOString(),
      correlation_id: null,
      entity_id: `CUST-${randomInt(1, customerNames.length).toString().padStart(4, '0')}`,
      schema_version: '1.1',
      payload: {
        reason: pick(['high lag', 'timeout', 'normal processing', 'risk signal']),
      },
      metadata: {
        stub: true,
      },
    }
  },
}
