import type {
  CustomerProfile,
  CustomersResponse,
  DashboardSummary,
  HealthResponse,
  MetricsComputeResponse,
} from '../types/api'
import type { AgentsStatusResponse } from '../types/agent'
import type { InvoiceResponse, PaymentResponse } from '../types/ledger'
import type { MetricsResult } from '../types/metrics'
import { stubs } from './stubs'

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000'
const API_KEY = import.meta.env.VITE_API_KEY ?? ''
const USE_STUBS = import.meta.env.VITE_USE_STUBS === 'true'

export class ApiError extends Error {
  readonly status: number
  readonly detail: unknown

  constructor(message: string, status: number, detail?: unknown) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.detail = detail
  }
}

async function apiRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY,
      ...(init?.headers ?? {}),
    },
  })

  if (!response.ok) {
    let detail: unknown
    try {
      detail = await response.json()
    } catch {
      detail = await response.text()
    }
    throw new ApiError(`API request failed: ${path}`, response.status, detail)
  }

  return (await response.json()) as T
}

export const api = {
  getHealth(): Promise<HealthResponse> {
    if (USE_STUBS) {
      return Promise.resolve(stubs.getHealth())
    }
    return apiRequest<HealthResponse>('/api/v1/health')
  },

  getDashboardSummary(): Promise<DashboardSummary> {
    if (USE_STUBS) {
      return Promise.resolve(stubs.getDashboardSummary())
    }
    return apiRequest<DashboardSummary>('/api/v1/dashboard/summary')
  },

  getCustomers(search = ''): Promise<CustomersResponse> {
    if (USE_STUBS) {
      return Promise.resolve(stubs.getCustomers(search))
    }
    const query = search ? `?search=${encodeURIComponent(search)}` : ''
    return apiRequest<CustomersResponse>(`/api/v1/customers${query}`)
  },

  getCustomerById(id: string): Promise<CustomerProfile> {
    if (USE_STUBS) {
      return Promise.resolve(stubs.getCustomerById(id))
    }
    return apiRequest<CustomerProfile>(`/api/v1/customers/${id}`)
  },

  getInvoices(customerId?: string, status?: string): Promise<InvoiceResponse> {
    if (USE_STUBS) {
      return Promise.resolve(stubs.getInvoices(customerId, status))
    }

    const params = new URLSearchParams()
    if (customerId) {
      params.set('customer_id', customerId)
    }
    if (status) {
      params.set('status', status)
    }
    params.set('page', '1')
    params.set('limit', '200')

    return apiRequest<InvoiceResponse>(`/api/v1/invoices?${params.toString()}`)
  },

  getPayments(customerId?: string, invoiceId?: string): Promise<PaymentResponse> {
    if (USE_STUBS) {
      return Promise.resolve(stubs.getPayments(customerId, invoiceId))
    }

    const params = new URLSearchParams()
    if (customerId) {
      params.set('customer_id', customerId)
    }
    if (invoiceId) {
      params.set('invoice_id', invoiceId)
    }
    params.set('page', '1')
    params.set('limit', '200')

    return apiRequest<PaymentResponse>(`/api/v1/payments?${params.toString()}`)
  },

  getAgentStatus(): Promise<AgentsStatusResponse> {
    if (USE_STUBS) {
      return Promise.resolve(stubs.getAgentsStatus())
    }
    return apiRequest<AgentsStatusResponse>('/api/v1/agents/status')
  },

  computeMetrics(): Promise<MetricsComputeResponse> {
    if (USE_STUBS) {
      return Promise.resolve(stubs.computeMetrics())
    }
    return apiRequest<MetricsComputeResponse>('/api/v1/metrics/compute', {
      method: 'POST',
      body: JSON.stringify({}),
    })
  },

  getMetricsResult(jobId: string): Promise<MetricsResult> {
    if (USE_STUBS) {
      return Promise.resolve(stubs.getMetricsResult(jobId))
    }
    return apiRequest<MetricsResult>(`/api/v1/metrics/result/${jobId}`)
  },
}

export { API_BASE_URL, API_KEY, USE_STUBS }
