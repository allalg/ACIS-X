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

function getResolvedApiBaseUrl(): string {
  if (typeof window !== 'undefined') {
    const urlParams = new URLSearchParams(window.location.search)
    const paramUrl = urlParams.get('api_url')
    if (paramUrl) {
      const cleanParam = paramUrl.trim().replace(/\/+$/, '')
      localStorage.setItem('acis_api_url', cleanParam)
      return cleanParam
    }

    const storedUrl = localStorage.getItem('acis_api_url')
    if (storedUrl) {
      return storedUrl.trim().replace(/\/+$/, '')
    }
  }

  const rawBaseUrl = import.meta.env.VITE_API_BASE_URL ?? 'http://localhost:8000'
  return rawBaseUrl.trim().replace(/\/+$/, '')
}

export function setRuntimeApiUrl(url: string) {
  const clean = url.trim().replace(/\/+$/, '')
  if (clean) {
    localStorage.setItem('acis_api_url', clean)
  } else {
    localStorage.removeItem('acis_api_url')
  }
  window.location.reload()
}

export function getRuntimeApiUrl(): string {
  return getResolvedApiBaseUrl()
}

export const API_BASE_URL = getResolvedApiBaseUrl()
export const API_KEY = import.meta.env.VITE_API_KEY || 'change_me'
const USE_STUBS = false // Keeping constant exported for backward compatibility if used elsewhere

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
  const baseUrl = getRuntimeApiUrl()
  const response = await fetch(`${baseUrl}${path}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      'X-API-Key': API_KEY,
      'bypass-tunnel-reminder': 'true',
      'ngrok-skip-browser-warning': 'true',
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
    return apiRequest<HealthResponse>('/api/v1/health')
  },

  getDashboardSummary(): Promise<DashboardSummary> {
    return apiRequest<DashboardSummary>('/api/v1/dashboard/summary')
  },

  getCustomers(search = ''): Promise<CustomersResponse> {
    const query = search ? `?search=${encodeURIComponent(search)}` : ''
    return apiRequest<CustomersResponse>(`/api/v1/customers${query}`)
  },

  getCustomerById(id: string): Promise<CustomerProfile> {
    return apiRequest<CustomerProfile>(`/api/v1/customers/${id}`)
  },

  getInvoices(customerId?: string, status?: string): Promise<InvoiceResponse> {
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
    return apiRequest<AgentsStatusResponse>('/api/v1/agents/status')
  },

  getPipelineHealth(): Promise<{
    timestamp: string
    dlq: {
      total_dlq_count: number
      last_60s_count: number
      top_reasons: Record<string, number>
      updated_at: string | null
    }
    recent_recoveries: Array<{
      event_time: string
      agent_id?: string
      agent_name?: string
      action?: string
      rule?: string
      status?: string
    }>
    consumer_status: Record<string, string | null>
    agent_count: number
  }> {
    return apiRequest('/api/v1/system/pipeline')
  },

  computeMetrics(): Promise<MetricsComputeResponse> {
    return apiRequest<MetricsComputeResponse>('/api/v1/metrics/compute', {
      method: 'POST',
      body: JSON.stringify({}),
    })
  },

  getMetricsResult(jobId: string): Promise<MetricsResult> {
    return apiRequest<MetricsResult>(`/api/v1/metrics/result/${jobId}`)
  },

  getCustomerCollections(customerId: string): Promise<any[]> {
    return apiRequest<any[]>(`/api/v1/customers/${customerId}/collections`)
  },

  getCustomerRiskExplanation(customerId: string): Promise<any> {
    return apiRequest<any>(`/api/v1/customers/${customerId}/risk-explanation`)
  },

  getCustomerExternalIntelligence(customerId: string): Promise<any> {
    return apiRequest<any>(`/api/v1/customers/${customerId}/external-intelligence`)
  },

  simulationControl(action: 'pause' | 'pause_scenario' | 'freeze_all' | 'resume'): Promise<{ status: string; message: string }> {
    return apiRequest<{ status: string; message: string }>('/api/v1/simulation/control', {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
  },

  simulationFault(instanceId: string): Promise<{ status: string; message: string }> {
    return apiRequest<{ status: string; message: string }>('/api/v1/simulation/fault', {
      method: 'POST',
      body: JSON.stringify({ instance_id: instanceId }),
    })
  },

  getDatabaseTableData(tableName: string, limit: number = 50): Promise<{ table_name: string; total: number; rows: any[] }> {
    return apiRequest<{ table_name: string; total: number; rows: any[] }>(`/api/v1/database/tables/${tableName}?limit=${limit}`)
  },
}

export { USE_STUBS }
