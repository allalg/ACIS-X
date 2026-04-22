import { useEffect, useMemo, useState } from 'react'
import { CustomerProfileSheet } from '../components/customers/profile/CustomerProfileSheet'
import { ComputeButton } from '../components/metrics/ComputeButton'
import { MetricsCanvas } from '../components/metrics/MetricsCanvas'
import { TopProgressBar } from '../components/ui/TopProgressBar'
import { useCustomerProfile } from '../hooks/useCustomerProfile'
import { useComputeMetrics, useMetricsResult } from '../hooks/useMetrics'

const LAYOUT_KEY = 'acis-x-metrics-layout'

export default function MetricsPage() {
  const [jobId, setJobId] = useState<string | null>(null)
  const [layout, setLayout] = useState<unknown>(null)
  const [selectedCustomerId, setSelectedCustomerId] = useState<string | null>(null)
  const computeMutation = useComputeMetrics()
  const metricsQuery = useMetricsResult(jobId)
  const selectedCustomer = useCustomerProfile(selectedCustomerId ?? '')

  useEffect(() => {
    const stored = localStorage.getItem(LAYOUT_KEY)
    if (stored) {
      try {
        setLayout(JSON.parse(stored) as unknown)
      } catch {
        setLayout(null)
      }
    }
  }, [])

  const computing = computeMutation.isPending || metricsQuery.data?.status === 'computing'

  const riskProfiles = useMemo(() => metricsQuery.data?.data.risk_profiles ?? [], [metricsQuery.data])
  const customerMetrics = useMemo(
    () => metricsQuery.data?.data.customer_metrics ?? [],
    [metricsQuery.data],
  )

  const handleCompute = async () => {
    const result = await computeMutation.mutateAsync()
    setJobId(result.job_id)
  }

  const handleLayoutChange = (next: unknown) => {
    setLayout(next)
    localStorage.setItem(LAYOUT_KEY, JSON.stringify(next))
  }

  const resetLayout = () => {
    localStorage.removeItem(LAYOUT_KEY)
    setLayout(null)
  }

  return (
    <div className="metrics-page">
      <TopProgressBar active={computing} />
      <header className="page-header">
        <div>
          <h1 className="page-title">METRICS</h1>
          <p className="page-subtitle">Risk & prediction analytics</p>
        </div>
        <div className="metrics-header-actions">
          <span className="mono page-subtitle">
            Last computed: {metricsQuery.data?.computed_at ? new Date(metricsQuery.data.computed_at).toLocaleTimeString() : 'never'}
          </span>
          <button className="button-dark" onClick={resetLayout}>
            RESET LAYOUT
          </button>
          <ComputeButton onClick={handleCompute} loading={computeMutation.isPending} />
        </div>
      </header>

      <MetricsCanvas
        riskProfiles={riskProfiles}
        customerMetrics={customerMetrics}
        computing={computing}
        layout={layout}
        onLayoutChange={handleLayoutChange}
        onSelectCustomer={setSelectedCustomerId}
      />

      <CustomerProfileSheet
        customer={selectedCustomer.data ?? null}
        open={Boolean(selectedCustomerId)}
        onClose={() => setSelectedCustomerId(null)}
      />
    </div>
  )
}
