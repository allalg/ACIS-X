import { useEffect, useMemo, useState } from 'react'
import { CustomerProfileSheet } from '../components/customers/profile/CustomerProfileSheet'
import { ComputeButton } from '../components/metrics/ComputeButton'
import { MetricsCanvas } from '../components/metrics/MetricsCanvas'
import { TopProgressBar } from '../components/ui/TopProgressBar'
import { useCustomerProfile } from '../hooks/useCustomerProfile'
import { useComputeMetrics, useMetricsResult } from '../hooks/useMetrics'

export default function MetricsPage() {
  const [jobId, setJobId] = useState<string | null>(null)
  const [selectedCustomerId, setSelectedCustomerId] = useState<string | null>(null)
  const computeMutation = useComputeMetrics()
  const metricsQuery = useMetricsResult(jobId)
  const selectedCustomer = useCustomerProfile(selectedCustomerId ?? '')

  // Auto-compute on first load so the page isn't empty
  useEffect(() => {
    if (!jobId && !computeMutation.isPending) {
      computeMutation.mutateAsync().then((result) => {
        setJobId(result.job_id)
      })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
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
            Last computed: {metricsQuery.data?.computed_at ? new Date(metricsQuery.data.computed_at).toLocaleTimeString() : 'loading…'}
          </span>
          <ComputeButton onClick={handleCompute} loading={computeMutation.isPending} />
        </div>
      </header>

      <MetricsCanvas
        riskProfiles={riskProfiles}
        customerMetrics={customerMetrics}
        computing={computing}
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
