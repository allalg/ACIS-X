import { useEffect, useMemo, useState } from 'react'
import { CustomerProfileSheet } from '../components/customers/profile/CustomerProfileSheet'
import { ComputeButton } from '../components/metrics/ComputeButton'
import { MetricsCanvas } from '../components/metrics/MetricsCanvas'
import { TopProgressBar } from '../components/ui/TopProgressBar'
import { useCustomerProfile } from '../hooks/useCustomerProfile'
import { useComputeMetrics, useMetricsResult } from '../hooks/useMetrics'
import { formatTime } from '../lib/utils'

export default function MetricsPage() {
  const [jobId, setJobId] = useState<string | null>(null)
  const [selectedCustomerId, setSelectedCustomerId] = useState<string | null>(null)
  const computeMutation = useComputeMetrics()
  const metricsQuery = useMetricsResult(jobId)
  const selectedCustomer = useCustomerProfile(selectedCustomerId ?? '')

  useEffect(() => {
    if (!jobId) {
      handleCompute()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const handleCompute = () => {
    computeMutation.mutate(undefined, {
      onSuccess: (data) => {
        setJobId(data.job_id)
      },
    })
  }

  const computing = computeMutation.isPending || metricsQuery.data?.status === 'computing'

  const riskProfiles = useMemo(() => metricsQuery.data?.data.risk_profiles ?? [], [metricsQuery.data])
  const customerMetrics = useMemo(
    () => metricsQuery.data?.data.customer_metrics ?? [],
    [metricsQuery.data],
  )

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
            Last computed: {metricsQuery.data?.computed_at ? formatTime(metricsQuery.data.computed_at) : 'loading…'}
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
