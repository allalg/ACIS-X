import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useAgentStatus } from '../../hooks/useAgentStatus'
import { api } from '../../services/api'

export function SystemHealthBadge() {
  const { data } = useAgentStatus()
  const { data: pipeline } = useQuery({
    queryKey: ['pipeline-health'],
    queryFn: () => api.getPipelineHealth(),
    refetchInterval: 15_000,
    staleTime: 10_000,
  })

  const health = useMemo(() => {
    const statuses = data?.agents.map((agent) => agent.status.toLowerCase()) ?? []
    const dlqBurst = (pipeline?.dlq.last_60s_count ?? 0) > 0
    const consumerRetrying = Object.values(pipeline?.consumer_status ?? {}).some(
      (value) => value === 'retrying',
    )

    if (statuses.some((status) => status.includes('error') || status.includes('critical'))) {
      return { label: 'FAULT', className: 'fault' }
    }
    if (dlqBurst || consumerRetrying) {
      return { label: dlqBurst ? `DLQ ${pipeline?.dlq.last_60s_count}` : 'BUS RETRY', className: 'degraded' }
    }
    if (statuses.some((status) => status.includes('degraded') || status.includes('overloaded'))) {
      return { label: 'DEGRADED', className: 'degraded' }
    }
    const recentRecovery = (pipeline?.recent_recoveries ?? []).some((item) => {
      if (!item.event_time) return false
      const ageMs = Date.now() - Date.parse(item.event_time.replace(/Z?$/, 'Z'))
      return Number.isFinite(ageMs) && ageMs >= 0 && ageMs < 120_000
    })
    if (recentRecovery) {
      return { label: 'HEALING', className: 'degraded' }
    }
    return { label: 'OPERATIONAL', className: 'operational' }
  }, [data?.agents, pipeline])

  const title = useMemo(() => {
    const dlqTotal = pipeline?.dlq.total_dlq_count ?? 0
    const recoveries = pipeline?.recent_recoveries?.length ?? 0
    return `DLQ total=${dlqTotal}; recent recoveries=${recoveries}`
  }, [pipeline])

  return (
    <div className={`system-health ${health.className}`} title={title}>
      <span className="dot" />
      <span className="mono">{health.label}</span>
    </div>
  )
}
