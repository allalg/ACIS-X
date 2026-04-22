import { useMemo } from 'react'
import { useAgentStatus } from '../../hooks/useAgentStatus'

export function SystemHealthBadge() {
  const { data } = useAgentStatus()

  const health = useMemo(() => {
    const statuses = data?.agents.map((agent) => agent.status.toLowerCase()) ?? []
    if (statuses.some((status) => status.includes('error') || status.includes('critical'))) {
      return { label: 'FAULT', className: 'fault' }
    }
    if (statuses.some((status) => status.includes('degraded') || status.includes('overloaded'))) {
      return { label: 'DEGRADED', className: 'degraded' }
    }
    return { label: 'OPERATIONAL', className: 'operational' }
  }, [data?.agents])

  return (
    <div className={`system-health ${health.className}`}>
      <span className="dot" />
      <span className="mono">{health.label}</span>
    </div>
  )
}
