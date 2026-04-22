import { formatRelativeTime } from '../../lib/utils'
import type { AgentInfo } from '../../types/agent'

type AgentActivityCardProps = {
  agent: AgentInfo
}

export function AgentActivityCard({ agent }: AgentActivityCardProps) {
  return (
    <article className={`agent-activity-card status-${agent.status.toLowerCase()}`}>
      <header>
        <span className="activity-dot" />
        <strong className="mono">{agent.agent_name}</strong>
        <span className="badge">{agent.status}</span>
      </header>
      <div className="activity-metrics numeric">
        <span>Events/min {Math.round(Math.random() * 100)}</span>
        <span>Last {formatRelativeTime(agent.last_heartbeat)}</span>
        <span>Lag {Math.round(Math.random() * 12)}</span>
      </div>
      <div className="activity-sparkline" aria-hidden="true">
        {Array.from({ length: 30 }).map((_, index) => (
          <i key={index} />
        ))}
      </div>
    </article>
  )
}
