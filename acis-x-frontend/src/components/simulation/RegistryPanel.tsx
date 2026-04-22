import { formatRelativeTime } from '../../lib/utils'
import type { AgentInfo } from '../../types/agent'

type RegistryPanelProps = {
  agents: AgentInfo[]
}

export function RegistryPanel({ agents }: RegistryPanelProps) {
  return (
    <aside className="registry-panel surface-card">
      <h3>AGENT REGISTRY</h3>
      <div className="registry-list">
        {agents.map((agent) => (
          <div key={agent.agent_id} className="registry-entry">
            <span className={`registry-dot status-${agent.status.toLowerCase()}`} />
            <span className="mono registry-name">{agent.agent_name}</span>
            <span className="registry-time">{formatRelativeTime(agent.registered_at)}</span>
          </div>
        ))}
      </div>
    </aside>
  )
}
