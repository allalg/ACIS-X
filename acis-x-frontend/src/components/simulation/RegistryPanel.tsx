import { formatRelativeTime } from '../../lib/utils'
import type { AgentInfo } from '../../types/agent'

type RegistryPanelProps = {
  agents: AgentInfo[]
  onSelectAgent?: (agentName: string) => void
}

export function RegistryPanel({ agents, onSelectAgent }: RegistryPanelProps) {
  return (
    <aside className="registry-panel surface-card">
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
        <h3 style={{ margin: 0 }}>AGENT REGISTRY</h3>
        <span className="mono text-secondary" style={{ fontSize: '10px' }}>{agents.length} AGENTS</span>
      </div>
      <div className="registry-list">
        {agents.map((agent) => {
          const isRestarting = agent.status?.toLowerCase() === 'restarting'
          return (
            <div
              key={agent.agent_id}
              className="registry-entry surface-card-hover"
              onClick={() => onSelectAgent?.(agent.agent_name)}
              style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', cursor: 'pointer' }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span className={`registry-dot status-${agent.status.toLowerCase()}`} style={{ backgroundColor: isRestarting ? 'var(--accent-red)' : undefined }} />
                <span className="mono registry-name" style={{ color: isRestarting ? 'var(--accent-red)' : 'var(--text-primary)', fontWeight: isRestarting ? 'bold' : 'normal' }}>
                  {agent.agent_name}
                </span>
              </div>
              {isRestarting ? (
                <span className="mono spin" style={{ color: 'var(--accent-red)', fontSize: '9px', fontWeight: 'bold' }}>
                  [REBOOTING...]
                </span>
              ) : (
                <span className="registry-time">{formatRelativeTime(agent.registered_at)}</span>
              )}
            </div>
          )
        })}
      </div>
    </aside>
  )
}
