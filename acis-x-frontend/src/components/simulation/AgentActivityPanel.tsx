import type { AgentInfo } from '../../types/agent'
import { AgentActivityCard } from './AgentActivityCard'

type AgentActivityPanelProps = {
  agents: AgentInfo[]
  onSelectAgent?: (agentName: string) => void
}

export function AgentActivityPanel({ agents, onSelectAgent }: AgentActivityPanelProps) {
  return (
    <section className="agent-activity-panel surface-card">
      <header>
        <h3>AGENT ACTIVITY</h3>
      </header>
      <div className="agent-activity-list">
        {agents.map((agent) => (
          <AgentActivityCard
            key={agent.agent_id}
            agent={agent}
            onClick={() => onSelectAgent?.(agent.agent_name)}
          />
        ))}
      </div>
    </section>
  )
}
