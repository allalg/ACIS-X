import type { AgentInfo } from '../../types/agent'
import { AgentActivityCard } from './AgentActivityCard'

type AgentActivityPanelProps = {
  agents: AgentInfo[]
}

export function AgentActivityPanel({ agents }: AgentActivityPanelProps) {
  return (
    <section className="agent-activity-panel surface-card">
      <header>
        <h3>AGENT ACTIVITY</h3>
      </header>
      <div className="agent-activity-list">
        {agents.map((agent) => (
          <AgentActivityCard key={agent.agent_id} agent={agent} />
        ))}
      </div>
    </section>
  )
}
