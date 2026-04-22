import { AgentBranch } from './AgentBranch'
import { AgentNode } from './AgentNode'

type OperationalAgentStripProps = {
  agents: string[]
  positions: number[]
}

export function OperationalAgentStrip({ agents, positions }: OperationalAgentStripProps) {
  return (
    <g>
      {agents.map((agent, idx) => (
        <g key={agent}>
          <AgentBranch x={positions[idx]} fromY={180} toY={110} colorClass="agent-color-monitoring" dimmed />
          <AgentNode x={positions[idx]} y={88} label={agent} colorClass="agent-color-monitoring" size="operational" />
        </g>
      ))}
    </g>
  )
}
