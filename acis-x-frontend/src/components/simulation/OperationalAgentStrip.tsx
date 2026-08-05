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
          <AgentBranch x1={positions[idx]} y1={180} x2={positions[idx]} y2={110} colorClass="agent-color-monitoring" dimmed />
          <AgentNode x={positions[idx]} y={88} label={agent} colorClass="agent-color-monitoring" size="operational" />
        </g>
      ))}
    </g>
  )
}
