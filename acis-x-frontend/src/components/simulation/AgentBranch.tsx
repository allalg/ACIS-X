type AgentBranchProps = {
  x: number
  fromY: number
  toY: number
  colorClass: string
  dimmed?: boolean
}

export function AgentBranch({ x, fromY, toY, colorClass, dimmed = false }: AgentBranchProps) {
  return (
    <line
      x1={x}
      y1={fromY}
      x2={x}
      y2={toY}
      className={`agent-branch ${colorClass} ${dimmed ? 'dimmed' : ''}`}
    />
  )
}
