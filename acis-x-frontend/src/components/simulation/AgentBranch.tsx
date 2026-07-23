type AgentBranchProps = {
  x1: number
  y1: number
  x2: number
  y2: number
  colorClass: string
  dimmed?: boolean
}

export function AgentBranch({ x1, y1, x2, y2, colorClass, dimmed = false }: AgentBranchProps) {
  return (
    <line
      x1={x1}
      y1={y1}
      x2={x2}
      y2={y2}
      className={`agent-branch ${colorClass} ${dimmed ? 'dimmed' : ''}`}
    />
  )
}
