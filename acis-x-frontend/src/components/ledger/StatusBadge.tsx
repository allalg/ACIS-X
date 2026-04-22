type StatusBadgeProps = {
  status: string
}

export function StatusBadge({ status }: StatusBadgeProps) {
  const normalized = status.toLowerCase()
  return <span className={`badge status-badge status-${normalized}`}>{normalized}</span>
}
