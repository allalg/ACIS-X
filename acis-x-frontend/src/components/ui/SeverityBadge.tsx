type Severity = 'low' | 'medium' | 'high' | 'critical' | 'pending'

type SeverityBadgeProps = {
  severity: Severity | string | null | undefined
}

export function SeverityBadge({ severity }: SeverityBadgeProps) {
  const value = (severity || 'pending').toLowerCase()
  return <span className={`badge severity severity-${value}`}>{value}</span>
}
