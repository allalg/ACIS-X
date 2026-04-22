type Severity = 'low' | 'medium' | 'high' | 'critical'

type SeverityBadgeProps = {
  severity: Severity
}

export function SeverityBadge({ severity }: SeverityBadgeProps) {
  return <span className={`badge severity severity-${severity}`}>{severity}</span>
}
