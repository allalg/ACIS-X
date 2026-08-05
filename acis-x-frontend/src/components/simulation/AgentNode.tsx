import { motion } from 'framer-motion'

type AgentNodeProps = {
  x: number
  y: number
  label: string
  colorClass: string
  size?: 'business' | 'operational' | 'center'
  status?: 'idle' | 'active' | 'processing' | 'error' | 'heartbeat'
  labelAngle?: number // 0 to 360 degrees
  isFocused?: boolean
  onClick?: () => void
}

export function AgentNode({
  x,
  y,
  label,
  colorClass,
  size = 'business',
  status = 'idle',
  labelAngle,
  isFocused,
  onClick,
}: AgentNodeProps) {
  const radius = size === 'center' ? 32 : size === 'business' ? 22 : 16

  // Calculate smart label positioning if angle is provided
  let labelX = x
  let labelY = y + radius + 16
  let anchor: 'start' | 'middle' | 'end' = 'middle'

  if (labelAngle !== undefined) {
    const rad = (labelAngle * Math.PI) / 180
    const distance = radius + 14
    labelX = x + Math.cos(rad) * distance
    labelY = y + Math.sin(rad) * distance + 4 // +4 for vertical text centering

    const normalizedAngle = (labelAngle % 360 + 360) % 360
    if (normalizedAngle > 315 || normalizedAngle <= 45) {
      anchor = 'start'
      labelX += 4
    } else if (normalizedAngle > 135 && normalizedAngle <= 225) {
      anchor = 'end'
      labelX -= 4
    } else if (normalizedAngle > 45 && normalizedAngle <= 135) {
      anchor = 'middle'
      labelY += 10
    } else {
      anchor = 'middle'
      labelY -= 14
    }
  }

  return (
    <g
      className={`agent-node ${colorClass} status-${status}`}
      style={{ opacity: isFocused === false ? 0.35 : 1, transition: 'opacity 0.3s', cursor: 'pointer' }}
      onClick={onClick}
    >
      {isFocused && (
        <motion.circle
          cx={x}
          cy={y}
          r={radius + 8}
          fill="none"
          stroke="var(--accent-amber)"
          strokeWidth="2.5"
          strokeDasharray="4 4"
          initial={{ opacity: 0, scale: 0.8 }}
          animate={{ opacity: 1, scale: 1, rotate: 360 }}
          transition={{ rotate: { duration: 8, repeat: Infinity, ease: 'linear' } }}
        />
      )}
      <motion.circle
        cx={x}
        cy={y}
        r={radius + 4}
        className="agent-node-pulse"
        initial={{ opacity: 0, scale: 1 }}
        animate={status === 'active' || status === 'heartbeat' ? { opacity: [0, 0.75, 0], scale: [1, 1.4, 1] } : { opacity: 0, scale: 1 }}
        transition={{ duration: status === 'heartbeat' ? 0.3 : 0.9, repeat: status === 'processing' ? Infinity : 0 }}
      />
      <circle cx={x} cy={y} r={radius} className="agent-node-main" />
      {status === 'processing' ? <circle cx={x} cy={y} r={radius + 3} className="agent-node-spinner" /> : null}
      {status === 'error' ? <text x={x} y={y - radius - 8} className="agent-err" textAnchor="middle">ERR</text> : null}
      <text x={labelX} y={labelY} textAnchor={anchor} className="agent-label">
        {label}
      </text>
    </g>
  )
}
