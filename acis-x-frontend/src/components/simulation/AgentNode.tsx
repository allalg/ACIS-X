import { motion } from 'framer-motion'

type AgentNodeProps = {
  x: number
  y: number
  label: string
  colorClass: string
  size?: 'business' | 'operational'
  status?: 'idle' | 'active' | 'processing' | 'error' | 'heartbeat'
}

export function AgentNode({
  x,
  y,
  label,
  colorClass,
  size = 'business',
  status = 'idle',
}: AgentNodeProps) {
  const radius = size === 'business' ? 22 : 16
  return (
    <g className={`agent-node ${colorClass} status-${status}`}>
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
      {status === 'error' ? <text x={x} y={y - radius - 8} className="agent-err">ERR</text> : null}
      <text x={x} y={y + radius + 16} textAnchor="middle" className="agent-label">
        {label}
      </text>
    </g>
  )
}
