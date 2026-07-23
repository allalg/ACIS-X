import { useMemo, useState, useEffect } from 'react'
import { AnimatePresence, motion } from 'framer-motion'
import { PIPELINE_AGENTS, OPERATIONAL_AGENTS } from '../../types/agent'
import type { EventEnvelope } from '../../types/events'
import { AgentNode } from './AgentNode'
import { BusLine } from './BusLine'
import { PacketOrchestrator } from './PacketOrchestrator'

type KafkaBusCanvasProps = {
  events: EventEnvelope[]
  focusAgent: string
}

// Map pipeline agents to their assigned color classes
const pipelineColors = [
  'agent-color-scenario',
  'agent-color-customer-state',
  'agent-color-profile',
  'agent-color-payment-pred',
  'agent-color-risk-scoring',
  'agent-color-credit-policy',
  'agent-color-collections',
]

export function KafkaBusCanvas({ events, focusAgent }: KafkaBusCanvasProps) {
  const [recoveryBeams, setRecoveryBeams] = useState<{ id: string; target: string; action: string }[]>([])

  // Determine outer ring agents (exclude SelfHealingAgent as it's centered)
  const outerAgents = useMemo(() => {
    const ops = OPERATIONAL_AGENTS.filter(a => a !== 'SelfHealingAgent')
    return [
      ...PIPELINE_AGENTS.map((agent, i) => ({ agent, type: 'pipeline' as const, index: i })),
      ...ops.map((agent, i) => ({ agent, type: 'operational' as const, index: i }))
    ]
  }, [])

  // Calculate coordinates for the radial layout
  // Center is (0,0) due to translation in the svg
  const radius = 180
  const angleStep = 360 / outerAgents.length

  const positions = useMemo(() => {
    const map = new Map<string, { x: number; y: number; angle: number; colorClass: string; size: 'business' | 'operational' | 'center' }>()
    
    // Add center agent
    map.set('SelfHealingAgent', { x: 0, y: 0, angle: 0, colorClass: 'agent-color-monitoring', size: 'center' })

    outerAgents.forEach(({ agent, type, index }, i) => {
      // Start from top (-90 degrees)
      const angle = (i * angleStep - 90) % 360
      const rad = (angle * Math.PI) / 180
      const x = Math.cos(rad) * radius
      const y = Math.sin(rad) * radius
      
      const colorClass = type === 'pipeline' ? pipelineColors[index % pipelineColors.length] : 'agent-color-monitoring'
      const size = type === 'pipeline' ? 'business' : 'operational'
      
      map.set(agent, { x, y, angle, colorClass, size })
    })
    
    return map
  }, [outerAgents, angleStep])

  // Track event activity for statuses
  const lastEventsBySource = useMemo(() => {
    const map = new Map<string, EventEnvelope>()
    for (const event of events.slice(-100)) {
      map.set(event.event_source, event)
    }
    return map
  }, [events])

  // Handle recovery beams
  useEffect(() => {
    const latest = events.at(-1)
    if (latest && latest.event_type === 'recovery.triggered') {
      const target = latest.payload.agent_name as string
      const action = latest.payload.recommended_action as string
      if (target) {
        const id = latest.event_id
        // eslint-disable-next-line react-hooks/set-state-in-effect
        setRecoveryBeams(prev => [...prev, { id, target, action }])
        // Remove beam after animation
        setTimeout(() => {
          setRecoveryBeams(prev => prev.filter(b => b.id !== id))
        }, 1500)
      }
    }
  }, [events])

  // Map to format PacketOrchestrator expects
  const positionMap = useMemo(() => {
    const p: Record<string, { x: number; y: number }> = {}
    positions.forEach((val, key) => p[key] = { x: val.x, y: val.y })
    return p
  }, [positions])

  // Use state to track the timestamp so we don't call Date.now() during render (impure)
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setNow(Date.now())
  }, [events])
  return (
    <section className="kafka-canvas surface-card" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: '400px' }}>
      <svg viewBox="-300 -240 600 480" className="kafka-svg" preserveAspectRatio="xMidYMid meet" style={{ width: '100%', height: '100%' }}>
        <defs>
          <filter id="laserGlow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="4" result="blur" />
            <feComposite in="SourceGraphic" in2="blur" operator="over" />
          </filter>
        </defs>

        <BusLine />

        {/* Draw recovery beams */}
        <AnimatePresence>
          {recoveryBeams.map((beam) => {
            const pos = positions.get(beam.target)
            if (!pos) return null
            return (
              <motion.g key={beam.id} initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
                <line x1="0" y1="0" x2={pos.x} y2={pos.y} stroke="var(--accent-amber)" strokeWidth="4" strokeDasharray="8 8" filter="url(#laserGlow)">
                  <animate attributeName="stroke-dashoffset" from="16" to="0" dur="0.3s" repeatCount="indefinite" />
                </line>
                <text x={pos.x / 2} y={pos.y / 2 - 10} fill="var(--accent-amber)" fontSize="11" fontFamily="var(--font-mono)" textAnchor="middle" filter="url(#laserGlow)">
                  {beam.action.toUpperCase()}
                </text>
              </motion.g>
            )
          })}
        </AnimatePresence>

        {/* Draw all agents */}
        {Array.from(positions.entries()).map(([agent, pos]) => {
          const sourceEvent = lastEventsBySource.get(agent)
          const ageMs = sourceEvent
            ? now - new Date(sourceEvent.event_time).getTime()
            : Number.MAX_SAFE_INTEGER
          
          let status: 'idle' | 'active' | 'processing' | 'error' = 'idle'
          
          if (agent === 'SelfHealingAgent') {
            // SelfHealingAgent is always active/processing if it recently received events
            status = ageMs < 3000 ? 'processing' : 'active'
          } else {
            status = ageMs < 1200 ? 'active' : ageMs < 4500 ? 'processing' : 'idle'
          }
          
          // In real implementation we'd check if status event says error/critical
          if (sourceEvent && sourceEvent.event_type === 'agent.health.critical') {
            status = 'error'
          }

          return (
            <AgentNode
              key={agent}
              x={pos.x}
              y={pos.y}
              label={agent}
              colorClass={pos.colorClass}
              status={status}
              size={pos.size}
              labelAngle={pos.size === 'center' ? undefined : pos.angle}
            />
          )
        })}

        <PacketOrchestrator events={events} positionMap={positionMap} focusAgent={focusAgent || undefined} />
      </svg>
    </section>
  )
}
