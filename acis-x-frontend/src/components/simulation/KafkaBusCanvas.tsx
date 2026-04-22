import { useMemo } from 'react'
import { BUSINESS_AGENTS, OPERATIONAL_AGENTS } from '../../types/agent'
import type { EventEnvelope } from '../../types/events'
import { AgentBranch } from './AgentBranch'
import { AgentNode } from './AgentNode'
import { BusLine } from './BusLine'
import { OperationalAgentStrip } from './OperationalAgentStrip'
import { PacketOrchestrator } from './PacketOrchestrator'

type KafkaBusCanvasProps = {
  events: EventEnvelope[]
  focusAgent: string
}

const colorClasses = [
  'agent-color-scenario',
  'agent-color-customer-state',
  'agent-color-aggregator',
  'agent-color-profile',
  'agent-color-risk-scoring',
  'agent-color-payment-pred',
  'agent-color-collections',
  'agent-color-overdue',
  'agent-color-credit-policy',
  'agent-color-external-data',
  'agent-color-external-scrape',
  'agent-color-db',
  'agent-color-memory',
  'agent-color-query',
  'agent-color-time-tick',
]

export function KafkaBusCanvas({ events, focusAgent }: KafkaBusCanvasProps) {
  const spacing = 920 / BUSINESS_AGENTS.length

  const positions = useMemo(
    () => BUSINESS_AGENTS.map((_, index) => 60 + spacing * index),
    [spacing],
  )

  const opPositions = useMemo(() => [430, 480, 530, 580, 630], [])

  const lastEventsBySource = useMemo(() => {
    const map = new Map<string, EventEnvelope>()
    for (const event of events.slice(-100)) {
      map.set(event.event_source, event)
    }
    return map
  }, [events])

  const positionMap = useMemo(
    () =>
      BUSINESS_AGENTS.reduce<Record<string, { x: number; y: number }>>((acc, agent, index) => {
        acc[agent] = { x: positions[index], y: 180 }
        return acc
      }, {}),
    [positions],
  )

  return (
    <section className="kafka-canvas surface-card">
      <svg viewBox="0 0 1000 360" className="kafka-svg" preserveAspectRatio="xMidYMid meet">
        <BusLine />

        <OperationalAgentStrip agents={[...OPERATIONAL_AGENTS]} positions={opPositions} />

        {BUSINESS_AGENTS.map((agent, index) => {
          const sourceEvent = lastEventsBySource.get(agent)
          const ageMs = sourceEvent
            ? Date.now() - new Date(sourceEvent.event_time).getTime()
            : Number.MAX_SAFE_INTEGER
          const status = ageMs < 1200 ? 'active' : ageMs < 4500 ? 'processing' : 'idle'

          return (
            <g key={agent}>
              <AgentBranch
                x={positions[index]}
                fromY={180}
                toY={262}
                colorClass={colorClasses[index]}
                dimmed={status === 'idle'}
              />
              <AgentNode
                x={positions[index]}
                y={284}
                label={agent}
                colorClass={colorClasses[index]}
                status={status}
              />
            </g>
          )
        })}

        <PacketOrchestrator events={events} positionMap={positionMap} focusAgent={focusAgent || undefined} />
      </svg>
    </section>
  )
}
