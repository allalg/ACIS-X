import { useMemo } from 'react'
import { PIPELINE_AGENTS, OPERATIONAL_AGENTS } from '../../types/agent'
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
  const spacing = 820 / PIPELINE_AGENTS.length

  const positions = useMemo(
    () => PIPELINE_AGENTS.map((_, index) => 100 + spacing * index),
    [spacing],
  )

  const opPositions = useMemo(() => [300, 400, 500, 600, 700], [])

  const lastEventsBySource = useMemo(() => {
    const map = new Map<string, EventEnvelope>()
    for (const event of events.slice(-100)) {
      map.set(event.event_source, event)
    }
    return map
  }, [events])

  const positionMap = useMemo(
    () =>
      PIPELINE_AGENTS.reduce<Record<string, { x: number; y: number }>>((acc, agent, index) => {
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

        {PIPELINE_AGENTS.map((agent, index) => {
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
                colorClass={pipelineColors[index]}
                dimmed={status === 'idle'}
              />
              <AgentNode
                x={positions[index]}
                y={284}
                label={agent}
                colorClass={pipelineColors[index]}
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
