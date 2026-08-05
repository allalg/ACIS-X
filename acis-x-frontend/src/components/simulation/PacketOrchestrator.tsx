import { AnimatePresence } from 'framer-motion'
import { EVENT_ABBREVIATIONS, type EventEnvelope } from '../../types/events'
import { DataPacket } from './DataPacket'

type PacketOrchestratorProps = {
  events: EventEnvelope[]
  positionMap: Record<string, { x: number; y: number }>
  focusAgent?: string
}

export function PacketOrchestrator({ events, positionMap, focusAgent }: PacketOrchestratorProps) {
  // Exclude background system heartbeats/metrics from data packet animations
  const businessEvents = events.filter(
    (e) => e.event_type !== 'agent.heartbeat' && e.event_type !== 'agent.metrics.updated'
  )
  const recentEvents = businessEvents.slice(-5)

  return (
    <AnimatePresence>
      {recentEvents
        .filter((event) => (focusAgent ? event.event_source === focusAgent : true))
        .map((event) => {
          const position = positionMap[event.event_source]
          if (!position) {
            return null
          }
          const abbreviation = EVENT_ABBREVIATIONS[event.event_type] ?? event.event_type.slice(0, 4).toUpperCase()
          
          // Compute trajectory from agent node to bus ring
          // Assuming agent is at radius 180, bus ring is at radius 100
          // If position is relative to (0,0):
          const rAgent = Math.sqrt(position.x * position.x + position.y * position.y)
          const ratio = rAgent > 0 ? 100 / rAgent : 0
          const toX = position.x * ratio
          const toY = position.y * ratio

          return (
            <DataPacket
              key={event.event_id}
              id={event.event_id}
              label={abbreviation}
              colorClass="agent-color-blue"
              fromX={position.x}
              fromY={position.y}
              toX={toX}
              toY={toY}
            />
          )
        })}
    </AnimatePresence>
  )
}
