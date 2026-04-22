import { AnimatePresence } from 'framer-motion'
import { EVENT_ABBREVIATIONS, type EventEnvelope } from '../../types/events'
import { DataPacket } from './DataPacket'

type PacketOrchestratorProps = {
  events: EventEnvelope[]
  positionMap: Record<string, { x: number; y: number }>
  focusAgent?: string
}

export function PacketOrchestrator({ events, positionMap, focusAgent }: PacketOrchestratorProps) {
  const recentEvents = events.slice(-20)

  return (
    <AnimatePresence>
      {recentEvents
        .filter((event) => (focusAgent ? event.event_source === focusAgent : true))
        .map((event, index) => {
          const position = positionMap[event.event_source]
          if (!position) {
            return null
          }
          const abbreviation = EVENT_ABBREVIATIONS[event.event_type] ?? event.event_type.slice(0, 4).toUpperCase()
          return (
            <DataPacket
              key={event.event_id}
              id={event.event_id}
              label={abbreviation}
              colorClass="agent-color-blue"
              x={position.x}
              y={160 + ((index % 3) - 1) * 16}
            />
          )
        })}
    </AnimatePresence>
  )
}
