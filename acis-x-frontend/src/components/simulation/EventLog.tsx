import { useMemo } from 'react'
import { formatTimestamp } from '../../lib/utils'
import type { EventEnvelope } from '../../types/events'

type EventLogProps = {
  events: EventEnvelope[]
  focusAgent?: string
}

export function EventLog({ events, focusAgent }: EventLogProps) {
  const filtered = useMemo(
    () => (focusAgent ? events.filter((event) => event.event_source === focusAgent) : events),
    [events, focusAgent],
  )

  return (
    <section className="event-log surface-card">
      <header>
        <h3>EVENT STREAM</h3>
        <span className="mono">{filtered.length}</span>
      </header>
      <div className="event-list-wrap">
        {filtered.slice(-240).map((event) => {
          const source = event.event_source.replace('Agent', '')
          return (
            <div key={event.event_id} className="event-row">
              <span className="event-time numeric">{formatTimestamp(event.event_time)}</span>
              <span className="event-id mono">{event.event_id.slice(0, 8)}...</span>
              <span className="event-type mono">{event.event_type}</span>
              <span className="event-entity mono">{event.entity_id}</span>
              <span className="event-source mono">{source}</span>
            </div>
          )
        })}
      </div>
    </section>
  )
}
