import { useMemo, useState } from 'react'
import { formatTimestamp } from '../../lib/utils'
import type { EventEnvelope } from '../../types/events'

type EventLogProps = {
  events: EventEnvelope[]
  focusAgent?: string
}

export function EventLog({ events, focusAgent }: EventLogProps) {
  const [hideHeartbeats, setHideHeartbeats] = useState(true)

  const filtered = useMemo(() => {
    let res = focusAgent ? events.filter((event) => event.event_source === focusAgent) : events
    if (hideHeartbeats) {
      res = res.filter(
        (e) => e.event_type !== 'agent.heartbeat' && e.event_type !== 'agent.metrics.updated',
      )
    }
    return res
  }, [events, focusAgent, hideHeartbeats])

  return (
    <section className="event-log surface-card">
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.8rem' }}>
          <h3 style={{ margin: 0 }}>EVENT STREAM</h3>
          <button
            className={hideHeartbeats ? 'button-dark active' : 'button-dark'}
            onClick={() => setHideHeartbeats(!hideHeartbeats)}
            style={{ fontSize: '0.75rem', padding: '0.2rem 0.5rem' }}
          >
            {hideHeartbeats ? 'FILTER: HIDING HEARTBEATS' : 'SHOW ALL EVENTS'}
          </button>
        </div>
        <span className="mono">{filtered.length >= 600 ? '600+' : filtered.length}</span>
      </header>
      <div className="event-list-wrap">
        {filtered.slice(-240).map((event) => {
          const source = event.event_source.replace('Agent', '')
          const isRestart = event.event_type === 'agent.restart.requested'
          const isPauseResume = event.event_type === 'scenario.pause' || event.event_type === 'scenario.resume'
          const isHeartbeat = event.event_type === 'agent.heartbeat' || event.event_type === 'agent.metrics.updated'

          return (
            <div
              key={event.event_id}
              className="event-row"
              style={{
                backgroundColor: isRestart
                  ? 'rgba(239, 68, 68, 0.15)'
                  : isPauseResume
                  ? 'rgba(245, 158, 11, 0.15)'
                  : undefined,
                opacity: isHeartbeat ? 0.6 : 1,
                fontWeight: isRestart || isPauseResume ? 'bold' : 'normal',
              }}
            >
              <span className="event-time numeric">{formatTimestamp(event.event_time)}</span>
              <span className="event-id mono">{event.event_id.slice(0, 8)}...</span>
              <span
                className="event-type mono"
                style={{
                  color: isRestart
                    ? 'var(--accent-red)'
                    : isPauseResume
                    ? 'var(--accent-amber)'
                    : isHeartbeat
                    ? 'var(--text-muted)'
                    : undefined,
                }}
              >
                {isRestart ? '⚡ RESTART_TRIGGERED' : isHeartbeat ? '💚 HEARTBEAT (HEALTH PING)' : event.event_type}
              </span>
              <span className="event-entity mono">{event.entity_id}</span>
              <span className="event-source mono">{source}</span>
            </div>
          )
        })}
      </div>
    </section>
  )
}
