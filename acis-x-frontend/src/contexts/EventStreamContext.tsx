import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren,
} from 'react'
import { eventStreamService } from '../services/eventStream'
import type { EventEnvelope, EventStreamStatus } from '../types/events'

type EventStreamContextValue = {
  events: EventEnvelope[]
  status: EventStreamStatus
  subscribeByType: (eventType: string, callback: (event: EventEnvelope) => void) => () => void
}

const EventStreamContext = createContext<EventStreamContextValue | undefined>(undefined)

const MAX_EVENTS = 600

export function EventStreamProvider({ children }: PropsWithChildren) {
  const [events, setEvents] = useState<EventEnvelope[]>([])
  const [status, setStatus] = useState<EventStreamStatus>('disconnected')

  useEffect(() => {
    eventStreamService.connect()

    const unsubscribeEvents = eventStreamService.subscribe((event) => {
      setEvents((prev) => {
        const next = [...prev, event]
        if (next.length > MAX_EVENTS) {
          return next.slice(next.length - MAX_EVENTS)
        }
        return next
      })
    })

    const unsubscribeStatus = eventStreamService.subscribeStatus(setStatus)

    return () => {
      unsubscribeEvents()
      unsubscribeStatus()
      eventStreamService.disconnect()
    }
  }, [])

  const subscribeByType = useCallback(
    (eventType: string, callback: (event: EventEnvelope) => void) => {
      return eventStreamService.subscribe((event) => {
        if (event.event_type === eventType) {
          callback(event)
        }
      })
    },
    [],
  )

  const value = useMemo(
    () => ({
      events,
      status,
      subscribeByType,
    }),
    [events, status, subscribeByType],
  )

  return <EventStreamContext.Provider value={value}>{children}</EventStreamContext.Provider>
}

export function useEventStreamContext() {
  const ctx = useContext(EventStreamContext)
  if (!ctx) {
    throw new Error('useEventStreamContext must be used inside EventStreamProvider')
  }
  return ctx
}
