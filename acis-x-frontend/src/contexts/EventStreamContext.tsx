import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type PropsWithChildren,
} from 'react'
import { useQueryClient } from '@tanstack/react-query'
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
  const queryClient = useQueryClient()

  useEffect(() => {
    eventStreamService.connect()

    const unsubscribeEvents = eventStreamService.subscribe((event) => {
      // Invalidate React Query cache based on event type for instant UI updates
      const et = event.event_type || ''
      if (et.startsWith('invoice.')) {
        queryClient.invalidateQueries({ queryKey: ['invoices'] })
        queryClient.invalidateQueries({ queryKey: ['dashboard-summary'] })
      } else if (et.startsWith('payment.')) {
        queryClient.invalidateQueries({ queryKey: ['payments'] })
        queryClient.invalidateQueries({ queryKey: ['metrics'] })
        queryClient.invalidateQueries({ queryKey: ['dashboard-summary'] })
      } else if (et.startsWith('customer.') || et.startsWith('risk.')) {
        queryClient.invalidateQueries({ queryKey: ['customers'] })
        queryClient.invalidateQueries({ queryKey: ['customer-profile'] })
        queryClient.invalidateQueries({ queryKey: ['dashboard-summary'] })
      } else if (et.startsWith('agent.') || et.startsWith('system.')) {
        queryClient.invalidateQueries({ queryKey: ['agents-status'] })
      } else {
        queryClient.invalidateQueries()
      }

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
  }, [queryClient])

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
