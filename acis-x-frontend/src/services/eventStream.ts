import type { EventEnvelope, EventStreamStatus } from '../types/events'
import { API_BASE_URL, API_KEY } from './api'

export type EventCallback = (event: EventEnvelope) => void
export type StatusCallback = (status: EventStreamStatus) => void

const STREAM_URL =
  import.meta.env.VITE_STREAM_URL ?? `${API_BASE_URL}/api/v1/events/stream`

class EventStreamService {
  private source: EventSource | null = null
  private reconnectTimeout: number | undefined
  private reconnectAttempt = 0
  private maxBackoff = 30000
  private listeners = new Set<EventCallback>()
  private statusListeners = new Set<StatusCallback>()
  private status: EventStreamStatus = 'disconnected'

  connect() {
    if (this.source) {
      return
    }

    this.setStatus('reconnecting')

    const url = new URL(STREAM_URL)
    if (API_KEY) {
      // Native EventSource does not support custom headers, so key is passed in query.
      url.searchParams.set('api_key', API_KEY)
    }
    url.searchParams.set('bypass-tunnel-reminder', 'true')
    url.searchParams.set('ngrok-skip-browser-warning', 'true')

    this.source = new EventSource(url.toString())

    this.source.addEventListener('open', () => {
      this.reconnectAttempt = 0
      this.setStatus('connected')
    })

    this.source.addEventListener('acis_event', (event) => {
      const parsed = JSON.parse((event as MessageEvent<string>).data) as EventEnvelope
      this.listeners.forEach((listener) => listener(parsed))
    })

    this.source.addEventListener('error', () => {
      this.setStatus('reconnecting')
      this.cleanupSource()
      this.scheduleReconnect()
    })
  }

  disconnect() {
    this.clearReconnectTimeout()
    this.cleanupSource()
    this.setStatus('disconnected')
  }

  subscribe(callback: EventCallback): () => void {
    this.listeners.add(callback)
    return () => {
      this.listeners.delete(callback)
    }
  }

  subscribeStatus(callback: StatusCallback): () => void {
    this.statusListeners.add(callback)
    callback(this.status)
    return () => {
      this.statusListeners.delete(callback)
    }
  }

  private scheduleReconnect() {
    this.clearReconnectTimeout()
    this.reconnectAttempt += 1
    const delay = Math.min(1000 * 2 ** (this.reconnectAttempt - 1), this.maxBackoff)
    this.reconnectTimeout = window.setTimeout(() => {
      this.cleanupSource()
      this.connect()
    }, delay)
  }

  private cleanupSource() {
    if (this.source) {
      this.source.close()
      this.source = null
    }
  }

  private clearReconnectTimeout() {
    if (this.reconnectTimeout) {
      window.clearTimeout(this.reconnectTimeout)
      this.reconnectTimeout = undefined
    }
  }

  private setStatus(status: EventStreamStatus) {
    this.status = status
    this.statusListeners.forEach((listener) => listener(status))
  }
}

export const eventStreamService = new EventStreamService()
