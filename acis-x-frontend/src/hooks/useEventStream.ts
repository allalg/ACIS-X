import { useEventStreamContext } from '../contexts/EventStreamContext'

export function useEventStream() {
  return useEventStreamContext()
}
