import { useEffect } from 'react'
import { toast } from 'sonner'
import type { EventEnvelope } from '../../types/events'

type SelfHealingSequenceProps = {
  latestEvent?: EventEnvelope
}

export function SelfHealingSequence({ latestEvent }: SelfHealingSequenceProps) {
  useEffect(() => {
    if (!latestEvent) {
      return
    }

    if (latestEvent.event_type === 'self.healing.triggered') {
      const reason = String(latestEvent.payload.reason ?? 'unknown reason')
      toast.warning(`Fault detected: ${latestEvent.event_source} - ${reason}`)
    }

    if (latestEvent.event_type === 'agent.health' && String(latestEvent.payload.status ?? '') === 'healthy') {
      toast.success(`Recovered: ${latestEvent.event_source} restored to operational state`)
    }
  }, [latestEvent])

  return null
}
