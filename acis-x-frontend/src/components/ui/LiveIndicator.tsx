import type { EventStreamStatus } from '../../types/events'

type LiveIndicatorProps = {
  status: EventStreamStatus
}

export function LiveIndicator({ status }: LiveIndicatorProps) {
  const label = status === 'connected' ? 'LIVE' : status === 'reconnecting' ? 'RECONNECTING' : 'OFFLINE'
  const className =
    status === 'connected'
      ? 'live-indicator live-connected'
      : status === 'reconnecting'
        ? 'live-indicator live-reconnecting'
        : 'live-indicator live-offline'

  return (
    <span className={className}>
      <span className="live-dot" />
      {label}
    </span>
  )
}
