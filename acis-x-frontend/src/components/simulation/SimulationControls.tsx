import { useMemo } from 'react'
import type { EventStreamStatus } from '../../types/events'
import { BUSINESS_AGENTS } from '../../types/agent'
import { LiveIndicator } from '../ui/LiveIndicator'

type SimulationControlsProps = {
  speed: number
  onSpeedChange: (speed: number) => void
  focusAgent: string
  onFocusAgentChange: (agent: string) => void
  streamStatus: EventStreamStatus
}

export function SimulationControls({
  speed,
  onSpeedChange,
  focusAgent,
  onFocusAgentChange,
  streamStatus,
}: SimulationControlsProps) {
  const speeds = useMemo(() => [0.5, 1, 2, 4], [])

  return (
    <section className="simulation-controls surface-card">
      <div className="controls-left">
        <span className="mono controls-label">SPEED</span>
        <div className="speed-buttons">
          {speeds.map((candidate) => (
            <button
              key={candidate}
              type="button"
              className={candidate === speed ? 'button-dark active' : 'button-dark'}
              onClick={() => onSpeedChange(candidate)}
            >
              {candidate}x
            </button>
          ))}
        </div>
      </div>

      <div className="controls-center">
        <label className="mono controls-label" htmlFor="focus-agent">
          FOCUS AGENT
        </label>
        <select
          id="focus-agent"
          className="select-dark"
          value={focusAgent}
          onChange={(event) => onFocusAgentChange(event.target.value)}
        >
          <option value="">All Agents</option>
          {BUSINESS_AGENTS.map((agent) => (
            <option key={agent} value={agent}>
              {agent}
            </option>
          ))}
        </select>
      </div>

      <div className="controls-right">
        <LiveIndicator status={streamStatus} />
      </div>
    </section>
  )
}
