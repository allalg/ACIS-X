import { useMemo } from 'react'
import type { EventStreamStatus } from '../../types/events'
import { BUSINESS_AGENTS } from '../../types/agent'
import { LiveIndicator } from '../ui/LiveIndicator'

type SimulationControlsProps = {
  focusAgent: string
  onFocusAgentChange: (agent: string) => void
  streamStatus: EventStreamStatus
}

export function SimulationControls({
  focusAgent,
  onFocusAgentChange,
  streamStatus,
}: SimulationControlsProps) {
  return (
    <section className="simulation-controls surface-card">
      <div className="controls-left">
        {/* Removed Speed control as it conflicts with live backend streaming */}
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
