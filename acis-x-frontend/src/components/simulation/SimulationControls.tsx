import { useState } from 'react'
import { toast } from 'sonner'
import type { EventStreamStatus } from '../../types/events'
import { BUSINESS_AGENTS } from '../../types/agent'
import { LiveIndicator } from '../ui/LiveIndicator'
import { api } from '../../services/api'

type SimulationControlsProps = {
  focusAgent: string
  onFocusAgentChange: (agent: string) => void
  streamStatus: EventStreamStatus
  agents: any[]
  isPaused: boolean
  onControlAction: (action: 'pause_scenario' | 'freeze_all' | 'resume') => void
}

export function SimulationControls({
  focusAgent,
  onFocusAgentChange,
  streamStatus,
  agents,
  isPaused,
  onControlAction,
}: SimulationControlsProps) {
  const [selectedAgentInstance, setSelectedAgentInstance] = useState('')

  const handleInjectFault = async () => {
    if (!selectedAgentInstance) {
      toast.error('Please select an agent instance to inject fault.')
      return
    }
    try {
      await api.simulationFault(selectedAgentInstance)
      toast.success(`Restart command sent for agent instance ${selectedAgentInstance}.`)
    } catch (err) {
      toast.error('Failed to inject fault / restart agent.')
    }
  }

  return (
    <section className="simulation-controls surface-card" style={{ display: 'flex', flexDirection: 'column', gap: '1rem', padding: '1.2rem' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', width: '100%', flexWrap: 'wrap', gap: '1rem' }}>
        <div className="controls-left" style={{ display: 'flex', gap: '0.6rem' }}>
          {isPaused ? (
            <button
              className="button-dark active"
              onClick={() => onControlAction('resume')}
              style={{ minWidth: '120px', backgroundColor: 'var(--accent-green)', color: '#000', fontWeight: 'bold' }}
            >
              ▶ RESUME ALL
            </button>
          ) : (
            <>
              <button
                className="button-dark"
                onClick={() => onControlAction('pause_scenario')}
                style={{ backgroundColor: 'var(--bg-elevated)', border: '1px solid var(--accent-amber)', color: 'var(--accent-amber)' }}
                title="Pause new scenario data generation, but leave time ticking & AR aging active"
              >
                ⏸ PAUSE DATA ONLY
              </button>
              <button
                className="button-dark"
                onClick={() => onControlAction('freeze_all')}
                style={{ backgroundColor: 'var(--bg-elevated)', border: '1px solid var(--accent-blue)', color: 'var(--accent-blue)' }}
                title="Freeze all event generators and simulated time ticks for static inspection"
              >
                ❄️ FREEZE ALL
              </button>
            </>
          )}
        </div>

        <div className="controls-center" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
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
      </div>

      <div style={{ display: 'flex', borderTop: '1px solid var(--bg-border)', paddingTop: '0.8rem', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '1rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
          <label className="mono controls-label" htmlFor="fault-agent" style={{ color: 'var(--accent-red)' }}>
            FAULT INJECTOR (RESTART AGENT)
          </label>
          <select
            id="fault-agent"
            className="select-dark"
            value={selectedAgentInstance}
            onChange={(event) => setSelectedAgentInstance(event.target.value)}
            style={{ minWidth: '200px' }}
          >
            <option value="">Select Running Instance...</option>
            {agents
              .filter(a => a.status === 'healthy')
              .map((a) => (
                <option key={a.agent_id} value={a.agent_id}>
                  {a.agent_name} ({a.agent_id.substring(a.agent_id.length - 8)})
                </option>
              ))}
          </select>
          <button
            className="button-dark"
            onClick={handleInjectFault}
            style={{
              backgroundColor: 'rgba(239, 68, 68, 0.15)',
              border: '1px solid var(--accent-red)',
              color: 'var(--accent-red)',
            }}
          >
            KILL & REBOOT
          </button>
        </div>
      </div>
    </section>
  )
}
