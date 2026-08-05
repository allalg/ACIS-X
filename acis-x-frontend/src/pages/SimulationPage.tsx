import { useMemo, useState } from 'react'
import { toast } from 'sonner'
import { useAgentStatus } from '../hooks/useAgentStatus'
import { useEventStream } from '../hooks/useEventStream'
import { AgentActivityPanel } from '../components/simulation/AgentActivityPanel'
import { EventLog } from '../components/simulation/EventLog'
import { KafkaBusCanvas } from '../components/simulation/KafkaBusCanvas'
import { RegistryPanel } from '../components/simulation/RegistryPanel'
import { SelfHealingSequence } from '../components/simulation/SelfHealingSequence'
import { SimulationControls } from '../components/simulation/SimulationControls'
import { AuxiliaryAgentsPanel } from '../components/simulation/AuxiliaryAgentsPanel'
import { AgentDetailModal } from '../components/simulation/AgentDetailModal'
import { LogConsole } from '../components/diagnostics/LogConsole'
import { api } from '../services/api'

export default function SimulationPage() {
  const { data: statusData } = useAgentStatus()
  const { events, status } = useEventStream()
  const [focusAgent, setFocusAgent] = useState('')
  const [selectedModalAgent, setSelectedModalAgent] = useState<string | null>(null)
  const [isPaused, setIsPaused] = useState(false)
  const [pauseMode, setPauseMode] = useState<'running' | 'scenario_only' | 'freeze_all'>('running')

  const latestEvent = useMemo(() => events.at(-1), [events])

  const handleControlAction = async (action: 'pause_scenario' | 'freeze_all' | 'resume') => {
    try {
      await api.simulationControl(action)
      if (action === 'resume') {
        setIsPaused(false)
        setPauseMode('running')
        toast.success('Simulation RESUMED across all agents & time ticks.')
      } else if (action === 'pause_scenario') {
        setIsPaused(true)
        setPauseMode('scenario_only')
        toast.success('Scenario generator PAUSED. Time ticks & AR aging remain active.')
      } else if (action === 'freeze_all') {
        setIsPaused(true)
        setPauseMode('freeze_all')
        toast.success('Simulation FROZEN completely. All data generation & time ticks paused.')
      }
    } catch (err) {
      toast.error('Failed to update simulation control state.')
    }
  }

  return (
    <div className="simulation-page">
      <header className="page-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '1rem' }}>
        <div>
          <h1 className="page-title">SIMULATION</h1>
          <p className="page-subtitle">Kafka event bus and autonomous agent flow visualization</p>
        </div>
        {isPaused && (
          <div
            className="mono"
            style={{
              backgroundColor: pauseMode === 'scenario_only' ? 'rgba(245, 158, 11, 0.15)' : 'rgba(59, 130, 246, 0.15)',
              border: `1px solid ${pauseMode === 'scenario_only' ? 'var(--accent-amber)' : 'var(--accent-blue)'}`,
              color: pauseMode === 'scenario_only' ? 'var(--accent-amber)' : 'var(--accent-blue)',
              padding: '0.6rem 1.2rem',
              borderRadius: '8px',
              fontSize: '0.85rem',
              fontWeight: 'bold',
              display: 'flex',
              alignItems: 'center',
              gap: '0.6rem',
            }}
          >
            {pauseMode === 'scenario_only'
              ? '⏸ SCENARIO DATA PAUSED — Time Ticks & Autonomous AR Pipeline Active'
              : '❄️ SIMULATION FROZEN — All Event Generators & Time Ticks Paused'}
          </div>
        )}
      </header>

      <SimulationControls
        focusAgent={focusAgent}
        onFocusAgentChange={setFocusAgent}
        streamStatus={status}
        agents={statusData?.agents ?? []}
        isPaused={isPaused}
        onControlAction={handleControlAction}
      />

      <section className="canvas-grid">
        <KafkaBusCanvas
          events={events}
          focusAgent={focusAgent}
          onSelectAgent={(ag) => setSelectedModalAgent(ag)}
        />
        <RegistryPanel
          agents={statusData?.agents ?? []}
          onSelectAgent={(ag) => setSelectedModalAgent(ag)}
        />
      </section>

      <section className="event-zone">
        <EventLog events={events} focusAgent={focusAgent || undefined} />
        <AgentActivityPanel
          agents={statusData?.agents ?? []}
          onSelectAgent={(ag) => setSelectedModalAgent(ag)}
        />
      </section>

      <AuxiliaryAgentsPanel
        agentsStatus={statusData?.agents ?? []}
        onSelectAgent={(ag) => setSelectedModalAgent(ag)}
      />

      <LogConsole />

      <SelfHealingSequence latestEvent={latestEvent} />

      <AgentDetailModal
        agentName={selectedModalAgent}
        agentsStatus={statusData?.agents ?? []}
        events={events}
        onClose={() => setSelectedModalAgent(null)}
      />
    </div>
  )
}
