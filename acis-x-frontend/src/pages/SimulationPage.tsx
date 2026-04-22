import { useMemo, useState } from 'react'
import { useAgentStatus } from '../hooks/useAgentStatus'
import { useEventStream } from '../hooks/useEventStream'
import { AgentActivityPanel } from '../components/simulation/AgentActivityPanel'
import { EventLog } from '../components/simulation/EventLog'
import { KafkaBusCanvas } from '../components/simulation/KafkaBusCanvas'
import { RegistryPanel } from '../components/simulation/RegistryPanel'
import { SelfHealingSequence } from '../components/simulation/SelfHealingSequence'
import { SimulationControls } from '../components/simulation/SimulationControls'

export default function SimulationPage() {
  const { data: statusData } = useAgentStatus()
  const { events, status } = useEventStream()
  const [speed, setSpeed] = useState(1)
  const [focusAgent, setFocusAgent] = useState('')

  const latestEvent = useMemo(() => events.at(-1), [events])

  return (
    <div className="simulation-page">
      <header className="page-header">
        <div>
          <h1 className="page-title">SIMULATION</h1>
          <p className="page-subtitle">Kafka event bus and autonomous agent flow visualization</p>
        </div>
      </header>

      <SimulationControls
        speed={speed}
        onSpeedChange={setSpeed}
        focusAgent={focusAgent}
        onFocusAgentChange={setFocusAgent}
        streamStatus={status}
      />

      <section className="canvas-grid">
        <KafkaBusCanvas events={events} focusAgent={focusAgent} />
        <RegistryPanel agents={statusData?.agents ?? []} />
      </section>

      <section className="event-zone">
        <EventLog events={events} focusAgent={focusAgent || undefined} />
        <AgentActivityPanel agents={statusData?.agents ?? []} />
      </section>

      <SelfHealingSequence latestEvent={latestEvent} />
    </div>
  )
}
