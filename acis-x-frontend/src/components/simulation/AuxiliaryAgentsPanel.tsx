import { AGENT_KNOWLEDGE_BASE } from '../../data/agentKnowledge'

type AuxiliaryAgentsPanelProps = {
  agentsStatus: any[]
  onSelectAgent: (agentName: string) => void
}

export function AuxiliaryAgentsPanel({ agentsStatus, onSelectAgent }: AuxiliaryAgentsPanelProps) {
  const infraAgents = Object.values(AGENT_KNOWLEDGE_BASE).filter(
    (a) => a.category === 'infrastructure' || a.category === 'operational'
  )

  return (
    <section className="surface-card" style={{ padding: '1.2rem', display: 'flex', flexDirection: 'column', gap: '1rem' }}>
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h3 style={{ margin: 0, fontSize: '0.95rem', letterSpacing: '0.05em' }}>AUXILIARY & INFRASTRUCTURE AGENTS</h3>
        <span className="mono" style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
          {infraAgents.length} Agents Active
        </span>
      </header>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '0.8rem' }}>
        {infraAgents.map((ag) => {
          const liveStatus = agentsStatus.find((a) => a.agent_name === ag.name || a.agent_type === ag.name)
          const isHealthy = liveStatus?.status === 'healthy'
          const isRestarting = liveStatus?.status === 'restarting'

          return (
            <div
              key={ag.name}
              onClick={() => onSelectAgent(ag.name)}
              className="surface-card-hover"
              style={{
                padding: '0.8rem 1rem',
                backgroundColor: 'var(--bg-elevated)',
                border: '1px solid var(--bg-border)',
                borderRadius: '8px',
                cursor: 'pointer',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                transition: 'all 0.2s',
              }}
            >
              <div>
                <div style={{ fontWeight: 'bold', fontSize: '0.85rem', color: 'var(--text-primary)' }}>{ag.name}</div>
                <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>{ag.category}</div>
              </div>

              <span
                style={{
                  width: '8px',
                  height: '8px',
                  borderRadius: '50%',
                  backgroundColor: isRestarting
                    ? 'var(--accent-red)'
                    : isHealthy
                    ? 'var(--accent-green)'
                    : 'var(--text-muted)',
                  boxShadow: isHealthy ? '0 0 6px var(--accent-green)' : undefined,
                }}
              />
            </div>
          )
        })}
      </div>
    </section>
  )
}
