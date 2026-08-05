import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { toast } from 'sonner'
import { AGENT_KNOWLEDGE_BASE } from '../../data/agentKnowledge'
import type { EventEnvelope } from '../../types/events'
import { api } from '../../services/api'

type AgentDetailModalProps = {
  agentName: string | null
  agentsStatus: any[]
  events: EventEnvelope[]
  onClose: () => void
}

export function AgentDetailModal({
  agentName,
  agentsStatus,
  events,
  onClose,
}: AgentDetailModalProps) {
  const [activeTab, setActiveTab] = useState<'overview' | 'workflow' | 'formulas' | 'data'>('overview')

  if (!agentName) return null

  // Find agent knowledge or generate default fallback
  const knowledge = AGENT_KNOWLEDGE_BASE[agentName] || {
    name: agentName,
    category: 'infrastructure',
    title: `${agentName} Service`,
    purpose: `Executes specialized background processing for ${agentName}.`,
    workflow: [`Processes events and executes asynchronous tasks for ${agentName}.`],
    formulas: [],
    consumedEvents: ['acis.*'],
    emittedEvents: ['agent.*'],
    databaseTables: [],
  }

  // Find live runtime agent instance status
  const agentInstance = agentsStatus.find((a) => a.agent_name === agentName || a.agent_type === agentName)
  const isHealthy = agentInstance?.status === 'healthy'
  const isRestarting = agentInstance?.status === 'restarting'

  // Filter events related to this agent
  const agentEvents = events
    .filter((e) => e.event_source === agentName || e.event_source?.includes(agentName))
    .slice(-15)

  const handleReboot = async () => {
    if (!agentInstance?.agent_id) {
      toast.error('Agent instance ID not found.')
      return
    }
    try {
      await api.simulationFault(agentInstance.agent_id)
      toast.success(`Reboot triggered for ${agentName}`)
    } catch (err) {
      toast.error('Failed to trigger agent reboot.')
    }
  }

  return (
    <AnimatePresence>
      <div
        style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(0, 0, 0, 0.75)',
          backdropFilter: 'blur(6px)',
          zIndex: 9999,
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          padding: '1.5rem',
        }}
        onClick={onClose}
      >
        <motion.div
          initial={{ opacity: 0, scale: 0.9, y: 20 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.9, y: 20 }}
          transition={{ duration: 0.2 }}
          className="surface-card"
          style={{
            width: '100%',
            maxWidth: '850px',
            maxHeight: '90vh',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
            border: '1px solid var(--accent-blue)',
            boxShadow: '0 20px 50px rgba(0, 0, 0, 0.6)',
            padding: 0,
          }}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <header
            style={{
              padding: '1.2rem 1.5rem',
              backgroundColor: 'var(--bg-elevated)',
              borderBottom: '1px solid var(--bg-border)',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              flexWrap: 'wrap',
              gap: '1rem',
            }}
          >
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.8rem' }}>
                <span className="mono" style={{ fontSize: '1.4rem' }}>
                  🤖
                </span>
                <h2 style={{ margin: 0, fontSize: '1.3rem', letterSpacing: '0.05em' }}>{knowledge.name}</h2>
                <span
                  className="mono"
                  style={{
                    fontSize: '0.75rem',
                    padding: '0.2rem 0.6rem',
                    borderRadius: '12px',
                    backgroundColor:
                      knowledge.category === 'business'
                        ? 'rgba(59, 130, 246, 0.15)'
                        : knowledge.category === 'operational'
                        ? 'rgba(245, 158, 11, 0.15)'
                        : 'rgba(168, 85, 247, 0.15)',
                    color:
                      knowledge.category === 'business'
                        ? 'var(--accent-blue)'
                        : knowledge.category === 'operational'
                        ? 'var(--accent-amber)'
                        : 'var(--accent-purple, #a855f7)',
                    border: '1px solid currentColor',
                    textTransform: 'uppercase',
                    fontWeight: 'bold',
                  }}
                >
                  {knowledge.category} AGENT
                </span>
              </div>
              <p style={{ margin: '0.4rem 0 0 0', color: 'var(--text-muted)', fontSize: '0.85rem' }}>
                {knowledge.title}
              </p>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '0.8rem' }}>
              <span
                className="mono"
                style={{
                  fontSize: '0.8rem',
                  padding: '0.3rem 0.8rem',
                  borderRadius: '6px',
                  backgroundColor: isRestarting
                    ? 'rgba(239, 68, 68, 0.2)'
                    : isHealthy
                    ? 'rgba(34, 197, 94, 0.15)'
                    : 'rgba(148, 163, 184, 0.15)',
                  color: isRestarting ? 'var(--accent-red)' : isHealthy ? 'var(--accent-green)' : 'var(--text-muted)',
                  border: '1px solid currentColor',
                  fontWeight: 'bold',
                }}
              >
                {isRestarting ? '⚡ REBOOTING...' : isHealthy ? '● HEALTHY & ACTIVE' : '⚪ STANDBY'}
              </span>

              {agentInstance && (
                <button
                  className="button-dark"
                  onClick={handleReboot}
                  style={{
                    backgroundColor: 'rgba(239, 68, 68, 0.15)',
                    border: '1px solid var(--accent-red)',
                    color: 'var(--accent-red)',
                    fontSize: '0.8rem',
                    padding: '0.4rem 0.8rem',
                  }}
                >
                  ⚡ REBOOT
                </button>
              )}

              <button
                className="button-dark"
                onClick={onClose}
                style={{ padding: '0.4rem 0.8rem', fontSize: '1rem', cursor: 'pointer' }}
              >
                ✕
              </button>
            </div>
          </header>

          {/* Navigation Tabs */}
          <div
            style={{
              display: 'flex',
              borderBottom: '1px solid var(--bg-border)',
              backgroundColor: 'var(--bg-card)',
            }}
          >
            <button
              className={`tab-btn ${activeTab === 'overview' ? 'active' : ''}`}
              onClick={() => setActiveTab('overview')}
              style={{
                flex: 1,
                padding: '0.8rem',
                background: activeTab === 'overview' ? 'var(--bg-elevated)' : 'transparent',
                border: 'none',
                borderBottom: activeTab === 'overview' ? '2px solid var(--accent-blue)' : '2px solid transparent',
                color: activeTab === 'overview' ? 'var(--text-primary)' : 'var(--text-muted)',
                fontWeight: 'bold',
                fontSize: '0.85rem',
                cursor: 'pointer',
              }}
            >
              📋 PURPOSE & METRICS
            </button>
            <button
              className={`tab-btn ${activeTab === 'workflow' ? 'active' : ''}`}
              onClick={() => setActiveTab('workflow')}
              style={{
                flex: 1,
                padding: '0.8rem',
                background: activeTab === 'workflow' ? 'var(--bg-elevated)' : 'transparent',
                border: 'none',
                borderBottom: activeTab === 'workflow' ? '2px solid var(--accent-blue)' : '2px solid transparent',
                color: activeTab === 'workflow' ? 'var(--text-primary)' : 'var(--text-muted)',
                fontWeight: 'bold',
                fontSize: '0.85rem',
                cursor: 'pointer',
              }}
            >
              ⚙️ WORKFLOW & TOPICS
            </button>
            <button
              className={`tab-btn ${activeTab === 'formulas' ? 'active' : ''}`}
              onClick={() => setActiveTab('formulas')}
              style={{
                flex: 1,
                padding: '0.8rem',
                background: activeTab === 'formulas' ? 'var(--bg-elevated)' : 'transparent',
                border: 'none',
                borderBottom: activeTab === 'formulas' ? '2px solid var(--accent-blue)' : '2px solid transparent',
                color: activeTab === 'formulas' ? 'var(--text-primary)' : 'var(--text-muted)',
                fontWeight: 'bold',
                fontSize: '0.85rem',
                cursor: 'pointer',
              }}
            >
              🧮 FORMULAS & MATH
            </button>
            <button
              className={`tab-btn ${activeTab === 'data' ? 'active' : ''}`}
              onClick={() => setActiveTab('data')}
              style={{
                flex: 1,
                padding: '0.8rem',
                background: activeTab === 'data' ? 'var(--bg-elevated)' : 'transparent',
                border: 'none',
                borderBottom: activeTab === 'data' ? '2px solid var(--accent-blue)' : '2px solid transparent',
                color: activeTab === 'data' ? 'var(--text-primary)' : 'var(--text-muted)',
                fontWeight: 'bold',
                fontSize: '0.85rem',
                cursor: 'pointer',
              }}
            >
              📊 RECENT EVENTS & DB
            </button>
          </div>

          {/* Modal Body */}
          <div style={{ padding: '1.5rem', overflowY: 'auto', flex: 1, display: 'flex', flexDirection: 'column', gap: '1.2rem' }}>
            {activeTab === 'overview' && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1.2rem' }}>
                <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '1.2rem', borderRadius: '8px', border: '1px solid var(--bg-border)' }}>
                  <h4 style={{ margin: '0 0 0.6rem 0', color: 'var(--accent-blue)', fontSize: '0.9rem' }}>AGENT PURPOSE & ROLE</h4>
                  <p style={{ margin: 0, lineHeight: 1.6, fontSize: '0.95rem' }}>{knowledge.purpose}</p>
                </div>

                {agentInstance && (
                  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '1rem' }}>
                    <div className="surface-card" style={{ padding: '1rem' }}>
                      <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', marginBottom: '0.4rem' }}>CPU UTILIZATION</div>
                      <div className="mono" style={{ fontSize: '1.3rem', fontWeight: 'bold', color: 'var(--accent-cyan)' }}>
                        {agentInstance.metrics?.cpu_percent ?? 0.0}%
                      </div>
                    </div>
                    <div className="surface-card" style={{ padding: '1rem' }}>
                      <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', marginBottom: '0.4rem' }}>MEMORY USAGE</div>
                      <div className="mono" style={{ fontSize: '1.3rem', fontWeight: 'bold', color: 'var(--accent-blue)' }}>
                        {agentInstance.metrics?.memory_percent ?? 0.0}%
                      </div>
                    </div>
                    <div className="surface-card" style={{ padding: '1rem' }}>
                      <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', marginBottom: '0.4rem' }}>QUEUE DEPTH</div>
                      <div className="mono" style={{ fontSize: '1.3rem', fontWeight: 'bold', color: 'var(--accent-amber)' }}>
                        {agentInstance.metrics?.queue_depth ?? 0}
                      </div>
                    </div>
                    <div className="surface-card" style={{ padding: '1rem' }}>
                      <div style={{ color: 'var(--text-muted)', fontSize: '0.75rem', marginBottom: '0.4rem' }}>RESTART COUNT</div>
                      <div className="mono" style={{ fontSize: '1.3rem', fontWeight: 'bold', color: 'var(--accent-green)' }}>
                        {agentInstance.restart_count ?? 0}
                      </div>
                    </div>
                  </div>
                )}

                <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '1.2rem', borderRadius: '8px', border: '1px solid var(--bg-border)' }}>
                  <h4 style={{ margin: '0 0 0.8rem 0', color: 'var(--text-secondary)', fontSize: '0.9rem' }}>DATABASE TABLES MODIFIED / QUERIED</h4>
                  {knowledge.databaseTables.length > 0 ? (
                    <div style={{ display: 'flex', gap: '0.6rem', flexWrap: 'wrap' }}>
                      {knowledge.databaseTables.map((tbl) => (
                        <span
                          key={tbl}
                          className="mono"
                          style={{
                            backgroundColor: 'rgba(59, 130, 246, 0.12)',
                            color: 'var(--accent-blue)',
                            border: '1px solid var(--accent-blue)',
                            padding: '0.3rem 0.8rem',
                            borderRadius: '6px',
                            fontSize: '0.85rem',
                          }}
                        >
                          🗄️ {tbl}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <p style={{ margin: 0, color: 'var(--text-muted)', fontSize: '0.85rem' }}>Stateless agent — does not modify database directly.</p>
                  )}
                </div>
              </div>
            )}

            {activeTab === 'workflow' && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1.2rem' }}>
                <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '1.2rem', borderRadius: '8px', border: '1px solid var(--bg-border)' }}>
                  <h4 style={{ margin: '0 0 0.8rem 0', color: 'var(--accent-blue)', fontSize: '0.9rem' }}>EXECUTION WORKFLOW</h4>
                  <ol style={{ margin: 0, paddingLeft: '1.2rem', display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
                    {knowledge.workflow.map((step, idx) => (
                      <li key={idx} style={{ lineHeight: 1.5, fontSize: '0.9rem' }}>
                        {step}
                      </li>
                    ))}
                  </ol>
                </div>

                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
                  <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '1.2rem', borderRadius: '8px', border: '1px solid var(--bg-border)' }}>
                    <h4 style={{ margin: '0 0 0.8rem 0', color: 'var(--accent-amber)', fontSize: '0.85rem' }}>CONSUMED TOPICS & EVENTS</h4>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
                      {knowledge.consumedEvents.map((evt) => (
                        <span key={evt} className="mono" style={{ fontSize: '0.8rem', color: 'var(--accent-amber)' }}>
                          📥 {evt}
                        </span>
                      ))}
                    </div>
                  </div>

                  <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '1.2rem', borderRadius: '8px', border: '1px solid var(--bg-border)' }}>
                    <h4 style={{ margin: '0 0 0.8rem 0', color: 'var(--accent-green)', fontSize: '0.85rem' }}>EMITTED TOPICS & EVENTS</h4>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
                      {knowledge.emittedEvents.length > 0 ? (
                        knowledge.emittedEvents.map((evt) => (
                          <span key={evt} className="mono" style={{ fontSize: '0.8rem', color: 'var(--accent-green)' }}>
                            📤 {evt}
                          </span>
                        ))
                      ) : (
                        <span style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>None (Sink / Consumer only)</span>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            )}

            {activeTab === 'formulas' && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1.2rem' }}>
                {knowledge.formulas.length > 0 ? (
                  knowledge.formulas.map((f, idx) => (
                    <div
                      key={idx}
                      style={{
                        backgroundColor: 'var(--bg-elevated)',
                        padding: '1.2rem',
                        borderRadius: '8px',
                        border: '1px solid var(--accent-blue)',
                      }}
                    >
                      <h4 style={{ margin: '0 0 0.6rem 0', color: 'var(--accent-cyan)', fontSize: '0.95rem' }}>{f.title}</h4>
                      <div
                        className="mono"
                        style={{
                          backgroundColor: '#0d1117',
                          padding: '1rem',
                          borderRadius: '6px',
                          border: '1px solid var(--bg-border)',
                          color: '#58a6ff',
                          fontSize: '0.95rem',
                          textAlign: 'center',
                          marginBottom: '0.8rem',
                          overflowX: 'auto',
                        }}
                      >
                        {f.latex}
                      </div>
                      <p style={{ margin: 0, color: 'var(--text-muted)', fontSize: '0.85rem', lineHeight: 1.5 }}>
                        {f.description}
                      </p>
                    </div>
                  ))
                ) : (
                  <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '2rem', borderRadius: '8px', textAlign: 'center', color: 'var(--text-muted)' }}>
                    No specific mathematical formula defined for this agent type.
                  </div>
                )}
              </div>
            )}

            {activeTab === 'data' && (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '1.2rem' }}>
                <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '1.2rem', borderRadius: '8px', border: '1px solid var(--bg-border)' }}>
                  <h4 style={{ margin: '0 0 0.8rem 0', color: 'var(--accent-blue)', fontSize: '0.9rem' }}>
                    RECENT EVENT STREAM ({agentEvents.length})
                  </h4>
                  {agentEvents.length > 0 ? (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem', maxHeight: '250px', overflowY: 'auto' }}>
                      {agentEvents.map((evt) => (
                        <div
                          key={evt.event_id}
                          className="mono"
                          style={{
                            display: 'flex',
                            justifyContent: 'space-between',
                            padding: '0.4rem 0.8rem',
                            backgroundColor: 'var(--bg-card)',
                            borderRadius: '4px',
                            fontSize: '0.8rem',
                          }}
                        >
                          <span style={{ color: 'var(--accent-blue)' }}>{evt.event_type}</span>
                          <span style={{ color: 'var(--text-muted)' }}>{evt.entity_id}</span>
                          <span style={{ color: 'var(--text-secondary)' }}>{evt.event_time.slice(11, 19)}</span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p style={{ margin: 0, color: 'var(--text-muted)', fontSize: '0.85rem' }}>No recent events published by this agent in the active window.</p>
                  )}
                </div>
              </div>
            )}
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  )
}
