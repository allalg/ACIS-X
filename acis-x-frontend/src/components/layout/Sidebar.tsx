import { useState } from 'react'
import { SidebarNav } from './SidebarNav'
import { SystemHealthBadge } from './SystemHealthBadge'
import { ApiConfigModal } from './ApiConfigModal'

type SidebarProps = {
  collapsed: boolean
  onToggle: () => void
}

export function Sidebar({ collapsed, onToggle }: SidebarProps) {
  const [isApiConfigOpen, setIsApiConfigOpen] = useState(false)

  return (
    <aside className={collapsed ? 'sidebar collapsed' : 'sidebar'}>
      <header className="sidebar-brand">
        <strong className="mono">ACIS-X</strong>
        <span className="sidebar-label">Credit Intelligence</span>
      </header>

      <SidebarNav collapsed={collapsed} />

      <footer className="sidebar-footer" style={{ display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', width: '100%' }}>
          <SystemHealthBadge />
          <button
            className="button-dark"
            onClick={() => setIsApiConfigOpen(true)}
            title="Configure Backend API URL"
            style={{ padding: '0.2rem 0.5rem', fontSize: '0.8rem', cursor: 'pointer' }}
          >
            ⚙️ API
          </button>
          <button className="button-dark sidebar-toggle" onClick={onToggle} aria-label="Toggle sidebar">
            {collapsed ? '›' : '‹'}
          </button>
        </div>
      </footer>

      <ApiConfigModal isOpen={isApiConfigOpen} onClose={() => setIsApiConfigOpen(false)} />
    </aside>
  )
}
