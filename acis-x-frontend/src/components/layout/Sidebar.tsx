import { SidebarNav } from './SidebarNav'
import { SystemHealthBadge } from './SystemHealthBadge'

type SidebarProps = {
  collapsed: boolean
  onToggle: () => void
}

export function Sidebar({ collapsed, onToggle }: SidebarProps) {
  return (
    <aside className={collapsed ? 'sidebar collapsed' : 'sidebar'}>
      <header className="sidebar-brand">
        <strong className="mono">ACIS-X</strong>
        <span className="sidebar-label">Credit Intelligence</span>
      </header>

      <SidebarNav collapsed={collapsed} />

      <footer className="sidebar-footer">
        <SystemHealthBadge />
        <button className="button-dark sidebar-toggle" onClick={onToggle} aria-label="Toggle sidebar">
          {collapsed ? '›' : '‹'}
        </button>
      </footer>
    </aside>
  )
}
