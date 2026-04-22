import { useEffect, useState } from 'react'
import { SidebarNav } from './SidebarNav'
import { SystemHealthBadge } from './SystemHealthBadge'

export function Sidebar() {
  const [collapsed, setCollapsed] = useState(false)

  useEffect(() => {
    const query = window.matchMedia('(max-width: 1279px)')
    const syncCollapsed = () => setCollapsed(query.matches)
    syncCollapsed()
    query.addEventListener('change', syncCollapsed)
    return () => query.removeEventListener('change', syncCollapsed)
  }, [])

  return (
    <aside className={collapsed ? 'sidebar collapsed' : 'sidebar'}>
      <header className="sidebar-brand">
        <strong className="mono">ACIS-X</strong>
        {!collapsed ? <span>Credit Intelligence</span> : null}
      </header>

      <SidebarNav collapsed={collapsed} />

      <footer className="sidebar-footer">
        {!collapsed ? <SystemHealthBadge /> : null}
        <button className="button-dark sidebar-toggle" onClick={() => setCollapsed((prev) => !prev)}>
          {collapsed ? '>' : '<'}
        </button>
      </footer>
    </aside>
  )
}
