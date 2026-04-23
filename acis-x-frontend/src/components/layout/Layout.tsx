import { useEffect, useState } from 'react'
import { Outlet } from 'react-router-dom'
import { Sidebar } from './Sidebar'

export function Layout() {
  const [collapsed, setCollapsed] = useState(false)

  useEffect(() => {
    const query = window.matchMedia('(max-width: 1279px)')
    const sync = () => setCollapsed(query.matches)
    sync()
    query.addEventListener('change', sync)
    return () => query.removeEventListener('change', sync)
  }, [])

  return (
    <div className={`app-shell${collapsed ? ' sidebar-collapsed' : ''}`}>
      <Sidebar collapsed={collapsed} onToggle={() => setCollapsed((c) => !c)} />
      <main className="main-content">
        <Outlet />
      </main>
    </div>
  )
}
