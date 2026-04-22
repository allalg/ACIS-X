import { NavLink } from 'react-router-dom'
import {
  CustomersIcon,
  LedgerIcon,
  MetricsIcon,
  SimulationIcon,
} from '../ui/icons'

const navItems = [
  { to: '/simulation', label: 'Simulation', Icon: SimulationIcon },
  { to: '/ledger', label: 'Ledger', Icon: LedgerIcon },
  { to: '/metrics', label: 'Metrics', Icon: MetricsIcon },
  { to: '/customers', label: 'Customers', Icon: CustomersIcon },
]

type SidebarNavProps = {
  collapsed: boolean
}

export function SidebarNav({ collapsed }: SidebarNavProps) {
  return (
    <nav className="sidebar-nav" aria-label="Primary">
      {navItems.map(({ to, label, Icon }) => (
        <NavLink
          key={to}
          to={to}
          className={({ isActive }) => (isActive ? 'nav-item active' : 'nav-item')}
        >
          <Icon size={16} />
          {!collapsed ? <span>{label}</span> : null}
        </NavLink>
      ))}
    </nav>
  )
}
