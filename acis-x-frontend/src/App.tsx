import { lazy, Suspense } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'
import { Layout } from './components/layout/Layout'
import { CustomerProfilePage } from './components/customers/profile/CustomerProfilePage'

const SimulationPage = lazy(() => import('./pages/SimulationPage'))
const LedgerPage = lazy(() => import('./pages/LedgerPage'))
const MetricsPage = lazy(() => import('./pages/MetricsPage'))
const CustomersPage = lazy(() => import('./pages/CustomersPage'))

function App() {
  return (
    <Suspense fallback={<div className="surface-card">Loading ACIS-X modules...</div>}>
      <Routes>
        <Route path="/" element={<Navigate to="/simulation" replace />} />
        <Route element={<Layout />}>
          <Route path="/simulation" element={<SimulationPage />} />
          <Route path="/ledger" element={<LedgerPage />} />
          <Route path="/metrics" element={<MetricsPage />} />
          <Route path="/customers" element={<CustomersPage />} />
          <Route path="/customers/:id" element={<CustomerProfilePage />} />
        </Route>
      </Routes>
    </Suspense>
  )
}

export default App
