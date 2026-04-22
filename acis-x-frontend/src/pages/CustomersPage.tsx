import { useState } from 'react'
import { CustomerSearch } from '../components/customers/CustomerSearch'
import { CustomerTable } from '../components/customers/CustomerTable'
import { useCustomers } from '../hooks/useCustomers'

const tabs = ['all', 'low', 'medium', 'high', 'critical'] as const

export default function CustomersPage() {
  const [search, setSearch] = useState('')
  const [severity, setSeverity] = useState<(typeof tabs)[number]>('all')
  const { data } = useCustomers(search)

  return (
    <div className="customers-page">
      <header className="page-header">
        <div>
          <h1 className="page-title">CUSTOMERS</h1>
          <p className="page-subtitle">Customer profiles and risk segmentation</p>
        </div>
      </header>

      <section className="surface-card customers-toolbar">
        <CustomerSearch value={search} onChange={setSearch} />
        <div className="chip-group">
          {tabs.map((tab) => (
            <button
              key={tab}
              className={severity === tab ? 'button-dark active' : 'button-dark'}
              onClick={() => setSeverity(tab)}
            >
              {tab}
            </button>
          ))}
        </div>
      </section>

      {data ? <CustomerTable data={data} severityFilter={severity} /> : null}
    </div>
  )
}
