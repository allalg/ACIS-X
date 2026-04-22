import { useState } from 'react'
import { useParams } from 'react-router-dom'
import { useCustomerProfile } from '../../../hooks/useCustomerProfile'
import { SeverityBadge } from '../../ui/SeverityBadge'
import { RiskBreakdownBar } from './RiskBreakdownBar'
import { RiskHistoryChart } from './RiskHistoryChart'

const tabs = ['overview', 'invoices', 'payments', 'risk history', 'collections'] as const

export function CustomerProfilePage() {
  const { id = '' } = useParams()
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('overview')
  const { data } = useCustomerProfile(id)

  if (!data) {
    return <section className="surface-card customer-profile-page">Loading customer profile...</section>
  }

  return (
    <section className="customer-profile-page">
      <header className="surface-card customer-profile-header">
        <div>
          <h2>{data.name}</h2>
          <p className="mono">{data.customer_id}</p>
        </div>
        <SeverityBadge severity={data.severity} />
      </header>

      <nav className="tabs-row surface-card">
        {tabs.map((tab) => (
          <button
            key={tab}
            className={activeTab === tab ? 'button-dark active' : 'button-dark'}
            onClick={() => setActiveTab(tab)}
          >
            {tab}
          </button>
        ))}
      </nav>

      <section className="surface-card customer-profile-content">
        {activeTab === 'overview' ? (
          <div className="profile-overview-grid">
            <div>
              <p>Total Outstanding</p>
              <h3 className="numeric">{Math.round(data.total_outstanding).toLocaleString()}</h3>
              <p>Avg Delay: {data.avg_delay.toFixed(1)}d</p>
              <p>On-Time Ratio: {Math.round(data.on_time_ratio * 100)}%</p>
              <p>Last Payment: {new Date(data.last_payment_date).toLocaleDateString()}</p>
            </div>
            <div>
              <p>Financial Risk: {Math.round(data.financial_risk * 100)}%</p>
              <p>Litigation Risk: {Math.round(data.litigation_risk * 100)}%</p>
              <p>Combined Risk: {Math.round(data.combined_risk * 100)}%</p>
              <p>Confidence: {Math.round(data.confidence * 100)}%</p>
              <RiskBreakdownBar financial={data.financial_risk} litigation={data.litigation_risk} />
            </div>
          </div>
        ) : null}

        {activeTab === 'risk history' ? <RiskHistoryChart currentRisk={data.combined_risk} /> : null}

        {activeTab !== 'overview' && activeTab !== 'risk history' ? (
          <p className="page-subtitle">
            Detailed {activeTab} data is sourced from the same live endpoints and can be expanded with
            deeper filters.
          </p>
        ) : null}
      </section>
    </section>
  )
}
