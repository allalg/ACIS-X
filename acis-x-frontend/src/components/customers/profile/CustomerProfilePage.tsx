import { useMemo, useState } from 'react'
import { useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts'
import { api } from '../../../services/api'
import { useCustomerProfile } from '../../../hooks/useCustomerProfile'
import { useInvoices } from '../../../hooks/useInvoices'
import { usePayments } from '../../../hooks/usePayments'
import { InvoiceTable } from '../../ledger/InvoiceTable'
import { PaymentTable } from '../../ledger/PaymentTable'
import { SeverityBadge } from '../../ui/SeverityBadge'
import { RiskBreakdownBar } from './RiskBreakdownBar'
import { RiskHistoryChart } from './RiskHistoryChart'

import { formatDate, formatDateTime } from '../../../lib/utils'

const tabs = ['overview', 'invoices', 'payments', 'risk history', 'collections'] as const

function ARAgingBuckets({ data }: { data: any }) {
  const current = data.aging_current || 0
  const aging1_30 = data.aging_1_30 || 0
  const aging31_60 = data.aging_31_60 || 0
  const aging61_90 = data.aging_61_90 || 0
  const aging90Plus = data.aging_90_plus || 0

  const total = current + aging1_30 + aging31_60 + aging61_90 + aging90Plus || 1

  const pctCurrent = (current / total) * 100
  const pct1_30 = (aging1_30 / total) * 100
  const pct31_60 = (aging31_60 / total) * 100
  const pct61_90 = (aging61_90 / total) * 100
  const pct90Plus = (aging90Plus / total) * 100

  return (
    <div style={{ marginTop: '1.5rem', width: '100%' }}>
      <h4 style={{ color: 'var(--text-primary)', marginBottom: '0.8rem', fontSize: '0.9rem' }}>ACCOUNTS RECEIVABLE AGING</h4>
      <div style={{ display: 'flex', height: '18px', width: '100%', borderRadius: '4px', overflow: 'hidden', backgroundColor: 'var(--bg-border)' }}>
        {current > 0 && <div style={{ width: `${pctCurrent}%`, backgroundColor: 'var(--accent-green)' }} title={`Current: ₹${current.toLocaleString()}`} />}
        {aging1_30 > 0 && <div style={{ width: `${pct1_30}%`, backgroundColor: '#3b82f6' }} title={`1-30 Days: ₹${aging1_30.toLocaleString()}`} />}
        {aging31_60 > 0 && <div style={{ width: `${pct31_60}%`, backgroundColor: '#f59e0b' }} title={`31-60 Days: ₹${aging31_60.toLocaleString()}`} />}
        {aging61_90 > 0 && <div style={{ width: `${pct61_90}%`, backgroundColor: '#e11d48' }} title={`61-90 Days: ₹${aging61_90.toLocaleString()}`} />}
        {aging90Plus > 0 && <div style={{ width: `${pct90Plus}%`, backgroundColor: '#7f1d1d' }} title={`90+ Days: ₹${aging90Plus.toLocaleString()}`} />}
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.8rem', marginTop: '0.6rem', fontSize: '0.75rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
          <span style={{ display: 'inline-block', width: '8px', height: '8px', borderRadius: '50%', backgroundColor: 'var(--accent-green)' }} />
          <span className="text-secondary">Current (₹{Math.round(current).toLocaleString()})</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
          <span style={{ display: 'inline-block', width: '8px', height: '8px', borderRadius: '50%', backgroundColor: '#3b82f6' }} />
          <span className="text-secondary">1-30d (₹{Math.round(aging1_30).toLocaleString()})</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
          <span style={{ display: 'inline-block', width: '8px', height: '8px', borderRadius: '50%', backgroundColor: '#f59e0b' }} />
          <span className="text-secondary">31-60d (₹{Math.round(aging31_60).toLocaleString()})</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
          <span style={{ display: 'inline-block', width: '8px', height: '8px', borderRadius: '50%', backgroundColor: '#e11d48' }} />
          <span className="text-secondary">61-90d (₹{Math.round(aging61_90).toLocaleString()})</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.3rem' }}>
          <span style={{ display: 'inline-block', width: '8px', height: '8px', borderRadius: '50%', backgroundColor: '#7f1d1d' }} />
          <span className="text-secondary">90d+ (₹{Math.round(aging90Plus).toLocaleString()})</span>
        </div>
      </div>
    </div>
  )
}

function RiskExplanationPanel({ explanation, shapData }: { explanation: any; shapData: any[] }) {
  if (!explanation) {
    return <p className="text-secondary">No AI model insights available for this customer.</p>
  }

  let parsedReasons = []
  if (explanation.reasons) {
    try {
      parsedReasons = JSON.parse(explanation.reasons)
    } catch {
      parsedReasons = [explanation.reasons]
    }
  }

  return (
    <div style={{ marginTop: '1.5rem', width: '100%' }}>
      <h4 style={{ color: 'var(--text-primary)', marginBottom: '0.5rem', fontSize: '0.9rem' }}>MODEL EXPLAINER (SHAP VALUES)</h4>
      <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginBottom: '1.2rem' }}>
        RF prediction risk level: <span className="mono" style={{ color: explanation.risk_level === 'high' || explanation.risk_level === 'critical' ? 'var(--risk-high)' : 'var(--risk-low)' }}>{explanation.risk_level?.toUpperCase()}</span> (score: {Math.round(explanation.risk_score * 100)}%)
      </p>

      {shapData.length > 0 && (
        <div style={{ width: '100%', height: '220px', backgroundColor: 'var(--bg-elevated)', borderRadius: '8px', padding: '0.8rem' }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={shapData}
              layout="vertical"
              margin={{ top: 5, right: 20, left: 10, bottom: 5 }}
            >
              <XAxis type="number" stroke="var(--text-secondary)" fontSize={10} />
              <YAxis dataKey="name" type="category" stroke="var(--text-secondary)" fontSize={9} width={130} />
              <Tooltip
                contentStyle={{ backgroundColor: 'var(--bg-surface)', borderColor: 'var(--bg-border)', color: 'var(--text-primary)' }}
                formatter={(value: any) => [Number(value || 0).toFixed(4), 'Weight']}
              />
              <Bar dataKey="weight">
                {shapData.map((entry, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={entry.weight >= 0 ? 'rgba(239, 68, 68, 0.75)' : 'rgba(34, 197, 94, 0.75)'}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {parsedReasons.length > 0 && (
        <div style={{ marginTop: '1.2rem' }}>
          <strong style={{ color: 'var(--text-primary)', fontSize: '0.85rem' }}>Top Risk Drivers:</strong>
          <ul style={{ paddingLeft: '1.2rem', marginTop: '0.5rem', fontSize: '0.85rem', color: 'var(--text-secondary)', display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
            {parsedReasons.map((reason: string, idx: number) => (
              <li key={idx}>{reason}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}

function ExternalIntelligencePanel({ intel }: { intel: any }) {
  if (!intel) return null
  return (
    <div style={{ marginTop: '2rem', padding: '1.2rem', borderTop: '1px solid var(--bg-border)' }}>
      <h4 style={{ color: 'var(--text-primary)', marginBottom: '0.8rem', fontSize: '0.9rem' }}>EXTERNAL LITIGATION & MARKET INTELLIGENCE</h4>
      <div style={{ display: 'flex', gap: '2rem', flexWrap: 'wrap' }}>
        <div style={{ minWidth: '180px' }}>
          <p className="text-secondary" style={{ fontSize: '0.85rem' }}>NCLT Litigation Risk</p>
          <h3 className="mono" style={{ color: intel.litigation_risk > 0.4 ? 'var(--risk-high)' : 'var(--risk-low)', margin: '0.4rem 0' }}>
            {Math.round(intel.litigation_risk * 100)}% ({intel.severity?.toUpperCase() || 'LOW'})
          </h3>
          <p className="text-muted" style={{ fontSize: '0.8rem' }}>
            Case Count: <span style={{ color: 'var(--text-primary)' }}>{intel.case_count || 0}</span>
          </p>
          {intel.case_types && (
            <p className="text-muted" style={{ fontSize: '0.8rem', marginTop: '0.2rem' }}>
              Types: <span style={{ color: 'var(--text-secondary)' }}>{intel.case_types}</span>
            </p>
          )}
        </div>
        {intel.cases && (
          <div style={{ flex: 1, minWidth: '240px' }}>
            <p className="text-secondary" style={{ fontSize: '0.85rem' }}>Filings Evidence Summaries</p>
            <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginTop: '0.3rem', lineHeight: '1.5' }}>
              {intel.cases}
            </p>
            {intel.evidence && (
              <p className="text-muted" style={{ fontSize: '0.8rem', marginTop: '0.4rem', borderTop: '1px solid var(--bg-border)', paddingTop: '0.4rem' }}>
                Evidence Source: <span className="mono">{intel.evidence}</span> (Source: {intel.litigation_source})
              </p>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function FinancialRiskExplanationPanel({ data }: { data: any }) {
  const financialPct = Math.round((data.financial_risk || 0) * 100)
  const delayDays = data.avg_delay || 0
  const onTimePct = Math.round((data.on_time_ratio || 0) * 100)
  const overdueCount = data.overdue_count || 0
  const limit = data.credit_limit || 1
  const outstanding = data.total_outstanding || 0
  const utilPct = Math.round((outstanding / limit) * 100)

  let summaryRationale = ''
  if (financialPct >= 80) {
    summaryRationale = `High financial risk score (${financialPct}%) driven by severe payment delinquency: ${onTimePct}% on-time payment ratio, ${overdueCount} overdue invoices averaging ${delayDays.toFixed(1)} days late, and ${utilPct}% credit limit utilization.`
  } else if (financialPct >= 40) {
    summaryRationale = `Moderate financial risk score (${financialPct}%) with average delay of ${delayDays.toFixed(1)} days and ${onTimePct}% on-time ratio against total outstanding ₹${Math.round(outstanding).toLocaleString()}.`
  } else {
    summaryRationale = `Low financial risk score (${financialPct}%) with strong payment performance (${onTimePct}% on-time ratio, average delay ${delayDays.toFixed(1)} days).`
  }

  return (
    <div style={{ marginTop: '1.5rem', width: '100%', borderTop: '1px solid var(--bg-border)', paddingTop: '1.2rem' }}>
      <h4 style={{ color: 'var(--text-primary)', marginBottom: '0.6rem', fontSize: '0.9rem' }}>FINANCIAL RISK EXPLANATION</h4>
      <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginBottom: '0.8rem', lineHeight: '1.4' }}>
        {summaryRationale}
      </p>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', gap: '0.8rem', fontSize: '0.8rem' }}>
        <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '0.6rem 0.8rem', borderRadius: '6px' }}>
          <span className="text-secondary" style={{ display: 'block', fontSize: '0.75rem' }}>Payment Delay Impact</span>
          <strong className="numeric" style={{ color: delayDays > 15 ? 'var(--risk-high)' : 'var(--accent-green)', fontSize: '0.95rem' }}>
            {delayDays.toFixed(1)} Days
          </strong>
        </div>
        <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '0.6rem 0.8rem', borderRadius: '6px' }}>
          <span className="text-secondary" style={{ display: 'block', fontSize: '0.75rem' }}>On-Time Ratio</span>
          <strong className="numeric" style={{ color: onTimePct < 50 ? 'var(--risk-high)' : 'var(--accent-green)', fontSize: '0.95rem' }}>
            {onTimePct}%
          </strong>
        </div>
        <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '0.6rem 0.8rem', borderRadius: '6px' }}>
          <span className="text-secondary" style={{ display: 'block', fontSize: '0.75rem' }}>Overdue Invoices</span>
          <strong className="numeric" style={{ color: overdueCount > 0 ? 'var(--risk-high)' : 'var(--accent-green)', fontSize: '0.95rem' }}>
            {overdueCount} Invoices
          </strong>
        </div>
        <div style={{ backgroundColor: 'var(--bg-elevated)', padding: '0.6rem 0.8rem', borderRadius: '6px' }}>
          <span className="text-secondary" style={{ display: 'block', fontSize: '0.75rem' }}>Credit Limit Used</span>
          <strong className="numeric" style={{ color: utilPct > 70 ? 'var(--risk-high)' : 'var(--accent-blue)', fontSize: '0.95rem' }}>
            {utilPct}% (₹{Math.round(outstanding).toLocaleString()})
          </strong>
        </div>
      </div>
    </div>
  )
}

function ScoreStageExplanationPanel({ data }: { data: any }) {
  const pending = data.severity === 'pending' || data.enrichment_status === 'pending'
  const finRisk = data.financial_risk ?? 0
  const litRisk = data.litigation_risk ?? 0
  const combinedPct = data.combined_risk == null ? null : Math.round(data.combined_risk * 100)
  const severity = (data.severity || 'pending').toUpperCase()

  let stageRationale = ''
  if (pending) {
    stageRationale =
      'Enrichment pending — external financial/litigation signals and risk fusion have not landed yet for this customer (eventual consistency in the async pipeline).'
  } else if (severity === 'CRITICAL') {
    stageRationale = `Assigned to CRITICAL STAGE because Combined Risk (${combinedPct}%) exceeds the 85% critical threshold. Urgent automated collection action & credit hold enforced.`
  } else if (severity === 'HIGH') {
    stageRationale = `Assigned to HIGH STAGE because Combined Risk (${combinedPct}%) reached the 70%-85% high risk window. Escalated collection reminders and credit limit freeze active.`
  } else if (severity === 'MEDIUM') {
    stageRationale = `Assigned to MEDIUM STAGE because Combined Risk (${combinedPct}%) is between 40% and 70%. Soft collection reminders and payment monitoring active.`
  } else {
    stageRationale = `Assigned to LOW STAGE because Combined Risk (${combinedPct}%) is below 40%. Standard payment terms apply.`
  }

  return (
    <div style={{ marginTop: '1.5rem', width: '100%', borderTop: '1px solid var(--bg-border)', paddingTop: '1.2rem' }}>
      <h4 style={{ color: 'var(--text-primary)', marginBottom: '0.6rem', fontSize: '0.9rem' }}>RISK SCORE FUSION & STAGE DECISION</h4>
      <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginBottom: '0.6rem', lineHeight: '1.4' }}>
        {stageRationale}
      </p>
      <div className="mono" style={{ fontSize: '0.78rem', color: 'var(--text-muted)', backgroundColor: 'var(--bg-elevated)', padding: '0.6rem 0.8rem', borderRadius: '6px' }}>
        {pending ? (
          <>Formula: awaiting risk.profile.updated / external enrichment events</>
        ) : (
          <>
            Formula: Combined Risk = (0.6 × Financial Risk {Math.round(finRisk * 100)}%) + (0.4 × Litigation Risk {Math.round(litRisk * 100)}%) ={' '}
            <span style={{ color: 'var(--text-primary)', fontWeight: 'bold' }}>{combinedPct}%</span> ({severity} STAGE)
          </>
        )}
      </div>
    </div>
  )
}

export function CustomerProfilePage() {
  const { id = '' } = useParams()
  const [activeTab, setActiveTab] = useState<(typeof tabs)[number]>('overview')
  const { data } = useCustomerProfile(id)

  const { data: invoicesData, isLoading: invoicesLoading } = useInvoices(id)
  const { data: paymentsData, isLoading: paymentsLoading } = usePayments(id)
  const { data: collections, isLoading: collectionsLoading } = useQuery({
    queryKey: ['customer-collections', id],
    queryFn: () => api.getCustomerCollections(id),
    enabled: Boolean(id),
  })

  const { data: riskExplanation } = useQuery({
    queryKey: ['customer-explanation', id],
    queryFn: () => api.getCustomerRiskExplanation(id),
    enabled: Boolean(id),
  })

  const { data: intel } = useQuery({
    queryKey: ['customer-intelligence', id],
    queryFn: () => api.getCustomerExternalIntelligence(id),
    enabled: Boolean(id),
  })

  const shapData = useMemo(() => {
    if (!riskExplanation?.shap_values) return []
    try {
      const parsed = JSON.parse(riskExplanation.shap_values)
      return Object.entries(parsed).map(([key, value]) => ({
        name: (key || '').replace(/_/g, ' ').toUpperCase(),
        weight: value as number,
      })).sort((a, b) => b.weight - a.weight)
    } catch {
      return []
    }
  }, [riskExplanation])

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
          <div>
            <div className="profile-overview-grid">
              <div>
                <p>Total Outstanding</p>
                <h3 className="numeric">₹{Math.round(data.total_outstanding).toLocaleString()}</h3>
                <p>Avg Delay: {data.avg_delay.toFixed(1)}d</p>
                <p>On-Time Ratio: {Math.round(data.on_time_ratio * 100)}%</p>
                <p>Last Payment: {formatDate(data.last_payment_date)}</p>
                <ARAgingBuckets data={data} />
                <FinancialRiskExplanationPanel data={data} />
              </div>
              <div>
                <p>
                  Financial Risk:{' '}
                  {data.financial_risk == null ? 'pending' : `${Math.round(data.financial_risk * 100)}%`}
                </p>
                <p>
                  Litigation Risk:{' '}
                  {data.litigation_risk == null ? 'pending' : `${Math.round(data.litigation_risk * 100)}%`}
                </p>
                <p>
                  Combined Risk:{' '}
                  {data.combined_risk == null ? 'pending' : `${Math.round(data.combined_risk * 100)}%`}
                </p>
                <p>
                  Confidence:{' '}
                  {data.confidence == null ? 'pending' : `${Math.round(data.confidence * 100)}%`}
                </p>
                <RiskBreakdownBar
                  financial={data.financial_risk ?? 0}
                  litigation={data.litigation_risk ?? 0}
                />
                <ScoreStageExplanationPanel data={data} />
                <RiskExplanationPanel explanation={riskExplanation} shapData={shapData} />
              </div>
            </div>
            <ExternalIntelligencePanel intel={intel} />
          </div>
        ) : null}

        {activeTab === 'risk history' ? (
          <RiskHistoryChart currentRisk={data.combined_risk ?? 0} />
        ) : null}

        {activeTab === 'invoices' ? (
          <InvoiceTable
            invoices={invoicesData?.invoices ?? []}
            loading={invoicesLoading}
            onHoverInvoiceId={() => {}}
            highlightedInvoiceId={null}
          />
        ) : null}

        {activeTab === 'payments' ? (
          <PaymentTable
            payments={paymentsData?.payments ?? []}
            loading={paymentsLoading}
            onHoverInvoiceId={() => {}}
          />
        ) : null}

        {activeTab === 'collections' ? (
          <div className="collections-timeline-container" style={{ width: '100%' }}>
            <h3 style={{ color: 'var(--text-primary)', marginBottom: '1.2rem' }}>COLLECTIONS ACTION LOG</h3>
            {collectionsLoading ? (
              <p className="text-secondary">Loading collections timeline...</p>
            ) : !collections || collections.length === 0 ? (
              <p className="text-secondary" style={{ marginTop: '1rem' }}>
                No collections actions have been taken for this customer yet.
              </p>
            ) : (
              <div className="collections-timeline" style={{ display: 'flex', flexDirection: 'column', gap: '1.2rem', marginTop: '1rem' }}>
                {collections.map((col) => (
                  <div
                    key={col.id}
                    className="timeline-item"
                    style={{
                      borderLeft: '4px solid var(--agent-collections)',
                      backgroundColor: 'var(--bg-elevated)',
                      padding: '1rem 1.2rem',
                      borderRadius: '0 8px 8px 0',
                    }}
                  >
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.4rem', flexWrap: 'wrap', gap: '0.5rem' }}>
                      <strong className="mono" style={{ color: 'var(--text-primary)', fontSize: '1rem' }}>
                        {col.action.toUpperCase()}
                      </strong>
                      <span className="text-secondary mono" style={{ fontSize: '0.85rem' }}>
                        {formatDateTime(col.timestamp)}
                      </span>
                    </div>
                    <p style={{ margin: '0.4rem 0', color: 'var(--text-secondary)', fontSize: '0.95rem', lineHeight: '1.4' }}>{col.reason}</p>
                    <div style={{ display: 'flex', gap: '1.2rem', fontSize: '0.85rem', color: 'var(--text-muted)', marginTop: '0.4rem', flexWrap: 'wrap' }}>
                      <span>Stage: <span className="mono" style={{ color: 'var(--text-secondary)' }}>{col.stage}</span></span>
                      <span>Priority: <span className="mono" style={{ color: 'var(--text-secondary)' }}>{col.priority}</span></span>
                      {col.invoice_id && (
                        <span>Invoice: <span className="mono" style={{ color: 'var(--text-secondary)' }}>{col.invoice_id}</span></span>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        ) : null}
      </section>
    </section>
  )
}
