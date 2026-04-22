import {
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
} from 'recharts'
import type { DashboardSummary } from '../../types/api'
import type { Invoice, Payment } from '../../types/ledger'
import { formatCurrency } from '../../lib/utils'
import { AnimatedCounter } from '../ui/AnimatedCounter'

type LedgerKPIStripProps = {
  summary?: DashboardSummary
  invoices: Invoice[]
  payments: Payment[]
}

export function LedgerKPIStrip({ summary, invoices, payments }: LedgerKPIStripProps) {
  const openInvoices = invoices.filter((invoice) => invoice.status !== 'paid')
  const outstanding = openInvoices.reduce(
    (sum, invoice) => sum + (invoice.total_amount - invoice.paid_amount),
    0,
  )
  const overdueInvoices = invoices.filter((invoice) => invoice.status === 'overdue')
  const overdueAmount = overdueInvoices.reduce(
    (sum, invoice) => sum + (invoice.total_amount - invoice.paid_amount),
    0,
  )
  const today = new Date().toDateString()
  const todayPayments = payments.filter(
    (payment) => new Date(payment.payment_date).toDateString() === today,
  )
  const paymentTodayAmount = todayPayments.reduce((sum, payment) => sum + payment.amount, 0)

  const ratio = summary?.on_time_ratio ?? 0
  const donutData = [
    { name: 'On-Time', value: ratio },
    { name: 'Late', value: Math.max(0, 1 - ratio) },
  ]

  return (
    <section className="ledger-kpi-strip">
      <article className="surface-card kpi-card">
        <h4>Total Outstanding</h4>
        <p className="kpi-value numeric">
          <AnimatedCounter value={outstanding} formatter={(value) => formatCurrency(value)} />
        </p>
        <small>{openInvoices.length} open invoices</small>
      </article>

      <article className="surface-card kpi-card">
        <h4>Overdue Amount</h4>
        <p className={overdueAmount > 0 ? 'kpi-value kpi-danger numeric' : 'kpi-value numeric'}>
          <AnimatedCounter value={overdueAmount} formatter={(value) => formatCurrency(value)} />
        </p>
        <small>{overdueInvoices.length} overdue invoices</small>
      </article>

      <article className="surface-card kpi-card">
        <h4>Payments Received Today</h4>
        <p className="kpi-value kpi-success numeric">
          <AnimatedCounter value={paymentTodayAmount} formatter={(value) => formatCurrency(value)} />
        </p>
        <small>{todayPayments.length} transactions</small>
      </article>

      <article className="surface-card kpi-card kpi-donut">
        <h4>On-Time Payment Ratio</h4>
        <div className="donut-wrap">
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={donutData}
                cx="50%"
                cy="50%"
                innerRadius={18}
                outerRadius={28}
                dataKey="value"
                stroke="none"
              >
                <Cell fill="var(--accent-green)" />
                <Cell fill="var(--accent-red)" />
              </Pie>
            </PieChart>
          </ResponsiveContainer>
          <span className="numeric">{Math.round(ratio * 100)}%</span>
        </div>
      </article>
    </section>
  )
}
