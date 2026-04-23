import { Fragment, useState } from 'react'
import { formatCurrency } from '../../lib/utils'
import type { Payment } from '../../types/ledger'
import { EmptyState } from '../ui/EmptyState'
import { StatusBadge } from './StatusBadge'
import { PaymentMethodIcon } from './PaymentMethodIcon'

type PaymentTableProps = {
  payments: Payment[]
  loading: boolean
  onHoverInvoiceId: (invoiceId: string | null) => void
}

export function PaymentTable({ payments, loading, onHoverInvoiceId }: PaymentTableProps) {
  const [expandedId, setExpandedId] = useState<string | null>(null)

  if (!loading && payments.length === 0) {
    return <EmptyState description="No payments have been received yet." />
  }

  const toggleExpand = (id: string) => {
    setExpandedId((prev) => (prev === id ? null : id))
  }

  return (
    <section className="surface-card ledger-table-card">
      <header className="ledger-table-header">
        <h3>PAYMENTS</h3>
      </header>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Payment ID</th>
              <th>Customer</th>
              <th>Invoice</th>
              <th>Amount</th>
              <th>Date</th>
              <th>Method</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {payments.map((payment) => {
              const isExpanded = expandedId === payment.payment_id
              return (
                <Fragment key={payment.payment_id}>
                  <tr
                    key={payment.payment_id}
                    onClick={() => toggleExpand(payment.payment_id)}
                    onMouseEnter={() => onHoverInvoiceId(payment.invoice_id)}
                    onMouseLeave={() => onHoverInvoiceId(null)}
                    className="row-clickable"
                  >
                    <td className="mono">{payment.payment_id}</td>
                    <td>{payment.customer_name}</td>
                    <td className="mono">{payment.invoice_id}</td>
                    <td className="numeric">{formatCurrency(payment.amount, payment.currency)}</td>
                    <td className="numeric">{new Date(payment.payment_date).toLocaleDateString('en-IN')}</td>
                    <td className="payment-method-cell">
                      <PaymentMethodIcon method={payment.payment_method} />
                      <span>{payment.payment_method.replace('_', ' ')}</span>
                    </td>
                    <td>
                      <StatusBadge status={payment.status} />
                    </td>
                  </tr>
                  {isExpanded && (
                    <tr key={`${payment.payment_id}-detail`} className="expanded-row">
                      <td colSpan={7}>
                        <div className="expanded-detail">
                          <div className="detail-grid">
                            <div className="detail-item">
                              <span className="detail-label">Customer ID</span>
                              <span className="detail-value mono">{payment.customer_id}</span>
                            </div>
                            <div className="detail-item">
                              <span className="detail-label">Invoice ID</span>
                              <span className="detail-value mono">{payment.invoice_id}</span>
                            </div>
                            <div className="detail-item">
                              <span className="detail-label">Reference</span>
                              <span className="detail-value mono">{payment.reference || '—'}</span>
                            </div>
                            <div className="detail-item">
                              <span className="detail-label">Payment Method</span>
                              <span className="detail-value">{payment.payment_method.replace('_', ' ')}</span>
                            </div>
                            <div className="detail-item">
                              <span className="detail-label">Payment Date</span>
                              <span className="detail-value numeric">{new Date(payment.payment_date).toLocaleString('en-IN')}</span>
                            </div>
                            <div className="detail-item">
                              <span className="detail-label">Created</span>
                              <span className="detail-value numeric">{new Date(payment.created_at).toLocaleString('en-IN')}</span>
                            </div>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </Fragment>
              )
            })}
          </tbody>
        </table>
      </div>
    </section>
  )
}
