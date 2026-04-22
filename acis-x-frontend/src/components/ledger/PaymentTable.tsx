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
  if (!loading && payments.length === 0) {
    return <EmptyState description="No payments have been received yet." />
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
              <th>Reference</th>
            </tr>
          </thead>
          <tbody>
            {payments.map((payment) => (
              <tr
                key={payment.payment_id}
                onMouseEnter={() => onHoverInvoiceId(payment.invoice_id)}
                onMouseLeave={() => onHoverInvoiceId(null)}
              >
                <td className="mono">{payment.payment_id}</td>
                <td>{payment.customer_name}</td>
                <td className="mono">{payment.invoice_id}</td>
                <td className="numeric">{formatCurrency(payment.amount, payment.currency)}</td>
                <td className="numeric">{new Date(payment.payment_date).toLocaleDateString()}</td>
                <td className="payment-method-cell">
                  <PaymentMethodIcon method={payment.payment_method} />
                  <span>{payment.payment_method.replace('_', ' ')}</span>
                </td>
                <td>
                  <StatusBadge status={payment.status} />
                </td>
                <td className="mono">{payment.reference}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}
