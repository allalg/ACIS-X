import { useMemo, useState } from 'react'
import { formatCurrency } from '../../lib/utils'
import type { Invoice } from '../../types/ledger'
import { EmptyState } from '../ui/EmptyState'
import { StatusBadge } from './StatusBadge'

type InvoiceTableProps = {
  invoices: Invoice[]
  loading: boolean
  onHoverInvoiceId: (invoiceId: string | null) => void
  highlightedInvoiceId: string | null
}

const filters = ['all', 'pending', 'overdue', 'paid', 'partial'] as const

export function InvoiceTable({
  invoices,
  loading,
  onHoverInvoiceId,
  highlightedInvoiceId,
}: InvoiceTableProps) {
  const [statusFilter, setStatusFilter] = useState<(typeof filters)[number]>('all')

  const rows = useMemo(() => {
    return invoices
      .filter((invoice) => (statusFilter === 'all' ? true : invoice.status === statusFilter))
      .sort((left, right) => left.due_date.localeCompare(right.due_date))
  }, [invoices, statusFilter])

  if (!loading && rows.length === 0) {
    return <EmptyState description="No invoices have been generated yet." />
  }

  return (
    <section className="surface-card ledger-table-card">
      <header className="ledger-table-header">
        <h3>INVOICES</h3>
        <div className="chip-group">
          {filters.map((filter) => (
            <button
              key={filter}
              className={statusFilter === filter ? 'button-dark active' : 'button-dark'}
              onClick={() => setStatusFilter(filter)}
            >
              {filter}
            </button>
          ))}
        </div>
      </header>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Invoice ID</th>
              <th>Customer</th>
              <th>Amount</th>
              <th>Paid</th>
              <th>Due Date</th>
              <th>Status</th>
              <th>Days Overdue</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((invoice) => (
              <tr
                key={invoice.invoice_id}
                onMouseEnter={() => onHoverInvoiceId(invoice.invoice_id)}
                onMouseLeave={() => onHoverInvoiceId(null)}
                className={`${invoice.status === 'overdue' ? 'row-overdue' : ''} ${highlightedInvoiceId === invoice.invoice_id ? 'row-highlight' : ''}`}
              >
                <td className="mono">{invoice.invoice_id}</td>
                <td>{invoice.customer_name}</td>
                <td className="numeric">{formatCurrency(invoice.total_amount, invoice.currency)}</td>
                <td className="numeric">{formatCurrency(invoice.paid_amount, invoice.currency)}</td>
                <td className="numeric">{new Date(invoice.due_date).toLocaleDateString()}</td>
                <td>
                  <StatusBadge status={invoice.status} />
                </td>
                <td className={invoice.status === 'overdue' ? 'overdue-days numeric' : 'numeric'}>
                  {invoice.days_overdue}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}
