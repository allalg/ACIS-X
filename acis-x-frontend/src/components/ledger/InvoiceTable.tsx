import { Fragment, useMemo, useState } from 'react'
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
  const [expandedId, setExpandedId] = useState<string | null>(null)

  const rows = useMemo(() => {
    return invoices
      .filter((invoice) => (statusFilter === 'all' ? true : invoice.status === statusFilter))
      .sort((left, right) => left.due_date.localeCompare(right.due_date))
  }, [invoices, statusFilter])

  if (!loading && rows.length === 0) {
    return <EmptyState description="No invoices have been generated yet." />
  }

  const toggleExpand = (id: string) => {
    setExpandedId((prev) => (prev === id ? null : id))
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
            {rows.map((invoice) => {
              const isExpanded = expandedId === invoice.invoice_id
              const balance = invoice.total_amount - invoice.paid_amount
              return (
                <Fragment key={invoice.invoice_id}>
                  <tr
                    key={invoice.invoice_id}
                    onClick={() => toggleExpand(invoice.invoice_id)}
                    onMouseEnter={() => onHoverInvoiceId(invoice.invoice_id)}
                    onMouseLeave={() => onHoverInvoiceId(null)}
                    className={`row-clickable ${invoice.status === 'overdue' ? 'row-overdue' : ''} ${highlightedInvoiceId === invoice.invoice_id ? 'row-highlight' : ''}`}
                  >
                    <td className="mono">{invoice.invoice_id}</td>
                    <td>{invoice.customer_name}</td>
                    <td className="numeric">{formatCurrency(invoice.total_amount, invoice.currency)}</td>
                    <td className="numeric">{formatCurrency(invoice.paid_amount, invoice.currency)}</td>
                    <td className="numeric">{new Date(invoice.due_date).toLocaleDateString('en-IN')}</td>
                    <td>
                      <StatusBadge status={invoice.status} />
                    </td>
                    <td className={invoice.status === 'overdue' ? 'overdue-days numeric' : 'numeric'}>
                      {invoice.days_overdue}
                    </td>
                  </tr>
                  {isExpanded && (
                    <tr key={`${invoice.invoice_id}-detail`} className="expanded-row">
                      <td colSpan={7}>
                        <div className="expanded-detail">
                          <div className="detail-grid">
                            <div className="detail-item">
                              <span className="detail-label">Customer ID</span>
                              <span className="detail-value mono">{invoice.customer_id}</span>
                            </div>
                            <div className="detail-item">
                              <span className="detail-label">Outstanding Balance</span>
                              <span className={`detail-value numeric ${balance > 0 ? 'kpi-danger' : 'kpi-success'}`}>
                                {formatCurrency(balance, invoice.currency)}
                              </span>
                            </div>
                            <div className="detail-item">
                              <span className="detail-label">Issued Date</span>
                              <span className="detail-value numeric">{new Date(invoice.issued_date).toLocaleDateString('en-IN')}</span>
                            </div>
                            <div className="detail-item">
                              <span className="detail-label">Due Date</span>
                              <span className="detail-value numeric">{new Date(invoice.due_date).toLocaleDateString('en-IN')}</span>
                            </div>
                            <div className="detail-item">
                              <span className="detail-label">Created</span>
                              <span className="detail-value numeric">{new Date(invoice.created_at).toLocaleString('en-IN')}</span>
                            </div>
                            <div className="detail-item">
                              <span className="detail-label">Last Updated</span>
                              <span className="detail-value numeric">{new Date(invoice.updated_at).toLocaleString('en-IN')}</span>
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
