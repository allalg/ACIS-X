import { useMemo, useState } from 'react'
import { InvoiceTable } from '../components/ledger/InvoiceTable'
import { LedgerKPIStrip } from '../components/ledger/LedgerKPIStrip'
import { PaymentTable } from '../components/ledger/PaymentTable'
import { LiveIndicator } from '../components/ui/LiveIndicator'
import { useDashboardSummary } from '../hooks/useDashboardSummary'
import { useEventStream } from '../hooks/useEventStream'
import { useInvoices } from '../hooks/useInvoices'
import { usePayments } from '../hooks/usePayments'

export default function LedgerPage() {
  const { data: summary } = useDashboardSummary()
  const { data: invoiceData, isLoading: invoicesLoading } = useInvoices()
  const { data: paymentData, isLoading: paymentsLoading } = usePayments()
  const { status: streamStatus } = useEventStream()
  const [hoveredInvoiceId, setHoveredInvoiceId] = useState<string | null>(null)

  const invoices = useMemo(() => invoiceData?.invoices ?? [], [invoiceData])
  const payments = useMemo(() => paymentData?.payments ?? [], [paymentData])

  return (
    <div className="ledger-page">
      <header className="page-header">
        <div>
          <h1 className="page-title">LEDGER</h1>
          <p className="page-subtitle">Real-time invoice and payment intelligence feed</p>
        </div>
        <LiveIndicator status={streamStatus} />
      </header>

      <section className="ledger-columns">
        <InvoiceTable
          invoices={invoices}
          loading={invoicesLoading}
          onHoverInvoiceId={setHoveredInvoiceId}
          highlightedInvoiceId={hoveredInvoiceId}
        />
        <PaymentTable
          payments={payments}
          loading={paymentsLoading}
          onHoverInvoiceId={setHoveredInvoiceId}
        />
      </section>

      <LedgerKPIStrip summary={summary} invoices={invoices} payments={payments} />
    </div>
  )
}
