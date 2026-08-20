import type { CustomerProfile } from '../../../types/api'
import { formatCurrency } from '../../../lib/utils'
import { SeverityBadge } from '../../ui/SeverityBadge'
import { RiskBreakdownBar } from './RiskBreakdownBar'

type CustomerProfileSheetProps = {
  customer: CustomerProfile | null
  open: boolean
  onClose: () => void
}

export function CustomerProfileSheet({ customer, open, onClose }: CustomerProfileSheetProps) {
  if (!open || !customer) {
    return null
  }

  return (
    <div className="sheet-backdrop" role="dialog" aria-modal="true">
      <div className="sheet-panel surface-card">
        <header>
          <h3>{customer.name}</h3>
          <button className="button-dark" onClick={onClose}>
            Close
          </button>
        </header>
        <div className="sheet-grid">
          <div>
            <p className="mono">{customer.customer_id}</p>
            <p className="numeric">Outstanding {formatCurrency(customer.total_outstanding)}</p>
            <p className="numeric">Credit Limit {formatCurrency(customer.credit_limit)}</p>
            <SeverityBadge severity={customer.severity} />
          </div>
          <div>
            <p className="numeric">
              Risk{' '}
              {customer.combined_risk == null
                ? 'pending'
                : `${Math.round(customer.combined_risk * 100)}%`}
            </p>
            <p className="numeric">
              Confidence{' '}
              {customer.confidence == null
                ? 'pending'
                : `${Math.round(customer.confidence * 100)}%`}
            </p>
            <RiskBreakdownBar
              financial={customer.financial_risk ?? 0}
              litigation={customer.litigation_risk ?? 0}
            />
          </div>
        </div>
      </div>
    </div>
  )
}
