import type { RiskProfile } from '../../../types/metrics'
import { TrendDownIcon, TrendUpIcon } from '../../ui/icons'

type TopRiskCustomersProps = {
  rows: RiskProfile[]
  onSelectCustomer: (customerId: string) => void
}

export function TopRiskCustomers({ rows, onSelectCustomer }: TopRiskCustomersProps) {
  const top = rows.slice().sort((left, right) => right.combined_risk - left.combined_risk).slice(0, 10)

  return (
    <ol className="top-risk-list">
      {top.map((row, index) => {
        const delta = Math.random() - 0.5
        return (
          <li key={row.customer_id} onClick={() => onSelectCustomer(row.customer_id)}>
            <span className="numeric">{index + 1}</span>
            <span>{row.company_name}</span>
            <span className="numeric">{Math.round(row.combined_risk * 100)}</span>
            <span className={delta >= 0 ? 'delta-up' : 'delta-down'}>
              {delta >= 0 ? <TrendUpIcon /> : <TrendDownIcon />}
            </span>
          </li>
        )
      })}
    </ol>
  )
}
