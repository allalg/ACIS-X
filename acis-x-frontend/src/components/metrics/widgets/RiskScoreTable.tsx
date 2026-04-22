import type { RiskProfile } from '../../../types/metrics'
import { SeverityBadge } from '../../ui/SeverityBadge'

type RiskScoreTableProps = {
  rows: RiskProfile[]
  onSelectCustomer: (customerId: string) => void
}

export function RiskScoreTable({ rows, onSelectCustomer }: RiskScoreTableProps) {
  const sorted = [...rows].sort((a, b) => b.combined_risk - a.combined_risk)
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Customer Name</th>
            <th>Risk Score</th>
            <th>Severity</th>
            <th>Financial</th>
            <th>Litigation</th>
            <th>Confidence</th>
            <th>Updated</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((row) => (
            <tr key={row.customer_id} onClick={() => onSelectCustomer(row.customer_id)} className="row-clickable">
              <td>{row.company_name}</td>
              <td className="numeric">{Math.round(row.combined_risk * 100)}</td>
              <td>
                <SeverityBadge severity={row.severity} />
              </td>
              <td className="numeric">{Math.round(row.financial_risk * 100)}</td>
              <td className="numeric">{Math.round(row.litigation_risk * 100)}</td>
              <td className="numeric">{Math.round(row.confidence * 100)}%</td>
              <td className="numeric">{new Date(row.updated_at).toLocaleTimeString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
