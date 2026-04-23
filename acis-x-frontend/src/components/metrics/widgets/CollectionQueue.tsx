import type { CustomerMetric, RiskProfile } from '../../../types/metrics'
import { formatCurrency } from '../../../lib/utils'
import { EmptyState } from '../../ui/EmptyState'

type CollectionQueueProps = {
  riskProfiles: RiskProfile[]
  metrics: CustomerMetric[]
}

export function CollectionQueue({ riskProfiles, metrics }: CollectionQueueProps) {
  // Include medium+ severity OR any customer with combined_risk > 0.3
  const rows = riskProfiles
    .filter(
      (row) =>
        row.severity === 'high' ||
        row.severity === 'critical' ||
        row.severity === 'medium' ||
        row.combined_risk > 0.3,
    )
    .map((row) => {
      const metric = metrics.find((candidate) => candidate.customer_id === row.customer_id)
      const delayFactor = metric ? 1 + metric.avg_delay / 30 : 1
      const urgency = row.combined_risk * 10 * delayFactor
      let recommendation = 'Monitor'
      if (row.combined_risk > 0.85) recommendation = 'Escalate immediately'
      else if (row.combined_risk > 0.7) recommendation = 'Send formal notice'
      else if (row.combined_risk > 0.5) recommendation = 'Follow up — payment due'
      else if (row.combined_risk > 0.3) recommendation = 'Review account'

      return {
        customer_id: row.customer_id,
        company_name: row.company_name,
        severity: row.severity,
        urgency,
        recommendation,
        days_since_last_payment: metric ? Math.round(metric.avg_delay) : 0,
        outstanding: metric?.total_outstanding ?? 0,
      }
    })
    .sort((left, right) => right.urgency - left.urgency)
    .slice(0, 10)

  if (rows.length === 0) {
    return <EmptyState description="No customers currently require collection action." />
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Customer</th>
            <th>Severity</th>
            <th>Urgency</th>
            <th>Action</th>
            <th>Avg Delay (days)</th>
            <th>Outstanding</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.customer_id}>
              <td>{row.company_name}</td>
              <td>
                <span className={`status-badge status-${row.severity === 'critical' ? 'failed' : row.severity === 'high' ? 'overdue' : row.severity === 'medium' ? 'pending' : 'paid'}`}>
                  {row.severity}
                </span>
              </td>
              <td className="numeric">{row.urgency.toFixed(1)}</td>
              <td>{row.recommendation}</td>
              <td className="numeric">{row.days_since_last_payment}</td>
              <td className="numeric">{formatCurrency(row.outstanding)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
