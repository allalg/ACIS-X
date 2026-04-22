import type { CustomerMetric, RiskProfile } from '../../../types/metrics'
import { formatCurrency } from '../../../lib/utils'

type CollectionQueueProps = {
  riskProfiles: RiskProfile[]
  metrics: CustomerMetric[]
}

export function CollectionQueue({ riskProfiles, metrics }: CollectionQueueProps) {
  const rows = riskProfiles
    .filter((row) => row.severity === 'high' || row.severity === 'critical')
    .map((row) => {
      const metric = metrics.find((candidate) => candidate.customer_id === row.customer_id)
      const delayFactor = metric ? 1 + metric.avg_delay / 30 : 1
      const urgency = row.combined_risk * 10 * delayFactor
      let recommendation = 'Monitor'
      if (row.combined_risk > 0.85) recommendation = 'Escalate immediately'
      else if (row.combined_risk > 0.7) recommendation = 'Send formal notice'
      else if (row.combined_risk > 0.5) recommendation = 'Follow up - payment due'

      return {
        customer_id: row.customer_id,
        urgency,
        recommendation,
        days_since_last_payment: metric ? Math.round(metric.avg_delay) : 0,
        outstanding: metric?.total_outstanding ?? 0,
      }
    })
    .sort((left, right) => right.urgency - left.urgency)
    .slice(0, 10)

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Customer</th>
            <th>Urgency</th>
            <th>Action</th>
            <th>Days</th>
            <th>Outstanding</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.customer_id}>
              <td className="mono">{row.customer_id}</td>
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
