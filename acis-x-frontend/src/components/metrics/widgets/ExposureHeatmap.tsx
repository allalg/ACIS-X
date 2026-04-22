import type { CustomerMetric } from '../../../types/metrics'
import { formatCurrency } from '../../../lib/utils'

type ExposureHeatmapProps = {
  rows: CustomerMetric[]
}

export function ExposureHeatmap({ rows }: ExposureHeatmapProps) {
  const top = rows
    .slice()
    .sort((left, right) => right.total_outstanding - left.total_outstanding)
    .slice(0, 10)

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Customer</th>
            <th>Current</th>
            <th>30-60d</th>
            <th>60-90d</th>
            <th>90d+</th>
          </tr>
        </thead>
        <tbody>
          {top.map((row) => (
            <tr key={row.customer_id}>
              <td className="mono">{row.customer_id}</td>
              <td className="numeric">{formatCurrency(row.total_outstanding * 0.35)}</td>
              <td className="numeric">{formatCurrency(row.total_outstanding * 0.28)}</td>
              <td className="numeric">{formatCurrency(row.total_outstanding * 0.22)}</td>
              <td className="numeric">{formatCurrency(row.total_outstanding * 0.15)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
