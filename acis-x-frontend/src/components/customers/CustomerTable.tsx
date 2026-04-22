import { Link } from 'react-router-dom'
import type { CustomersResponse } from '../../types/api'
import { SeverityBadge } from '../ui/SeverityBadge'

type CustomerTableProps = {
  data: CustomersResponse
  severityFilter: 'all' | 'low' | 'medium' | 'high' | 'critical'
}

export function CustomerTable({ data, severityFilter }: CustomerTableProps) {
  const rows = data.customers.filter((customer) =>
    severityFilter === 'all' ? true : customer.severity === severityFilter,
  )

  return (
    <section className="surface-card customer-table-card">
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Customer ID</th>
              <th>Name</th>
              <th>Credit Limit</th>
              <th>Risk Score</th>
              <th>Severity</th>
              <th>Avg Delay</th>
              <th>On-Time Ratio</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((customer) => (
              <tr key={customer.customer_id}>
                <td className="mono">
                  <Link to={`/customers/${customer.customer_id}`}>{customer.customer_id}</Link>
                </td>
                <td>{customer.name}</td>
                <td className="numeric">{Math.round(customer.credit_limit).toLocaleString()}</td>
                <td className="numeric">{Math.round(customer.risk_score * 100)}</td>
                <td>
                  <SeverityBadge severity={customer.severity} />
                </td>
                <td className="numeric">{customer.avg_delay.toFixed(1)}</td>
                <td className="numeric">{Math.round(customer.on_time_ratio * 100)}%</td>
                <td>{customer.status}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}
