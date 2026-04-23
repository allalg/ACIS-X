import {
  Bar,
  BarChart,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { RiskProfile } from '../../../types/metrics'

type RiskDistributionChartProps = {
  rows: RiskProfile[]
}

export function RiskDistributionChart({ rows }: RiskDistributionChartProps) {
  const bySeverity = [
    { severity: 'low', count: rows.filter((row) => row.severity === 'low').length, color: 'var(--risk-low)' },
    {
      severity: 'medium',
      count: rows.filter((row) => row.severity === 'medium').length,
      color: 'var(--risk-medium)',
    },
    { severity: 'high', count: rows.filter((row) => row.severity === 'high').length, color: 'var(--risk-high)' },
    {
      severity: 'critical',
      count: rows.filter((row) => row.severity === 'critical').length,
      color: 'var(--risk-critical)',
    },
  ]

  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={bySeverity}>
          <XAxis dataKey="severity" tick={{ fill: 'var(--text-secondary)', fontSize: 11 }} />
          <YAxis tick={{ fill: 'var(--text-secondary)', fontSize: 11 }} />
          <Tooltip
            contentStyle={{ background: 'var(--bg-elevated)', border: '1px solid var(--bg-border)', color: 'var(--text-primary)' }}
            cursor={{ fill: 'var(--accent-blue-soft)' }}
          />
          <Bar dataKey="count" radius={[4, 4, 0, 0]}>
            {bySeverity.map((entry) => (
              <Cell key={entry.severity} fill={entry.color} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
