import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { CustomerMetric } from '../../../types/metrics'

type PaymentTrendChartProps = {
  rows: CustomerMetric[]
}

export function PaymentTrendChart({ rows }: PaymentTrendChartProps) {
  const trend = rows.slice(0, 10).map((row, index) => ({
    day: `D-${10 - index}`,
    onTimeRatio: Math.round(row.on_time_ratio * 100),
    avgDelay: Number(row.avg_delay.toFixed(1)),
  }))

  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={trend}>
          <CartesianGrid stroke="var(--bg-border-subtle)" strokeDasharray="2 2" />
          <XAxis dataKey="day" tick={{ fill: 'var(--text-secondary)', fontSize: 11 }} />
          <YAxis tick={{ fill: 'var(--text-secondary)', fontSize: 11 }} />
          <Tooltip contentStyle={{ background: 'var(--bg-elevated)', border: '1px solid var(--bg-border)', color: 'var(--text-primary)' }} />
          <Area
            type="monotone"
            dataKey="onTimeRatio"
            stroke="var(--accent-blue)"
            fill="var(--accent-blue-soft)"
            strokeWidth={2}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  )
}
