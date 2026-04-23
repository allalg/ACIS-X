import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

type RiskHistoryChartProps = {
  currentRisk: number
}

export function RiskHistoryChart({ currentRisk }: RiskHistoryChartProps) {
  const points = Array.from({ length: 12 }).map((_, index) => ({
    month: `M${index + 1}`,
    risk: Math.max(0.08, Math.min(0.98, currentRisk + (Math.random() - 0.5) * 0.18)),
  }))

  return (
    <div className="chart-wrap">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points}>
          <CartesianGrid stroke="var(--bg-border-subtle)" strokeDasharray="2 2" />
          <XAxis dataKey="month" tick={{ fill: 'var(--text-secondary)', fontSize: 11 }} />
          <YAxis domain={[0, 1]} tick={{ fill: 'var(--text-secondary)', fontSize: 11 }} />
          <Tooltip
            formatter={(value) => {
              if (typeof value !== 'number') {
                return '-'
              }
              return `${Math.round(value * 100)}%`
            }}
            contentStyle={{ background: 'var(--bg-elevated)', border: '1px solid var(--bg-border)', color: 'var(--text-primary)' }}
          />
          <Line type="monotone" dataKey="risk" stroke="var(--accent-blue)" strokeWidth={2} dot={false} />
          <Line type="monotone" dataKey={() => 0.4} stroke="var(--accent-amber)" strokeDasharray="4 3" dot={false} />
          <Line type="monotone" dataKey={() => 0.7} stroke="var(--accent-red)" strokeDasharray="4 3" dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
