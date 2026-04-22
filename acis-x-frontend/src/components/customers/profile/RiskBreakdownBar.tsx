type RiskBreakdownBarProps = {
  financial: number
  litigation: number
}

export function RiskBreakdownBar({ financial, litigation }: RiskBreakdownBarProps) {
  const total = Math.max(financial + litigation, 0.0001)
  const financialPct = Math.round((financial / total) * 100)
  const litigationPct = Math.max(0, 100 - financialPct)

  return (
    <div className="risk-breakdown">
      <div className="risk-breakdown-track">
        <progress className="risk-progress financial" max={100} value={financialPct} />
        <progress className="risk-progress litigation" max={100} value={litigationPct} />
      </div>
      <div className="risk-breakdown-labels">
        <span>Financial {financialPct}%</span>
        <span>Litigation {litigationPct}%</span>
      </div>
    </div>
  )
}
