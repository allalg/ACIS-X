type RiskBreakdownBarProps = {
  financial: number
  litigation: number
}

export function RiskBreakdownBar({ financial, litigation }: RiskBreakdownBarProps) {
  const total = financial + litigation
  const financialPct = total > 0 ? Math.round((financial / total) * 100) : 0
  const litigationPct = total > 0 ? Math.round((litigation / total) * 100) : 0

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
