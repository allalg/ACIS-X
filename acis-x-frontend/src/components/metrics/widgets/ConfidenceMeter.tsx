type ConfidenceMeterProps = {
  value: number
  count: number
}

export function ConfidenceMeter({ value, count }: ConfidenceMeterProps) {
  const pct = Math.max(0, Math.min(100, Math.round(value * 100)))
  const state = pct > 80 ? 'good' : pct > 60 ? 'warn' : 'bad'

  return (
    <div className="confidence-meter">
      <div className={`confidence-ring ${state}`}>
        <span className="numeric">{pct}%</span>
      </div>
      <small>Based on {count} risk profiles</small>
    </div>
  )
}
