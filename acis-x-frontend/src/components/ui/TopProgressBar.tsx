type TopProgressBarProps = {
  active: boolean
}

export function TopProgressBar({ active }: TopProgressBarProps) {
  if (!active) {
    return null
  }

  return (
    <div className="top-progress" aria-hidden="true">
      <div className="top-progress-bar" />
    </div>
  )
}
