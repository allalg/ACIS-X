type EmptyStateProps = {
  title?: string
  description: string
}

export function EmptyState({ title = 'No data yet', description }: EmptyStateProps) {
  return (
    <div className="empty-state surface-card">
      <svg className="empty-state-graphic" viewBox="0 0 220 64" fill="none" stroke="currentColor">
        <path d="M16 32h188" strokeWidth="1" />
        <circle cx="40" cy="32" r="7" strokeWidth="1.2" />
        <circle cx="96" cy="32" r="7" strokeWidth="1.2" />
        <circle cx="148" cy="32" r="7" strokeWidth="1.2" />
        <circle cx="188" cy="32" r="7" strokeWidth="1.2" />
      </svg>
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  )
}
