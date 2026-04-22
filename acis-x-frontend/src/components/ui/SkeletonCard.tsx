type SkeletonCardProps = {
  height?: 'sm' | 'md' | 'lg'
}

export function SkeletonCard({ height = 'md' }: SkeletonCardProps) {
  return <div className={`surface-card skeleton-card skeleton-${height}`} aria-hidden="true" />
}
