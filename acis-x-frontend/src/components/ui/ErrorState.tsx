import { AlertTriangleIcon } from './icons'

type ErrorStateProps = {
  message: string
  retryInSeconds?: number
}

export function ErrorState({ message, retryInSeconds = 0 }: ErrorStateProps) {
  return (
    <div className="error-state surface-card" role="alert">
      <AlertTriangleIcon size={18} />
      <span>{message}</span>
      {retryInSeconds > 0 ? <span>Retrying in {retryInSeconds}s.</span> : null}
    </div>
  )
}
