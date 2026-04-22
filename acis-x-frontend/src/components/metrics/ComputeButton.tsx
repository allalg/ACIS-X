import { SpinnerIcon } from '../ui/icons'

type ComputeButtonProps = {
  onClick: () => void
  loading: boolean
}

export function ComputeButton({ onClick, loading }: ComputeButtonProps) {
  return (
    <button className="button-dark compute-button" onClick={onClick} disabled={loading}>
      {loading ? (
        <>
          <SpinnerIcon className="spin" />
          COMPUTING
        </>
      ) : (
        'COMPUTE METRICS'
      )}
    </button>
  )
}
