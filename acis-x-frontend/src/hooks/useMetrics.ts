import { useMutation, useQuery } from '@tanstack/react-query'
import { api } from '../services/api'

export function useComputeMetrics() {
  return useMutation({
    mutationFn: () => api.computeMetrics(),
  })
}

export function useMetricsResult(jobId: string | null) {
  return useQuery({
    queryKey: ['metrics-result', jobId],
    queryFn: () => api.getMetricsResult(jobId as string),
    enabled: Boolean(jobId),
    refetchInterval: (query) => {
      const data = query.state.data
      if (!data) {
        return 1500
      }
      return data.status === 'ready' ? false : 1500
    },
    staleTime: 0,
  })
}
