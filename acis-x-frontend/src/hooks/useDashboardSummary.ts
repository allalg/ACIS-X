import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'

export function useDashboardSummary() {
  return useQuery({
    queryKey: ['dashboard-summary'],
    queryFn: () => api.getDashboardSummary(),
    refetchInterval: 5000,
    staleTime: 0,
  })
}
