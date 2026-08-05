import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'

export function useDashboardSummary() {
  return useQuery({
    queryKey: ['dashboard-summary'],
    queryFn: () => api.getDashboardSummary(),
    staleTime: 0,
  })
}
