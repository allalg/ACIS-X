import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'

export function useAgentStatus() {
  return useQuery({
    queryKey: ['agents-status'],
    queryFn: () => api.getAgentStatus(),
    refetchInterval: 5000,
    staleTime: 0,
  })
}
