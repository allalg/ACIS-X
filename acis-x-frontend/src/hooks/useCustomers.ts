import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'

export function useCustomers(search = '') {
  return useQuery({
    queryKey: ['customers', search],
    queryFn: () => api.getCustomers(search),
    refetchInterval: 10000,
    staleTime: 0,
  })
}
