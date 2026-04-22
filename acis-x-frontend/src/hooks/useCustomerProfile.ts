import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'

export function useCustomerProfile(customerId: string) {
  return useQuery({
    queryKey: ['customer-profile', customerId],
    queryFn: () => api.getCustomerById(customerId),
    enabled: Boolean(customerId),
    refetchInterval: 5000,
    staleTime: 0,
  })
}
