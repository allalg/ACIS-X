import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'

export function usePayments(customerId?: string, invoiceId?: string) {
  return useQuery({
    queryKey: ['payments', customerId ?? 'all', invoiceId ?? 'all'],
    queryFn: () => api.getPayments(customerId, invoiceId),
    refetchInterval: 3000,
    staleTime: 0,
  })
}
