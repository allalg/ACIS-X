import { useQuery } from '@tanstack/react-query'
import { api } from '../services/api'

export function useInvoices(customerId?: string, status?: string) {
  return useQuery({
    queryKey: ['invoices', customerId ?? 'all', status ?? 'all'],
    queryFn: () => api.getInvoices(customerId, status),
    staleTime: 0,
  })
}
