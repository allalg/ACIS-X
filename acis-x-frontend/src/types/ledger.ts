export type Invoice = {
  invoice_id: string
  customer_id: string
  customer_name: string
  total_amount: number
  paid_amount: number
  currency: string
  issued_date: string
  due_date: string
  status: 'pending' | 'overdue' | 'paid' | 'partial' | 'cancelled' | 'disputed'
  days_overdue: number
  created_at: string
  updated_at: string
}

export type Payment = {
  payment_id: string
  invoice_id: string
  customer_id: string
  customer_name: string
  amount: number
  currency: string
  payment_date: string
  payment_method: 'bank_transfer' | 'card' | 'cheque' | 'other'
  status: 'received' | 'processing' | 'applied' | 'failed'
  reference: string
  created_at: string
}

export type InvoiceResponse = {
  invoices: Invoice[]
  total: number
}

export type PaymentResponse = {
  payments: Payment[]
  total: number
}
