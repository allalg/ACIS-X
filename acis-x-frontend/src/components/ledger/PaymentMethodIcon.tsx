import {
  BankTransferIcon,
  CardPaymentIcon,
  ChequeIcon,
  GenericPaymentIcon,
} from '../ui/icons'

type PaymentMethodIconProps = {
  method: 'bank_transfer' | 'card' | 'cheque' | 'other'
}

export function PaymentMethodIcon({ method }: PaymentMethodIconProps) {
  if (method === 'bank_transfer') {
    return <BankTransferIcon size={16} />
  }
  if (method === 'card') {
    return <CardPaymentIcon size={16} />
  }
  if (method === 'cheque') {
    return <ChequeIcon size={16} />
  }
  return <GenericPaymentIcon size={16} />
}
