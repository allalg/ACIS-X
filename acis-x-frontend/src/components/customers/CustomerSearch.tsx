type CustomerSearchProps = {
  value: string
  onChange: (value: string) => void
}

export function CustomerSearch({ value, onChange }: CustomerSearchProps) {
  return (
    <input
      className="input-dark customer-search"
      placeholder="Search by customer name or ID"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    />
  )
}
