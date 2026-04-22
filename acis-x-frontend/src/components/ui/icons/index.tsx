import type { SVGProps } from 'react'

type IconProps = SVGProps<SVGSVGElement> & {
  size?: number
}

function BaseIcon({ size = 16, children, ...props }: IconProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      {...props}
    >
      {children}
    </svg>
  )
}

export function SimulationIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M1.5 5.5h13" />
      <path d="M4 5.5V12" />
      <path d="M8 5.5V11" />
      <path d="M12 5.5V13" />
      <circle cx="4" cy="12.5" r="1" />
      <circle cx="8" cy="11.5" r="1" />
      <circle cx="12" cy="13.5" r="1" />
    </BaseIcon>
  )
}

export function LedgerIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M3 1.8h7l3 3V14.2H3z" />
      <path d="M10 1.8v3h3" />
      <path d="M5.2 7h5.8" />
      <path d="M5.2 9.7h5.8" />
    </BaseIcon>
  )
}

export function MetricsIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M3 12.5V7.5" />
      <path d="M7.5 12.5V5" />
      <path d="M12 12.5V2.5" />
      <path d="M1.8 12.5h12.4" />
    </BaseIcon>
  )
}

export function CustomersIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <circle cx="6" cy="6" r="2.5" />
      <circle cx="11" cy="7" r="2.2" />
      <path d="M2.5 13.5c0-2 1.9-3.3 4.2-3.3S11 11.5 11 13.5" />
      <path d="M9.5 13.5c0-1.5 1.4-2.6 3.2-2.6" />
    </BaseIcon>
  )
}

export function LiveDotIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <circle cx="8" cy="8" r="3.3" />
    </BaseIcon>
  )
}

export function AlertTriangleIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M8 2.2 1.8 13h12.4z" />
      <path d="M8 6v3.5" />
      <circle cx="8" cy="11.7" r="0.7" fill="currentColor" stroke="none" />
    </BaseIcon>
  )
}

export function CheckCircleIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <circle cx="8" cy="8" r="6" />
      <path d="M5.2 8.2 7.2 10.2 10.9 6.5" />
    </BaseIcon>
  )
}

export function SpinnerIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M13.2 8a5.2 5.2 0 1 1-2.8-4.6" />
    </BaseIcon>
  )
}

export function AgentNodeIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <circle cx="8" cy="8" r="6" />
      <path d="M8 4.7 10.4 6v2.7L8 10 5.6 8.7V6z" />
    </BaseIcon>
  )
}

export function KafkaBusIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M1.5 6h13" />
      <path d="M1.5 10h13" />
      <path d="M4 4.3v7.4" />
      <path d="M8 4.3v7.4" />
      <path d="M12 4.3v7.4" />
    </BaseIcon>
  )
}

export function RegistryIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <circle cx="4" cy="4" r="1" />
      <circle cx="8" cy="4" r="1" />
      <circle cx="12" cy="4" r="1" />
      <circle cx="4" cy="8" r="1" />
      <circle cx="8" cy="8" r="1" />
      <circle cx="12" cy="8" r="1" />
    </BaseIcon>
  )
}

export function SelfHealIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M13 8a5 5 0 1 1-1.4-3.5" />
      <path d="M11.6 2.8v2.4H9.2" />
      <path d="M8 6.4v3.2" />
      <path d="M6.4 8h3.2" />
    </BaseIcon>
  )
}

export function SortAscIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M8 4.2 4.8 7.4h6.4z" />
    </BaseIcon>
  )
}

export function SortDescIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="m8 11.8 3.2-3.2H4.8z" />
    </BaseIcon>
  )
}

export function FilterIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M2.2 3.2h11.6L9.2 8.1v4.2L6.8 13V8.1z" />
    </BaseIcon>
  )
}

export const DragHandleIcon = RegistryIcon

export function ExpandIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M6.2 2.5H2.5v3.7" />
      <path d="M9.8 2.5h3.7v3.7" />
      <path d="M6.2 13.5H2.5V9.8" />
      <path d="M9.8 13.5h3.7V9.8" />
      <path d="m2.8 6 3.4-3.4" />
      <path d="m13.2 6-3.4-3.4" />
      <path d="m2.8 10 3.4 3.4" />
      <path d="m13.2 10-3.4 3.4" />
    </BaseIcon>
  )
}

export function CollapseIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="m2.7 2.7 3 3" />
      <path d="m13.3 2.7-3 3" />
      <path d="m2.7 13.3 3-3" />
      <path d="m13.3 13.3-3-3" />
      <path d="M6 6h4v4H6z" />
    </BaseIcon>
  )
}

export function RefreshIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M13 8a5 5 0 1 1-1.7-3.8" />
      <path d="M11.3 2.9v2.7H8.6" />
    </BaseIcon>
  )
}

export function RiskShieldIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M8 1.8 12.5 3v3.8c0 2.9-1.7 5-4.5 7.2C5.2 11.8 3.5 9.7 3.5 6.8V3z" />
      <path d="M5.3 9.4 7 7.7 8.3 8.9 10.7 6.5" />
    </BaseIcon>
  )
}

export function TrendUpIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M2.5 11.8 7 7.3l2.3 2.3 4.2-4.2" />
      <path d="M10.8 5.4h2.7v2.7" />
    </BaseIcon>
  )
}

export function TrendDownIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M2.5 4.2 7 8.7l2.3-2.3 4.2 4.2" />
      <path d="M10.8 10.6h2.7V7.9" />
    </BaseIcon>
  )
}

export function BankTransferIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <path d="M1.8 8h12.4" />
      <path d="m4.5 5.2-2.7 2.8 2.7 2.8" />
      <path d="m11.5 5.2 2.7 2.8-2.7 2.8" />
    </BaseIcon>
  )
}

export function CardPaymentIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <rect x="1.8" y="3" width="12.4" height="10" rx="1.4" />
      <path d="M1.8 6.6h12.4" />
    </BaseIcon>
  )
}

export function ChequeIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <rect x="2" y="3" width="12" height="10" rx="1.2" />
      <path d="M4 6.2h5.8" />
      <path d="M4 8.7h3.8" />
      <path d="m10 8 2.2 2.2" />
      <path d="m10 10.2 2.2 2.2" />
    </BaseIcon>
  )
}

export function GenericPaymentIcon(props: IconProps) {
  return (
    <BaseIcon {...props}>
      <circle cx="8" cy="8" r="5.8" />
      <path d="M8 4.8v6.4" />
      <path d="M10.2 5.9c-0.5-0.7-1.5-1-2.3-0.7-0.8 0.3-1.1 1.3-0.6 1.9 0.8 1.1 3.7 0.6 3.4 2.4-0.2 1.4-2.5 1.9-3.9 0.9" />
    </BaseIcon>
  )
}
