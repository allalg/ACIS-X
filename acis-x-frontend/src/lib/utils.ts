import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatCurrency(amount: number, _currency = 'INR'): string {
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency: 'INR',
    maximumFractionDigits: 2,
  }).format(amount)
}

export function formatNumber(value: number): string {
  return new Intl.NumberFormat('en-IN').format(value)
}

export function safeDate(input: string | number | Date | null | undefined): Date | null {
  if (!input) return null
  const d = new Date(input)
  return isNaN(d.getTime()) ? null : d
}

export function formatDate(input: string | number | Date | null | undefined, locale = 'en-IN'): string {
  const d = safeDate(input)
  if (!d) return '—'
  return d.toLocaleDateString(locale)
}

export function formatDateTime(input: string | number | Date | null | undefined, locale = 'en-IN'): string {
  const d = safeDate(input)
  if (!d) return '—'
  return d.toLocaleString(locale)
}

export function formatTime(input: string | number | Date | null | undefined, locale = 'en-IN'): string {
  const d = safeDate(input)
  if (!d) return '—'
  return d.toLocaleTimeString(locale)
}

export function formatRelativeTime(isoString: string | null | undefined): string {
  const date = safeDate(isoString)
  if (!date) return '—'
  const diffMs = Date.now() - date.getTime()
  const diffSec = Math.round(diffMs / 1000)
  if (diffSec < 60) {
    return `${Math.max(0, diffSec)}s ago`
  }
  const diffMin = Math.round(diffSec / 60)
  if (diffMin < 60) {
    return `${diffMin}m ago`
  }
  const diffHour = Math.round(diffMin / 60)
  if (diffHour < 24) {
    return `${diffHour}h ago`
  }
  const diffDay = Math.round(diffHour / 24)
  return `${diffDay}d ago`
}

export function formatTimestamp(isoString: string | null | undefined): string {
  const date = safeDate(isoString)
  if (!date) return '—'
  return new Intl.DateTimeFormat('en-IN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    fractionalSecondDigits: 3,
    hour12: false,
  }).format(date)
}
