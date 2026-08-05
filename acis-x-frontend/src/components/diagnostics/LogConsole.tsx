import { useEffect, useRef, useState } from 'react'
import { getApiBaseUrl, API_KEY } from '../../services/api'

function getLogsUrl(): string {
  const envLogsUrl = import.meta.env.VITE_LOGS_URL
  if (envLogsUrl && !envLogsUrl.includes('localhost')) {
    return envLogsUrl
  }
  const baseUrl = getApiBaseUrl()
  return `${baseUrl}/api/v1/system/logs/stream`
}

type LogEntry = {
  ts?: string
  level?: string
  logger?: string
  msg?: string
  raw: string
}

export function LogConsole() {
  const [logs, setLogs] = useState<LogEntry[]>([])
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const url = new URL(getLogsUrl())
    if (API_KEY) {
      url.searchParams.set('api_key', API_KEY)
    }

    const source = new EventSource(url.toString())

    source.onmessage = (event) => {
      const data = event.data
      if (!data) return

      let parsed: LogEntry = { raw: data }
      try {
        const json = JSON.parse(data)
        parsed = {
          ts: json.ts,
          level: json.level,
          logger: json.logger,
          msg: json.msg,
          raw: data,
        }
      } catch {
        // Fallback for non-JSON logs
        parsed = { raw: data }
      }

      setLogs((prev) => {
        const next = [...prev, parsed]
        if (next.length > 300) {
          return next.slice(next.length - 300)
        }
        return next
      })
    }

    return () => {
      source.close()
    }
  }, [])

  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    // Only auto-scroll if the user is already scrolled to the bottom (within a tolerance of 60px)
    const isNearBottom = container.scrollHeight - container.clientHeight - container.scrollTop < 60
    if (isNearBottom || container.scrollTop === 0) {
      container.scrollTop = container.scrollHeight
    }
  }, [logs])

  const getLogLevelColor = (level?: string) => {
    switch (level?.toUpperCase()) {
      case 'ERROR':
        return 'var(--risk-high)'
      case 'WARNING':
      case 'WARN':
        return 'var(--risk-medium)'
      case 'INFO':
        return 'var(--accent-blue)'
      default:
        return 'var(--text-secondary)'
    }
  }

  return (
    <section className="surface-card" style={{ padding: '1.2rem', marginTop: '1.5rem', display: 'flex', flexDirection: 'column', height: '400px' }}>
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.8rem' }}>
        <h3 className="mono" style={{ margin: 0, fontSize: '0.95rem', color: 'var(--text-primary)' }}>
          SYSTEM DIAGNOSTICS CONSOLE
        </h3>
        <span className="mono" style={{ fontSize: '0.75rem', color: 'var(--accent-green)' }}>
          ● LIVE LOG STREAM
        </span>
      </header>

      <div
        ref={containerRef}
        style={{
          flex: 1,
          backgroundColor: '#05070a',
          borderRadius: '6px',
          padding: '1rem',
          overflowY: 'auto',
          fontFamily: 'var(--font-mono, monospace)',
          fontSize: '0.8rem',
          lineHeight: '1.5',
          border: '1px solid var(--bg-border)',
        }}
      >
        {logs.length === 0 ? (
          <div className="text-secondary mono" style={{ opacity: 0.5 }}>
            Connecting to log stream...
          </div>
        ) : (
          logs.map((log, idx) => (
            <div key={idx} style={{ marginBottom: '0.3rem', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
              {log.ts && (
                <span className="text-secondary" style={{ marginRight: '0.6rem', fontSize: '0.75rem' }}>
                  [{log.ts}]
                </span>
              )}
              {log.level && (
                <span style={{ color: getLogLevelColor(log.level), fontWeight: 'bold', marginRight: '0.6rem' }}>
                  {log.level}
                </span>
              )}
              {log.logger && (
                <span style={{ color: '#8b5cf6', marginRight: '0.6rem' }}>
                  ({log.logger})
                </span>
              )}
              <span style={{ color: 'var(--text-primary)' }}>
                {log.msg || log.raw}
              </span>
            </div>
          ))
        )}
      </div>
    </section>
  )
}
