import { useEffect, useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { api } from '../../services/api'
import { toast } from 'sonner'

type TableViewerModalProps = {
  tableName: string | null
  onClose: () => void
}

export function TableViewerModal({ tableName, onClose }: TableViewerModalProps) {
  const [loading, setLoading] = useState(false)
  const [tableData, setTableData] = useState<{ table_name: string; total: number; rows: any[] } | null>(null)
  const [searchTerm, setSearchTerm] = useState('')

  useEffect(() => {
    if (!tableName) return
    let isMounted = true

    const fetchTable = async () => {
      setLoading(true)
      try {
        const res = await api.getDatabaseTableData(tableName, 100)
        if (isMounted) {
          setTableData(res)
        }
      } catch (err) {
        if (isMounted) {
          toast.error(`Failed to fetch database records for table '${tableName}'`)
        }
      } finally {
        if (isMounted) setLoading(false)
      }
    }

    fetchTable()

    return () => {
      isMounted = false
    }
  }, [tableName])

  if (!tableName) return null

  const rows = tableData?.rows ?? []
  const columns = rows.length > 0 ? Object.keys(rows[0]) : []

  const filteredRows = rows.filter((r) =>
    Object.values(r).some(
      (val) =>
        val !== null &&
        val !== undefined &&
        String(val).toLowerCase().includes(searchTerm.toLowerCase())
    )
  )

  return (
    <AnimatePresence>
      <div
        style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(0, 0, 0, 0.82)',
          backdropFilter: 'blur(8px)',
          zIndex: 10000,
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          padding: '1.5rem',
        }}
        onClick={onClose}
      >
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 15 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 15 }}
          transition={{ duration: 0.2 }}
          className="surface-card"
          style={{
            width: '100%',
            maxWidth: '1050px',
            maxHeight: '85vh',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
            border: '1px solid var(--accent-cyan)',
            boxShadow: '0 25px 60px rgba(0, 0, 0, 0.7)',
            padding: 0,
          }}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <header
            style={{
              padding: '1.2rem 1.5rem',
              backgroundColor: 'var(--bg-elevated)',
              borderBottom: '1px solid var(--bg-border)',
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              flexWrap: 'wrap',
              gap: '1rem',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.8rem' }}>
              <span style={{ fontSize: '1.4rem' }}>💾</span>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
                  <h3 className="mono" style={{ margin: 0, color: 'var(--accent-cyan)', fontSize: '1.1rem' }}>
                    {tableName}
                  </h3>
                  <span
                    className="badge mono"
                    style={{
                      backgroundColor: 'rgba(59, 130, 246, 0.15)',
                      color: 'var(--accent-blue)',
                      border: '1px solid var(--accent-blue)',
                      padding: '0.2rem 0.6rem',
                    }}
                  >
                    {tableData?.total ?? 0} TOTAL RECORDS
                  </span>
                </div>
                <p style={{ margin: '0.2rem 0 0 0', color: 'var(--text-muted)', fontSize: '0.8rem' }}>
                  Live SQLite Relational Database Records (acis.db)
                </p>
              </div>
            </div>

            <div style={{ display: 'flex', alignItems: 'center', gap: '0.8rem' }}>
              <input
                type="text"
                placeholder="Filter table rows..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="input"
                style={{ width: '200px', fontSize: '0.8rem', padding: '0.4rem 0.8rem' }}
              />
              <button
                className="button-dark"
                onClick={onClose}
                style={{ padding: '0.4rem 0.8rem', fontSize: '1rem', cursor: 'pointer' }}
              >
                ✕
              </button>
            </div>
          </header>

          {/* Table Content Body */}
          <div style={{ flex: 1, overflowY: 'auto', overflowX: 'auto', padding: '1.2rem' }}>
            {loading ? (
              <div style={{ padding: '3rem', textAlign: 'center', color: 'var(--text-muted)' }}>
                ⏳ Querying SQLite database records for <strong>{tableName}</strong>...
              </div>
            ) : columns.length > 0 ? (
              <table
                style={{
                  width: '100%',
                  borderCollapse: 'collapse',
                  fontSize: '0.82rem',
                  textAlign: 'left',
                }}
              >
                <thead>
                  <tr
                    style={{
                      backgroundColor: 'var(--bg-elevated)',
                      borderBottom: '2px solid var(--bg-border)',
                    }}
                  >
                    {columns.map((col) => (
                      <th
                        key={col}
                        className="mono"
                        style={{
                          padding: '0.75rem 1rem',
                          color: 'var(--accent-cyan)',
                          fontWeight: 'bold',
                          whiteSpace: 'nowrap',
                        }}
                      >
                        {col.toUpperCase()}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {filteredRows.length > 0 ? (
                    filteredRows.map((row, idx) => (
                      <tr
                        key={idx}
                        style={{
                          borderBottom: '1px solid var(--bg-border)',
                          backgroundColor: idx % 2 === 0 ? 'transparent' : 'rgba(255, 255, 255, 0.015)',
                        }}
                      >
                        {columns.map((col) => {
                          const val = row[col]
                          const isNumeric = typeof val === 'number'
                          const isId = col.includes('id') || col.includes('gstin')
                          return (
                            <td
                              key={col}
                              className={isNumeric || isId ? 'mono' : ''}
                              style={{
                                padding: '0.65rem 1rem',
                                color: isId ? 'var(--accent-blue)' : 'var(--text-primary)',
                                maxWidth: '240px',
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                              }}
                              title={String(val ?? '')}
                            >
                              {val === null || val === undefined ? (
                                <span style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}>NULL</span>
                              ) : typeof val === 'object' ? (
                                JSON.stringify(val)
                              ) : (
                                String(val)
                              )}
                            </td>
                          )
                        })}
                      </tr>
                    ))
                  ) : (
                    <tr>
                      <td colSpan={columns.length} style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-muted)' }}>
                        No records match filter search query.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            ) : (
              <div style={{ padding: '3rem', textAlign: 'center', color: 'var(--text-muted)' }}>
                📭 Database table <strong>{tableName}</strong> is currently empty.
              </div>
            )}
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  )
}
