import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { getRuntimeApiUrl, setRuntimeApiUrl } from '../../services/api'
import { toast } from 'sonner'

type ApiConfigModalProps = {
  isOpen: boolean
  onClose: () => void
}

export function ApiConfigModal({ isOpen, onClose }: ApiConfigModalProps) {
  const currentUrl = getRuntimeApiUrl()
  const [inputUrl, setInputUrl] = useState(currentUrl)

  if (!isOpen) return null

  const handleSave = () => {
    const clean = inputUrl.trim().replace(/\/+$/, '')
    if (!clean) {
      toast.error('API URL cannot be empty.')
      return
    }
    setRuntimeApiUrl(clean)
    toast.success(`API URL updated to ${clean}`)
    onClose()
  }

  const handleReset = () => {
    setRuntimeApiUrl('')
    toast.success('Reset to default build URL')
    onClose()
  }

  return (
    <AnimatePresence>
      <div
        style={{
          position: 'fixed',
          top: 0,
          left: 0,
          right: 0,
          bottom: 0,
          backgroundColor: 'rgba(0, 0, 0, 0.8)',
          backdropFilter: 'blur(6px)',
          zIndex: 10000,
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          padding: '1.5rem',
        }}
        onClick={onClose}
      >
        <motion.div
          initial={{ opacity: 0, scale: 0.95, y: 10 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.95, y: 10 }}
          transition={{ duration: 0.2 }}
          className="surface-card"
          style={{
            width: '100%',
            maxWidth: '520px',
            border: '1px solid var(--accent-cyan)',
            boxShadow: '0 20px 50px rgba(0,0,0,0.8)',
            padding: '1.5rem',
          }}
          onClick={(e) => e.stopPropagation()}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
              <span style={{ fontSize: '1.4rem' }}>⚙️</span>
              <h3 style={{ margin: 0, color: 'var(--accent-cyan)', fontSize: '1.1rem' }} className="mono">
                Backend API Connection
              </h3>
            </div>
            <button className="button-dark" onClick={onClose} style={{ padding: '0.3rem 0.6rem' }}>
              ✕
            </button>
          </div>

          <p style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginBottom: '1.2rem' }}>
            Enter your active Cloudflare Tunnel, Localtunnel, or Remote Backend URL below. The frontend will immediately connect to this backend.
          </p>

          <div style={{ marginBottom: '1.2rem' }}>
            <label style={{ display: 'block', color: 'var(--text-muted)', fontSize: '0.75rem', marginBottom: '0.4rem' }}>
              ACTIVE API BACKEND URL
            </label>
            <input
              type="text"
              className="input mono"
              value={inputUrl}
              onChange={(e) => setInputUrl(e.target.value)}
              placeholder="e.g. https://personal-modern-sending-yale.trycloudflare.com"
              style={{ width: '100%', fontSize: '0.85rem', padding: '0.6rem 0.8rem' }}
            />
          </div>

          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '0.8rem' }}>
            <button
              className="button-dark"
              onClick={handleReset}
              style={{ fontSize: '0.8rem', padding: '0.5rem 1rem' }}
            >
              🔄 Reset Default
            </button>

            <div style={{ display: 'flex', gap: '0.6rem' }}>
              <button
                className="button-dark"
                onClick={onClose}
                style={{ fontSize: '0.8rem', padding: '0.5rem 1rem' }}
              >
                Cancel
              </button>
              <button
                className="button"
                onClick={handleSave}
                style={{ fontSize: '0.8rem', padding: '0.5rem 1.2rem', backgroundColor: 'var(--accent-cyan)', color: '#000' }}
              >
                💾 Connect & Save
              </button>
            </div>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  )
}
