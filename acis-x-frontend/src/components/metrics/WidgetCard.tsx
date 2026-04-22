import { useState, type PropsWithChildren, type ReactNode } from 'react'
import { CollapseIcon, ExpandIcon } from '../ui/icons'

type WidgetCardProps = PropsWithChildren<{
  title: string
  actions?: ReactNode
  computing?: boolean
}>

export function WidgetCard({ title, actions, computing = false, children }: WidgetCardProps) {
  const [minimized, setMinimized] = useState(false)

  return (
    <article className="surface-card widget-card">
      <header className="widget-header" onDoubleClick={() => setMinimized((prev) => !prev)}>
        <div className="widget-title-wrap">
          <span className="widget-drag-handle" aria-hidden="true" />
          <h3>{title}</h3>
          {computing ? <small>Computing...</small> : null}
        </div>
        <div className="widget-actions">
          {actions}
          <button className="button-dark icon-button" onClick={() => setMinimized((prev) => !prev)}>
            {minimized ? <ExpandIcon /> : <CollapseIcon />}
          </button>
        </div>
      </header>
      {!minimized ? <div className="widget-body">{children}</div> : null}
    </article>
  )
}
