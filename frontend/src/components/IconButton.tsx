import type { ButtonHTMLAttributes, ReactNode } from 'react'

interface IconButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  label: string
  active?: boolean
  badge?: ReactNode
}

export function IconButton({ label, active, badge, className = '', children, ...props }: IconButtonProps) {
  return (
    <button
      type="button"
      className={`icon-button ${active ? 'is-active' : ''} ${className}`}
      aria-label={label}
      title={label}
      {...props}
    >
      {children}
      {badge !== undefined && <span className="icon-button__badge">{badge}</span>}
    </button>
  )
}

