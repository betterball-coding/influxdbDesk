import { ChevronDown, Clock3, LockKeyhole, PanelRightOpen, UnlockKeyhole } from 'lucide-react'
import { useEffect, useMemo, useState } from 'react'
import { useWorkbenchStore } from '../store'
import { IconButton } from './IconButton'

export function TopBar() {
  const connections = useWorkbenchStore((state) => state.connections)
  const activeConnectionId = useWorkbenchStore((state) => state.activeConnectionId)
  const setActiveConnection = useWorkbenchStore((state) => state.setActiveConnection)
  const protection = useWorkbenchStore((state) => state.protection)
  const unlockUntil = useWorkbenchStore((state) => state.unlockUntil)
  const toggleProtection = useWorkbenchStore((state) => state.toggleProtection)
  const setConnectionDialogOpen = useWorkbenchStore((state) => state.setConnectionDialogOpen)
  const setTaskDrawerOpen = useWorkbenchStore((state) => state.setTaskDrawerOpen)
  const activeTasks = useWorkbenchStore((state) => state.tasks.reduce(
    (count, task) => count + (task.state === 'running' || task.state === 'needs-decision' ? 1 : 0),
    0,
  ))
  const [now, setNow] = useState(Date.now())

  useEffect(() => {
    if (protection !== 'unlocked') return
    const timer = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [protection])

  const remaining = useMemo(() => {
    if (!unlockUntil) return ''
    const seconds = Math.max(0, Math.floor((unlockUntil - now) / 1000))
    return `${String(Math.floor(seconds / 60)).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}`
  }, [now, unlockUntil])

  const active = connections.find((connection) => connection.id === activeConnectionId)

  return (
    <header className="top-bar">
      <div className="connection-picker">
        <span className={`connection-dot connection-dot--${active?.state}`} />
        <span className="connection-picker__copy"><strong>{active?.name ?? '未选择连接'}</strong><small>{active?.defaultDatabase ?? '选择数据源'}</small></span>
        <select
          aria-label="当前连接"
          value={activeConnectionId}
          onChange={(event) => setActiveConnection(event.target.value)}
        >
          {connections.map((connection) => (
            <option key={connection.id} value={connection.id}>
              {connection.name}
            </option>
          ))}
        </select>
        <ChevronDown size={14} aria-hidden="true" />
      </div>
      <button className="top-bar__meta" onClick={() => setConnectionDialogOpen(true, active?.id)}>
        <span>{active?.url}</span>
        {active?.version && <span className="version-chip">v{active.version}</span>}
      </button>
      <div className="top-bar__actions">
        <button
          className={`protection-control protection-control--${protection}`}
          onClick={() => void toggleProtection()}
          disabled={!active}
          aria-label={protection === 'locked' ? '解锁保护模式' : '锁定保护模式'}
        >
          {protection === 'unlocked' ? <UnlockKeyhole size={15} /> : <LockKeyhole size={15} />}
          <span>{protection === 'unlocked' ? '已解锁' : protection === 'locked' ? '保护模式' : '永久只读'}</span>
          {protection === 'unlocked' && (
            <span className="protection-control__timer"><Clock3 size={12} />{remaining}</span>
          )}
        </button>
        <IconButton label="后台任务" badge={activeTasks || undefined} onClick={() => setTaskDrawerOpen(true)}>
          <PanelRightOpen size={18} />
        </IconButton>
      </div>
    </header>
  )
}
