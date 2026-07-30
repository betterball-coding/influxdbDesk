import {
  ArrowLeftRight,
  CheckCircle2,
  Cloud,
  Database,
  HardDrive,
  Link2,
  ListChecks,
  LockKeyhole,
  Settings,
} from 'lucide-react'
import { bridge } from '../bridge'
import { useWorkbenchStore } from '../store'
import { IconButton } from './IconButton'

export function StatusBar() {
  const activeView = useWorkbenchStore((state) => state.activeView)
  const setActiveView = useWorkbenchStore((state) => state.setActiveView)
  const setTaskDrawerOpen = useWorkbenchStore((state) => state.setTaskDrawerOpen)
  const queryState = useWorkbenchStore((state) => state.queryState)
  const result = useWorkbenchStore((state) => state.result)
  const protection = useWorkbenchStore((state) => state.protection)
  const activeTaskCount = useWorkbenchStore((state) => state.tasks.filter((task) => task.state === 'running').length)

  const openTaskDrawer = () => {
    if (activeView === 'tasks') setActiveView('query')
    setTaskDrawerOpen(true)
  }

  return (
    <footer className="status-bar">
      <nav className="status-nav" aria-label="工作区导航">
        <IconButton label="查询工作台" active={activeView === 'query'} onClick={() => setActiveView('query')}><Database size={14} /></IconButton>
        <IconButton label="连接管理" active={activeView === 'connections'} onClick={() => setActiveView('connections')}><Link2 size={14} /></IconButton>
        <IconButton label="传输任务" active={activeView === 'tasks'} onClick={() => setActiveView('tasks')}><ArrowLeftRight size={14} /></IconButton>
        <IconButton label="设置" active={activeView === 'settings'} onClick={() => setActiveView('settings')}><Settings size={14} /></IconButton>
      </nav>
      <div className="status-summary">
        <span className="status-ready"><CheckCircle2 size={12} />就绪</span>
        <span className="status-bridge">{bridge.isNative() ? 'Wails bridge' : 'Mock bridge'}</span>
        {result && <span>{result.rows.length} rows</span>}
        <span className="status-capacity"><Cloud size={12} />{queryState === 'running' ? '查询中' : '4 slots available'}</span>
        <span className="status-cache"><HardDrive size={12} />缓存 0.8 MiB</span>
        <span><LockKeyhole size={12} />{protection === 'unlocked' ? '写入已解锁' : '写入受保护'}</span>
        <IconButton label="后台任务" badge={activeTaskCount || undefined} onClick={openTaskDrawer}><ListChecks size={14} /></IconButton>
      </div>
    </footer>
  )
}
