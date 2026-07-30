import {
  ArrowRightLeft,
  CircleHelp,
  Database,
  ServerCog,
  Settings2,
  SquareTerminal,
} from 'lucide-react'
import { useWorkbenchStore } from '../store'

export function NavigationRail() {
  const activeView = useWorkbenchStore((state) => state.activeView)
  const setActiveView = useWorkbenchStore((state) => state.setActiveView)
  const activeTasks = useWorkbenchStore((state) => state.tasks.reduce(
    (count, task) => count + (task.state === 'running' || task.state === 'needs-decision' ? 1 : 0),
    0,
  ))

  const navClass = (view: typeof activeView) => `nav-item${activeView === view ? ' is-active' : ''}`

  return (
    <nav className="nav-rail" aria-label="主导航">
      <button className="brand-mark" title="InfluxDesk" onClick={() => setActiveView('query')}>
        <span className="brand-mark__icon"><Database size={22} strokeWidth={1.9} /></span>
        <span className="brand-mark__copy"><strong>InfluxDesk</strong><small>InfluxDB 1.x</small></span>
      </button>
      <div className="nav-rail__primary">
        <button className={navClass('query')} aria-label="查询工作台" onClick={() => setActiveView('query')}><SquareTerminal size={19} /><span>查询</span></button>
        <button className={navClass('connections')} aria-label="连接管理" onClick={() => setActiveView('connections')}><ServerCog size={19} /><span>连接</span></button>
        <button className={navClass('tasks')} aria-label="传输任务" onClick={() => setActiveView('tasks')}>
          <ArrowRightLeft size={19} /><span>传输</span>{activeTasks > 0 ? <b>{activeTasks}</b> : null}
        </button>
      </div>
      <div className="nav-rail__secondary">
        <button className={navClass('settings')} aria-label="设置" onClick={() => setActiveView('settings')}><Settings2 size={18} /><span>设置</span></button>
        <button className="nav-item" aria-label="帮助"><CircleHelp size={18} /><span>帮助</span></button>
      </div>
    </nav>
  )
}
