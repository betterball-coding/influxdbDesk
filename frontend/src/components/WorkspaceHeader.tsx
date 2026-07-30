import { Database } from 'lucide-react'
import { useWorkbenchStore } from '../store'

const viewLabels = {
  query: '查询工作台',
  connections: '连接管理',
  tasks: '数据传输',
  settings: '设置',
} as const

export function WorkspaceHeader() {
  const activeView = useWorkbenchStore((state) => state.activeView)
  const setActiveView = useWorkbenchStore((state) => state.setActiveView)
  const connections = useWorkbenchStore((state) => state.connections)
  const activeConnectionId = useWorkbenchStore((state) => state.activeConnectionId)
  const tabs = useWorkbenchStore((state) => state.tabs)
  const activeTabId = useWorkbenchStore((state) => state.activeTabId)
  const activeConnection = connections.find((connection) => connection.id === activeConnectionId)
  const activeTab = tabs.find((tab) => tab.id === activeTabId)
  const context = activeView === 'query'
    ? [activeConnection?.name, activeTab?.database, activeTab?.title].filter(Boolean).join(' / ')
    : viewLabels[activeView]

  return (
    <header className="workspace-header">
      <button className="workspace-brand" aria-label="查询工作台" onClick={() => setActiveView('query')}>
        <Database size={17} />
        <strong>InfluxDesk</strong>
      </button>
      <div className="workspace-context" title={context}>{context}</div>
    </header>
  )
}
