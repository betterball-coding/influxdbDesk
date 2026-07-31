import { lazy, Suspense, useEffect, useRef, useState } from 'react'
import './App.css'
import { ConnectionDialog } from './components/ConnectionDialog'
import { ConnectionsPage } from './components/ConnectionsPage'
import { MutationPreviewDialog } from './components/MutationPreviewDialog'
import { QueryAssistant } from './components/QueryAssistant'
import { ResultPane } from './components/ResultPane'
import { SchemaPanel } from './components/SchemaPanel'
import { SettingsPage } from './components/SettingsPage'
import { StatusBar } from './components/StatusBar'
import { TaskDrawer } from './components/TaskDrawer'
import { TransferPanel } from './components/TransferPanel'
import { bridge } from './bridge'
import { useWorkbenchStore } from './store'

const QueryEditor = lazy(() => import('./components/QueryEditor').then((module) => ({ default: module.QueryEditor })))

function QueryWorkbench() {
  const assistantOpen = useWorkbenchStore((state) => state.assistantOpen)
  const [schemaWidth, setSchemaWidth] = useState(286)
  const [schemaCollapsed, setSchemaCollapsed] = useState(false)
  const resizing = useRef(false)
  const schemaColumnWidth = schemaCollapsed ? 38 : schemaWidth

  return (
    <div
      className={`workbench-grid${schemaCollapsed ? ' workbench-grid--schema-collapsed' : ''}`}
      style={{ gridTemplateColumns: `${schemaColumnWidth}px ${schemaCollapsed ? 0 : 5}px minmax(0, 1fr)` }}
    >
      <SchemaPanel collapsed={schemaCollapsed} onToggleCollapsed={() => setSchemaCollapsed((current) => !current)} />
      {!schemaCollapsed ? <div
        className="schema-resizer"
        role="separator"
        aria-label="调整资源浏览器宽度"
        aria-orientation="vertical"
        aria-valuemin={220}
        aria-valuemax={420}
        aria-valuenow={schemaWidth}
        tabIndex={0}
        onKeyDown={(event) => {
          if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return
          event.preventDefault()
          setSchemaWidth((current) => Math.max(220, Math.min(420, current + (event.key === 'ArrowLeft' ? -10 : 10))))
        }}
        onPointerDown={(event) => {
          resizing.current = true
          event.currentTarget.setPointerCapture(event.pointerId)
        }}
        onPointerMove={(event) => {
          if (!resizing.current) return
          setSchemaWidth(Math.max(220, Math.min(420, event.clientX)))
        }}
        onPointerUp={(event) => {
          resizing.current = false
          event.currentTarget.releasePointerCapture(event.pointerId)
        }}
        onPointerCancel={() => { resizing.current = false }}
      /> : null}
      <main className="query-workspace">
        <Suspense fallback={<div className="query-editor-placeholder"><span className="query-loader" /></div>}>
          <QueryEditor />
        </Suspense>
        <ResultPane />
      </main>
      {assistantOpen ? <aside className="query-assistant-drawer" aria-label="查询上下文工具"><QueryAssistant /></aside> : null}
    </div>
  )
}

function App() {
  const activeView = useWorkbenchStore((state) => state.activeView)
  const theme = useWorkbenchStore((state) => state.theme)
  const initialize = useWorkbenchStore((state) => state.initialize)

  useEffect(() => {
    document.documentElement.dataset.theme = theme
  }, [theme])

  useEffect(() => {
    void initialize()
  }, [initialize])

  useEffect(() => bridge.subscribeAppCommands((command) => {
    const workbench = useWorkbenchStore.getState()
    switch (command) {
      case 'NEW_QUERY':
        workbench.setActiveView('query')
        workbench.addQueryTab()
        break
      case 'EXECUTE_QUERY':
        workbench.setActiveView('query')
        void workbench.executeQuery()
        break
      case 'SHOW_QUERY':
        workbench.setActiveView('query')
        break
      case 'SHOW_CONNECTIONS':
        workbench.setActiveView('connections')
        break
      case 'SHOW_TASKS':
        workbench.setActiveView('tasks')
        break
      case 'SHOW_SETTINGS':
        workbench.setActiveView('settings')
        break
    }
  }), [])

  return (
    <div className="app-shell">
      <div className="app-content">
        {activeView === 'query' && <QueryWorkbench />}
        {activeView === 'connections' && <ConnectionsPage />}
        {activeView === 'tasks' && <TransferPanel embedded />}
        {activeView === 'settings' && <SettingsPage />}
      </div>
      <StatusBar />
      {activeView !== 'tasks' ? <TaskDrawer /> : null}
      <ConnectionDialog />
      <MutationPreviewDialog />
    </div>
  )
}

export default App
