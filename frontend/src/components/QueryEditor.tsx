import {
  AlignLeft,
  Braces,
  LockKeyhole,
  PanelRightClose,
  PanelRightOpen,
  Play,
  Plus,
  Redo2,
  ShieldAlert,
  Square,
  Undo2,
  UnlockKeyhole,
  X,
} from 'lucide-react'
import { forwardRef, useEffect, useImperativeHandle, useRef, useState } from 'react'
// TypeScript's legacy Node resolver cannot read Monaco's exports map; Vite resolves this subpath correctly.
// @ts-expect-error See the comment above.
import * as monaco from 'monaco-editor/editor/editor.api'
import EditorWorker from 'monaco-editor/editor/editor.worker?worker'
import { useWorkbenchStore } from '../store'
import { IconButton } from './IconButton'

type MonacoWorkerEnvironment = typeof self & {
  MonacoEnvironment?: { getWorker: () => Worker }
}

interface MonacoModelHandle {
  getValue: () => string
  setValue: (value: string) => void
  getFullModelRange: () => {
    startLineNumber: number
    startColumn: number
    endLineNumber: number
    endColumn: number
  }
  canUndo: () => boolean
  canRedo: () => boolean
  dispose: () => void
}

interface MonacoEditorHandle {
  executeEdits: (
    source: string,
    edits: Array<{
      range: ReturnType<MonacoModelHandle['getFullModelRange']>
      text: string
      forceMoveMarkers: boolean
    }>,
  ) => boolean
  pushUndoStop: () => boolean
  trigger: (source: string, handlerId: string, payload: unknown) => void
  focus: () => void
}

interface MonacoSurfaceHandle {
  replaceValue: (value: string) => void
  undo: () => void
  redo: () => void
}

interface EditorHistoryState {
  canUndo: boolean
  canRedo: boolean
}

;(self as MonacoWorkerEnvironment).MonacoEnvironment = {
  getWorker: () => new EditorWorker(),
}

let influxQLConfigured = false

function configureInfluxQL() {
  if (influxQLConfigured) return
  influxQLConfigured = true
  monaco.languages.register({ id: 'influxql' })
  monaco.languages.setMonarchTokensProvider('influxql', {
    ignoreCase: true,
    keywords: [
      'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'LIMIT', 'SLIMIT', 'OFFSET', 'SOFFSET',
      'AS', 'AND', 'OR', 'NOT', 'INTO', 'SHOW', 'MEASUREMENTS', 'DATABASES', 'RETENTION', 'POLICIES',
      'FIELD', 'KEYS', 'TAG', 'SERIES', 'EXPLAIN', 'ANALYZE', 'FILL', 'ASC', 'DESC',
    ],
    tokenizer: {
      root: [
        [/"(?:[^"]|"")*"/, 'string.quote'],
        [/'(?:[^'\\]|\\.)*'/, 'string'],
        [/\b\d+(?:\.\d+)?(?:e[+-]?\d+)?\b/i, 'number'],
        [/[a-zA-Z_][\w$]*/, { cases: { '@keywords': 'keyword', '@default': 'identifier' } }],
        [/--.*$/, 'comment'],
        [/[(),.*+\-/=<>]/, 'operator'],
      ],
    },
  })
  monaco.languages.registerCompletionItemProvider('influxql', {
    provideCompletionItems: (model: { getWordUntilPosition: (position: unknown) => { startColumn: number; endColumn: number } }, position: { lineNumber: number }) => {
      const word = model.getWordUntilPosition(position)
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      }
      return {
        suggestions: ['SELECT', 'FROM', 'WHERE', 'GROUP BY time()', 'fill(null)', 'mean()', 'sum()', 'count()'].map((label) => ({
          label,
          kind: monaco.languages.CompletionItemKind.Keyword,
          insertText: label,
          range,
        })),
      }
    },
  })
}

interface MonacoSurfaceProps {
  path: string
  value: string
  theme: 'light' | 'dark'
  onChange: (value: string) => void
  onExecute: () => void
  onCursorChange: (cursor: { line: number; column: number }) => void
  onHistoryChange: (history: EditorHistoryState) => void
}

const MonacoSurface = forwardRef<MonacoSurfaceHandle, MonacoSurfaceProps>(function MonacoSurface({
  path,
  value,
  theme,
  onChange,
  onExecute,
  onCursorChange,
  onHistoryChange,
}, surfaceRef) {
  const containerRef = useRef<HTMLDivElement>(null)
  const modelRef = useRef<MonacoModelHandle | null>(null)
  const editorRef = useRef<MonacoEditorHandle | null>(null)
  const onChangeRef = useRef(onChange)
  const onExecuteRef = useRef(onExecute)
  const onCursorRef = useRef(onCursorChange)
  const onHistoryRef = useRef(onHistoryChange)
  onChangeRef.current = onChange
  onExecuteRef.current = onExecute
  onCursorRef.current = onCursorChange
  onHistoryRef.current = onHistoryChange

  const emitHistory = () => {
    const model = modelRef.current
    onHistoryRef.current({
      canUndo: model?.canUndo() ?? false,
      canRedo: model?.canRedo() ?? false,
    })
  }

  useImperativeHandle(surfaceRef, () => ({
    replaceValue: (nextValue) => {
      const editor = editorRef.current
      const model = modelRef.current
      if (!editor || !model || model.getValue() === nextValue) return
      editor.pushUndoStop()
      editor.executeEdits('influxdesk.format', [{
        range: model.getFullModelRange(),
        text: nextValue,
        forceMoveMarkers: true,
      }])
      editor.pushUndoStop()
      editor.focus()
      emitHistory()
    },
    undo: () => {
      editorRef.current?.trigger('influxdesk.toolbar', 'undo', null)
      editorRef.current?.focus()
      emitHistory()
    },
    redo: () => {
      editorRef.current?.trigger('influxdesk.toolbar', 'redo', null)
      editorRef.current?.focus()
      emitHistory()
    },
  }), [])

  useEffect(() => {
    if (!containerRef.current) return
    configureInfluxQL()
    const uri = monaco.Uri.parse(`inmemory://influxdesk/${encodeURIComponent(path)}.influxql`)
    const model = monaco.editor.createModel(value, 'influxql', uri) as MonacoModelHandle
    modelRef.current = model
    const editor = monaco.editor.create(containerRef.current, {
      model,
      theme: theme === 'dark' ? 'vs-dark' : 'vs',
      ariaLabel: 'InfluxQL 查询',
      automaticLayout: true,
      minimap: { enabled: false },
      fontFamily: 'Cascadia Code, SFMono-Regular, Consolas, monospace',
      fontSize: 12,
      lineHeight: 21,
      lineNumbersMinChars: 3,
      padding: { top: 9, bottom: 20 },
      scrollBeyondLastLine: false,
      smoothScrolling: true,
      renderLineHighlight: 'gutter',
      overviewRulerLanes: 0,
      hideCursorInOverviewRuler: true,
      folding: false,
      wordWrap: 'off',
      tabSize: 2,
      fixedOverflowWidgets: true,
    })
    editorRef.current = editor as MonacoEditorHandle
    const contentDisposable = editor.onDidChangeModelContent(() => {
      onChangeRef.current(model.getValue())
      emitHistory()
    })
    const cursorDisposable = editor.onDidChangeCursorPosition((event: { position: { lineNumber: number; column: number } }) =>
      onCursorRef.current({ line: event.position.lineNumber, column: event.position.column }),
    )
    editor.addAction({
      id: 'influxdesk.execute-query',
      label: '执行查询',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => onExecuteRef.current(),
    })
    editor.focus()
    emitHistory()
    return () => {
      contentDisposable.dispose()
      cursorDisposable.dispose()
      editor.dispose()
      model.dispose()
      modelRef.current = null
      editorRef.current = null
      onHistoryRef.current({ canUndo: false, canRedo: false })
    }
  }, [path])

  useEffect(() => {
    monaco.editor.setTheme(theme === 'dark' ? 'vs-dark' : 'vs')
  }, [theme])

  useEffect(() => {
    const model = modelRef.current
    if (model && model.getValue() !== value) model.setValue(value)
  }, [value])

  return <div className="monaco-editor-host" ref={containerRef} />
})

function formatInfluxQL(query: string): string {
  return query
    .replace(/\s+(FROM|WHERE|GROUP BY|ORDER BY|LIMIT|SLIMIT|OFFSET|SOFFSET)\s+/gi, '\n$1 ')
    .replace(/\s+(AND|OR)\s+/gi, '\n  $1 ')
    .replace(/\b(select|from|where|group by|fill|limit|and|or|as)\b/gi, (token) => token.toUpperCase())
    .trim()
}

export function QueryEditor() {
  const tabs = useWorkbenchStore((state) => state.tabs)
  const activeTabId = useWorkbenchStore((state) => state.activeTabId)
  const schema = useWorkbenchStore((state) => state.schema)
  const queryState = useWorkbenchStore((state) => state.queryState)
  const activeConnectionId = useWorkbenchStore((state) => state.activeConnectionId)
  const connections = useWorkbenchStore((state) => state.connections)
  const mutationPhase = useWorkbenchStore((state) => state.mutationPhase)
  const updateQuery = useWorkbenchStore((state) => state.updateQuery)
  const addQueryTab = useWorkbenchStore((state) => state.addQueryTab)
  const closeQueryTab = useWorkbenchStore((state) => state.closeQueryTab)
  const setActiveTab = useWorkbenchStore((state) => state.setActiveTab)
  const updateQueryDatabase = useWorkbenchStore((state) => state.updateQueryDatabase)
  const executeQuery = useWorkbenchStore((state) => state.executeQuery)
  const cancelQuery = useWorkbenchStore((state) => state.cancelQuery)
  const previewMutation = useWorkbenchStore((state) => state.previewMutation)
  const assistantOpen = useWorkbenchStore((state) => state.assistantOpen)
  const toggleAssistant = useWorkbenchStore((state) => state.toggleAssistant)
  const protection = useWorkbenchStore((state) => state.protection)
  const toggleProtection = useWorkbenchStore((state) => state.toggleProtection)
  const theme = useWorkbenchStore((state) => state.theme)
  const editorRef = useRef<MonacoSurfaceHandle>(null)
  const [cursor, setCursor] = useState({ line: 1, column: 1 })
  const [history, setHistory] = useState<EditorHistoryState>({ canUndo: false, canRedo: false })
  const [retentionPolicyByDatabase, setRetentionPolicyByDatabase] = useState<Record<string, string>>({})
  const activeTab = tabs.find((tab) => tab.id === activeTabId) ?? tabs[0]
  const mutationBusy = mutationPhase === 'previewing' || mutationPhase === 'executing'
  const connectionReady = connections.some((connection) => connection.id === activeConnectionId && connection.state === 'connected')
  const mutationDisabled = !connectionReady || queryState === 'running' || mutationBusy || !activeTab.query.trim()
  const retentionPolicies = schema.find((database) => database.name === activeTab.database)?.retentionPolicies ?? []
  const selectedRetentionPolicy = retentionPolicies.includes(retentionPolicyByDatabase[activeTab.database])
    ? retentionPolicyByDatabase[activeTab.database]
    : retentionPolicies[0] ?? ''

  return (
    <section className="query-editor" aria-label="InfluxQL 编辑器">
      <div className="query-tabs">
        <div className="query-tabs__scroll">
          {tabs.map((tab) => (
            <button key={tab.id} className={`query-tab ${tab.id === activeTabId ? 'is-active' : ''}`} onClick={() => setActiveTab(tab.id)}>
              <Braces size={13} />
              <span>{tab.title}</span>
              {tab.dirty && <i aria-label="未保存" />}
              <span
                role="button"
                tabIndex={0}
                className="query-tab__close"
                aria-label={`关闭 ${tab.title}`}
                onClick={(event) => { event.stopPropagation(); closeQueryTab(tab.id) }}
                onKeyDown={(event) => event.key === 'Enter' && closeQueryTab(tab.id)}
              >
                <X size={12} />
              </span>
            </button>
          ))}
        </div>
        <IconButton label="新建查询" onClick={addQueryTab}><Plus size={15} /></IconButton>
      </div>
      <div className="editor-toolbar">
        <div className="editor-toolbar__primary">
          {queryState === 'running' ? (
            <button className="command-button command-button--danger" onClick={() => void cancelQuery()}><Square size={14} fill="currentColor" /> 停止</button>
          ) : (
            <button className="command-button command-button--primary" onClick={() => void executeQuery()}><Play size={15} fill="currentColor" /> 执行<kbd>Ctrl ↵</kbd></button>
          )}
          <span className="toolbar-separator" />
          <button className="toolbar-button" onClick={() => editorRef.current?.replaceValue(formatInfluxQL(activeTab.query))}><AlignLeft size={14} /> 格式化</button>
          <IconButton label="撤销编辑" onClick={() => editorRef.current?.undo()} disabled={!history.canUndo}><Undo2 size={14} /></IconButton>
          <IconButton label="重做编辑" onClick={() => editorRef.current?.redo()} disabled={!history.canRedo}><Redo2 size={14} /></IconButton>
          <IconButton
            label={protection === 'locked' ? '解锁保护模式' : '锁定保护模式'}
            className={`editor-protection editor-protection--${protection}`}
            onClick={() => void toggleProtection()}
            disabled={!connectionReady}
          >
            {protection === 'unlocked' ? <UnlockKeyhole size={14} /> : <LockKeyhole size={14} />}
          </IconButton>
          <button
            className="toolbar-button toolbar-button--mutation"
            title={mutationDisabled ? '需要已连接且当前没有运行中的查询' : '预览当前语句的变更影响'}
            disabled={mutationDisabled}
            onClick={() => void previewMutation()}
          ><ShieldAlert size={14} /> 预览变更</button>
        </div>
        <div className="editor-toolbar__secondary">
          <label className="compact-select">
            <span>数据库</span>
            <select value={activeTab.database} onChange={(event) => updateQueryDatabase(event.target.value)}>
              {schema.map((database) => <option key={database.name} value={database.name}>{database.name}</option>)}
            </select>
          </label>
          <label className="compact-select compact-select--rp">
            <span>RP</span>
            <select
              aria-label="Retention Policy"
              value={selectedRetentionPolicy}
              onChange={(event) => setRetentionPolicyByDatabase((current) => ({
                ...current,
                [activeTab.database]: event.target.value,
              }))}
            >
              {retentionPolicies.length > 0
                ? retentionPolicies.map((policy) => <option key={policy} value={policy}>{policy}</option>)
                : <option value="">default</option>}
            </select>
          </label>
          <IconButton label={assistantOpen ? '关闭查询辅助器' : '展开查询辅助器'} onClick={toggleAssistant}>
            {assistantOpen ? <PanelRightClose size={16} /> : <PanelRightOpen size={16} />}
          </IconButton>
        </div>
      </div>
      <div className="code-editor-shell monaco-shell">
        <MonacoSurface
          ref={editorRef}
          path={activeTab.id}
          value={activeTab.query}
          theme={theme}
          onChange={updateQuery}
          onExecute={() => { void executeQuery() }}
          onCursorChange={setCursor}
          onHistoryChange={setHistory}
        />
        <div className="editor-language">InfluxQL&nbsp;&nbsp; Ln {cursor.line}, Col {cursor.column}</div>
      </div>
    </section>
  )
}
