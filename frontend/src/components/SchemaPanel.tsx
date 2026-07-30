import { observeElementRect, useVirtualizer } from '@tanstack/react-virtual'
import * as DropdownMenu from '@radix-ui/react-dropdown-menu'
import {
  Check,
  ChevronDown,
  ChevronRight,
  CircleGauge,
  Database,
  FolderTree,
  Hash,
  KeyRound,
  PanelLeftClose,
  PanelLeftOpen,
  Pencil,
  Plus,
  RefreshCw,
  Search,
  Server,
  Tag,
} from 'lucide-react'
import { memo, useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from 'react'
import { useWorkbenchStore } from '../store'
import type { SchemaDatabase, SchemaMeasurement } from '../types'
import { IconButton } from './IconButton'

type SchemaTreeRow =
  | { kind: 'root'; key: string }
  | { kind: 'database'; key: string; database: SchemaDatabase }
  | { kind: 'rp-section'; key: string; database: SchemaDatabase }
  | { kind: 'retention-policy'; key: string; policy: string }
  | { kind: 'measurement-section'; key: string; database: SchemaDatabase }
  | { kind: 'measurement'; key: string; database: string; measurement: SchemaMeasurement }
  | { kind: 'field'; key: string; name: string; fieldType: string }
  | { kind: 'tag'; key: string; name: string }

const measurementNodeKey = (database: string, measurement: string) => `${database}\u0000${measurement}`

interface SchemaPanelProps {
  collapsed?: boolean
  onToggleCollapsed?: () => void
}

const MeasurementNode = memo(function MeasurementNode({
  database,
  measurement,
  selected,
  open,
  loading,
  onToggle,
}: {
  database: string
  measurement: SchemaMeasurement
  selected: boolean
  open: boolean
  loading: boolean
  onToggle: (database: string, measurement: string, open: boolean, selected: boolean) => void
}) {
  return (
    <div className="tree-node tree-node--measurement">
      <button
        className={`tree-row tree-row--nested${selected ? ' is-selected' : ''}`}
        aria-expanded={open}
        aria-pressed={selected}
        onClick={() => onToggle(database, measurement.name, open, selected)}
      >
        {open ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
        <CircleGauge size={14} className="tree-icon tree-icon--measurement" />
        <span title={measurement.name}>{measurement.name}</span>
        {loading
          ? <RefreshCw size={11} className="spin" aria-label="正在加载 Measurement Schema" />
          : measurement.fields.length > 0 ? <span className="tree-count">{measurement.fields.length}</span> : null}
      </button>
    </div>
  )
})

export function SchemaPanel({ collapsed = false, onToggleCollapsed }: SchemaPanelProps) {
  const schema = useWorkbenchStore((state) => state.schema)
  const schemaLoading = useWorkbenchStore((state) => state.schemaLoading)
  const loadSchema = useWorkbenchStore((state) => state.loadSchema)
  const selectedMeasurement = useWorkbenchStore((state) => state.selectedMeasurement)
  const measurementSchemaLoading = useWorkbenchStore((state) => state.measurementSchemaLoading)
  const selectMeasurement = useWorkbenchStore((state) => state.selectMeasurement)
  const setConnectionDialogOpen = useWorkbenchStore((state) => state.setConnectionDialogOpen)
  const connections = useWorkbenchStore((state) => state.connections)
  const activeConnectionId = useWorkbenchStore((state) => state.activeConnectionId)
  const setActiveConnection = useWorkbenchStore((state) => state.setActiveConnection)
  const [filter, setFilter] = useState('')
  const deferredFilter = useDeferredValue(filter)
  const [openDatabases, setOpenDatabases] = useState<Set<string>>(() => new Set(['telemetry']))
  const [sections, setSections] = useState<Set<string>>(() => new Set(['telemetry:measurements']))
  const [openMeasurements, setOpenMeasurements] = useState<Set<string>>(() => new Set())
  const treeRef = useRef<HTMLDivElement>(null)
  const activeConnection = connections.find((connection) => connection.id === activeConnectionId)
  const preferredDatabase = useMemo(() => {
    if (activeConnection?.defaultDatabase && schema.some((database) => database.name === activeConnection.defaultDatabase)) {
      return activeConnection.defaultDatabase
    }
    return schema.find((database) => !database.name.startsWith('_'))?.name
      ?? schema[0]?.name
      ?? activeConnection?.defaultDatabase
      ?? ''
  }, [activeConnection?.defaultDatabase, schema])

  useEffect(() => {
    if (!preferredDatabase) return
    setOpenDatabases((current) => {
      if (current.has(preferredDatabase)) return current
      const next = new Set(current)
      next.add(preferredDatabase)
      return next
    })
    setSections((current) => {
      const key = `${preferredDatabase}:measurements`
      if (current.has(key)) return current
      const next = new Set(current)
      next.add(key)
      return next
    })
  }, [activeConnectionId, preferredDatabase])

  const filtered = useMemo(() => {
    const needle = deferredFilter.trim().toLowerCase()
    if (!needle) return schema
    return schema
      .map((database) => ({
        ...database,
        measurements: database.measurements.filter((measurement) =>
          [measurement.name, ...measurement.fields.map((field) => field.name), ...measurement.tags]
            .join(' ')
            .toLowerCase()
            .includes(needle),
        ),
      }))
      .filter((database) => database.name.toLowerCase().includes(needle) || database.measurements.length > 0)
  }, [deferredFilter, schema])

  const searching = deferredFilter.trim() !== ''
  const rows = useMemo(() => {
    const next: SchemaTreeRow[] = [{ kind: 'root', key: 'root' }]
    for (const database of filtered) {
      next.push({ kind: 'database', key: `database:${database.name}`, database })
      const databaseOpen = searching || openDatabases.has(database.name)
      if (!databaseOpen) continue

      const rpKey = `${database.name}:rp`
      next.push({ kind: 'rp-section', key: `section:${rpKey}`, database })
      if (!searching && sections.has(rpKey)) {
        for (const policy of database.retentionPolicies) {
          next.push({ kind: 'retention-policy', key: `rp:${database.name}\u0000${policy}`, policy })
        }
      }

      const measurementKey = `${database.name}:measurements`
      next.push({ kind: 'measurement-section', key: `section:${measurementKey}`, database })
      if (!searching && !sections.has(measurementKey)) continue
      for (const measurement of database.measurements) {
        const nodeKey = measurementNodeKey(database.name, measurement.name)
        next.push({
          kind: 'measurement', key: `measurement:${nodeKey}`,
          database: database.name, measurement,
        })
        if (!openMeasurements.has(nodeKey)) continue
        for (const field of measurement.fields) {
          next.push({
            kind: 'field', key: `field:${nodeKey}\u0000${field.name}`,
            name: field.name, fieldType: field.type,
          })
        }
        for (const tagName of measurement.tags) {
          next.push({ kind: 'tag', key: `tag:${nodeKey}\u0000${tagName}`, name: tagName })
        }
      }
    }
    return next
  }, [filtered, openDatabases, openMeasurements, searching, sections])

  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => treeRef.current,
    observeElementRect: (instance, callback) => observeElementRect(instance, (rect) => callback({
      width: rect.width || 260,
      height: rect.height || 600,
    })),
    estimateSize: () => 27,
    overscan: 12,
    getItemKey: (index) => rows[index]?.key ?? index,
    initialRect: { width: 260, height: 600 },
  })

  const toggleDatabase = useCallback((name: string) => {
    setOpenDatabases((current) => {
      const next = new Set(current)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }, [])

  const toggleSection = useCallback((key: string) => {
    setSections((current) => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }, [])

  const toggleMeasurement = useCallback((database: string, measurement: string, open: boolean, selected: boolean) => {
    const key = measurementNodeKey(database, measurement)
    setOpenMeasurements((current) => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
    if (!open || !selected) void selectMeasurement(database, measurement)
  }, [selectMeasurement])

  const renderRow = (row: SchemaTreeRow) => {
    switch (row.kind) {
      case 'root':
        return (
          <div className="tree-row tree-row--root">
            <Server size={14} />
            <span>{activeConnection?.name ?? '未选择连接'}</span>
            <span className="tree-status">{activeConnection?.state === 'connected' ? '在线' : '离线'}</span>
          </div>
        )
      case 'database': {
        const databaseOpen = searching || openDatabases.has(row.database.name)
        return (
          <button
            className="tree-row"
            aria-expanded={databaseOpen}
            onClick={() => toggleDatabase(row.database.name)}
          >
            {databaseOpen ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            <Database size={14} className="tree-icon tree-icon--database" />
            <span>{row.database.name}</span>
          </button>
        )
      }
      case 'rp-section': {
        const key = `${row.database.name}:rp`
        return (
          <button
            className="tree-row tree-row--nested"
            aria-expanded={sections.has(key)}
            onClick={() => toggleSection(key)}
          >
            {sections.has(key) ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            <KeyRound size={13} />
            <span>Retention Policies</span>
            <span className="tree-count">{row.database.retentionPolicies.length}</span>
          </button>
        )
      }
      case 'retention-policy':
        return <button className="tree-row tree-row--leaf tree-row--deep"><span>{row.policy}</span></button>
      case 'measurement-section': {
        const key = `${row.database.name}:measurements`
        return (
          <button
            className="tree-row tree-row--nested"
            aria-expanded={sections.has(key)}
            onClick={() => toggleSection(key)}
          >
            {sections.has(key) ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            <CircleGauge size={13} />
            <span>Measurements</span>
            <span className="tree-count">{row.database.measurements.length}</span>
          </button>
        )
      }
      case 'measurement': {
        const nodeKey = measurementNodeKey(row.database, row.measurement.name)
        const selected = selectedMeasurement?.database === row.database
          && selectedMeasurement.measurement === row.measurement.name
        return (
          <MeasurementNode
            database={row.database}
            measurement={row.measurement}
            selected={selected}
            open={openMeasurements.has(nodeKey)}
            loading={measurementSchemaLoading && selected}
            onToggle={toggleMeasurement}
          />
        )
      }
      case 'field':
        return (
          <button className="tree-row tree-row--leaf" title={`${row.name} · ${row.fieldType}`}>
            <Hash size={12} /><span>{row.name}</span><small>{row.fieldType}</small>
          </button>
        )
      case 'tag':
        return (
          <button className="tree-row tree-row--leaf" title={`${row.name} · tag`}>
            <Tag size={12} /><span>{row.name}</span><small>tag</small>
          </button>
        )
    }
  }

  if (collapsed) {
    return (
      <aside className="schema-panel schema-panel--collapsed" aria-label="已收起的资源浏览器">
        <IconButton label="展开资源浏览器" onClick={onToggleCollapsed}><PanelLeftOpen size={16} /></IconButton>
        <FolderTree size={15} aria-hidden="true" />
        <span>资源</span>
      </aside>
    )
  }

  return (
    <aside className="schema-panel">
      <div className="panel-heading">
        <div><FolderTree size={16} /><strong>资源浏览器</strong></div>
        {onToggleCollapsed ? <IconButton label="收起资源浏览器" onClick={onToggleCollapsed}><PanelLeftClose size={15} /></IconButton> : null}
      </div>
      <div className="schema-connection-tools">
        <DropdownMenu.Root>
          <DropdownMenu.Trigger asChild>
            <button
              aria-label="当前连接"
              className={`schema-connection-trigger schema-connection-trigger--${activeConnection?.state ?? 'disconnected'}`}
              type="button"
            >
              <Server size={14} />
              <span>
                <strong>{activeConnection?.name ?? '选择连接'}</strong>
                <small>{activeConnection?.state === 'connected' ? '已连接' : '未连接'}</small>
              </span>
              <ChevronDown size={12} />
            </button>
          </DropdownMenu.Trigger>
          <DropdownMenu.Portal>
            <DropdownMenu.Content align="start" className="schema-connection-menu" sideOffset={5}>
              <DropdownMenu.Label className="schema-connection-menu__label">选择连接</DropdownMenu.Label>
              <DropdownMenu.RadioGroup value={activeConnectionId} onValueChange={(id) => {
                if (id !== activeConnectionId) setActiveConnection(id)
              }}>
                {connections.map((connection) => (
                  <DropdownMenu.RadioItem
                    aria-label={`选择连接 ${connection.name}`}
                    className="schema-connection-option"
                    key={connection.id}
                    value={connection.id}
                  >
                    <span className={`connection-option-status connection-option-status--${connection.state}`} />
                    <span className="schema-connection-option__copy">
                      <strong>{connection.name}</strong>
                      <small>{connection.url}</small>
                    </span>
                    <span className="schema-connection-option__meta">
                      <small>{connection.version ?? (connection.state === 'connected' ? '在线' : '离线')}</small>
                      <DropdownMenu.ItemIndicator><Check size={14} /></DropdownMenu.ItemIndicator>
                    </span>
                  </DropdownMenu.RadioItem>
                ))}
              </DropdownMenu.RadioGroup>
            </DropdownMenu.Content>
          </DropdownMenu.Portal>
        </DropdownMenu.Root>
        <div className="panel-heading__actions">
          <IconButton label="新建连接" onClick={() => setConnectionDialogOpen(true)}><Plus size={15} /></IconButton>
          <IconButton label="编辑连接" onClick={() => setConnectionDialogOpen(true, activeConnection?.id)} disabled={!activeConnection}><Pencil size={14} /></IconButton>
          <IconButton
            label="刷新 Schema"
            onClick={() => void loadSchema()}
            className={schemaLoading ? 'is-spinning' : ''}
            disabled={schemaLoading}
          >
            <RefreshCw size={15} />
          </IconButton>
        </div>
      </div>
      <label className="search-box">
        <Search size={14} />
        <input value={filter} onChange={(event) => setFilter(event.target.value)} placeholder="筛选数据库对象" />
        <kbd>Ctrl K</kbd>
      </label>
      <div className="schema-tree" ref={treeRef}>
        <div className="schema-tree__virtual" style={{ height: `${rowVirtualizer.getTotalSize()}px` }}>
          {rowVirtualizer.getVirtualItems().map((virtualRow) => {
            const row = rows[virtualRow.index]
            return (
              <div
                className="schema-tree__virtual-row"
                key={virtualRow.key}
                style={{ height: `${virtualRow.size}px`, transform: `translateY(${virtualRow.start}px)` }}
              >
                {renderRow(row)}
              </div>
            )
          })}
        </div>
      </div>
      <div className="schema-panel__footer">
        <span>{schema.length} databases</span>
        <span>{schema.reduce((total, database) => total + database.measurements.length, 0)} measurements</span>
      </div>
    </aside>
  )
}
