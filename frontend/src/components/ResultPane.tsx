import * as DropdownMenu from '@radix-ui/react-dropdown-menu'
import {
  AlertCircle,
  AlertTriangle,
  BarChart3,
  CheckCircle2,
  ChevronDown,
  Clock3,
  Copy,
  Download,
  Filter,
  ListChecks,
  LoaderCircle,
  Rows3,
  Table2,
} from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'
import { useWorkbenchStore } from '../store'
import { bridge } from '../bridge'
import {
  buildChartProjection,
  formatChartTooltip,
  formatProjectedAxisValue,
  scalarDisplayText,
} from '../chartPrecision'
import { downloadQueryResultCSV, queryResultFilename } from '../queryResultCsv'
import { formatTimestampNs } from '../timestampFormat'
import type { QueryResult, ResultColumn, TypedScalar } from '../types'
import { IconButton } from './IconButton'

const ROW_HEIGHT = 31
const VIEWPORT_HEIGHT = 244
const SELECTION_COLUMN_WIDTH = 36
const MIN_COLUMN_WIDTH = 72
const MAX_COLUMN_WIDTH = 800
const EXACT_SCALAR_KINDS = new Set(['timestamp_ns', 'int64', 'uint64'])

function resultCellDisplayText(value: TypedScalar | undefined): string {
  if (value?.kind === 'timestamp_ns') return formatTimestampNs(value.decimalText)
  return scalarDisplayText(value)
}

function compareScalar(left: TypedScalar | undefined, right: TypedScalar | undefined): number {
  if (!left || left.kind === 'null') return !right || right.kind === 'null' ? 0 : -1
  if (!right || right.kind === 'null') return 1
  if (EXACT_SCALAR_KINDS.has(left.kind) && EXACT_SCALAR_KINDS.has(right.kind)) {
    const leftValue = BigInt('decimalText' in left ? left.decimalText : '0')
    const rightValue = BigInt('decimalText' in right ? right.decimalText : '0')
    return leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0
  }
  return scalarDisplayText(left).localeCompare(scalarDisplayText(right), 'zh-CN', { numeric: true })
}

function defaultColumnWidth(column: ResultColumn): number {
  return column.width ?? 140
}

function clampColumnWidth(width: number): number {
  return Math.min(MAX_COLUMN_WIDTH, Math.max(MIN_COLUMN_WIDTH, Math.round(width)))
}

interface ResultGridProps {
  result: QueryResult
  selectedRowIds?: ReadonlySet<string>
  onSelectedRowsChange?: (rowIds: Set<string>) => void
}

const EMPTY_ROW_SELECTION: ReadonlySet<string> = new Set()

export function ResultGrid({
  result,
  selectedRowIds = EMPTY_ROW_SELECTION,
  onSelectedRowsChange,
}: ResultGridProps) {
  const [scrollTop, setScrollTop] = useState(0)
  const [sort, setSort] = useState<{ key: string; direction: 'asc' | 'desc' }>({ key: 'time', direction: 'asc' })
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>(() => Object.fromEntries(
    result.columns.map((column) => [column.key, defaultColumnWidth(column)]),
  ))
  const resizeRef = useRef<{
    columnKey: string
    pointerId: number
    startWidth: number
    startX: number
  } | null>(null)
  const selectAllRef = useRef<HTMLInputElement>(null)
  const dragSelectionRef = useRef<{
    pointerId: number
    selecting: boolean
    selectedIds: Set<string>
    visitedIds: Set<string>
  } | null>(null)
  const suppressRowClickRef = useRef<Set<string>>(new Set())
  const [resizingColumn, setResizingColumn] = useState<string>()
  const [dragSelecting, setDragSelecting] = useState(false)
  const rows = useMemo(() => {
    return [...result.rows].sort((a, b) => {
      const compared = compareScalar(a.cells[sort.key], b.cells[sort.key])
      return sort.direction === 'asc' ? compared : -compared
    })
  }, [result.rows, sort])
  const start = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - 4)
  const visibleCount = Math.ceil(VIEWPORT_HEIGHT / ROW_HEIGHT) + 8
  const visibleRows = rows.slice(start, start + visibleCount)
  const widths = result.columns.map((column) => columnWidths[column.key] ?? defaultColumnWidth(column))
  const gridTemplate = `${SELECTION_COLUMN_WIDTH}px ${widths.map((width) => `${width}px`).join(' ')}`
  const gridWidth = SELECTION_COLUMN_WIDTH + widths.reduce((sum, width) => sum + width, 0)
  const allRowsSelected = result.rows.length > 0 && result.rows.every((row) => selectedRowIds.has(row.id))
  const someRowsSelected = result.rows.some((row) => selectedRowIds.has(row.id))

  useEffect(() => {
    if (selectAllRef.current) selectAllRef.current.indeterminate = someRowsSelected && !allRowsSelected
  }, [allRowsSelected, someRowsSelected])

  useEffect(() => {
    const finishDragSelection = () => {
      if (!dragSelectionRef.current) return
      dragSelectionRef.current = null
      setDragSelecting(false)
      window.setTimeout(() => suppressRowClickRef.current.clear(), 0)
    }
    window.addEventListener('pointerup', finishDragSelection)
    window.addEventListener('pointercancel', finishDragSelection)
    return () => {
      window.removeEventListener('pointerup', finishDragSelection)
      window.removeEventListener('pointercancel', finishDragSelection)
    }
  }, [])

  const changeSort = (column: ResultColumn) => {
    setSort((current) => ({
      key: column.key,
      direction: current.key === column.key && current.direction === 'asc' ? 'desc' : 'asc',
    }))
  }

  const resizeColumnBy = (column: ResultColumn, delta: number) => {
    setColumnWidths((current) => ({
      ...current,
      [column.key]: clampColumnWidth((current[column.key] ?? defaultColumnWidth(column)) + delta),
    }))
  }

  const startColumnResize = (event: React.PointerEvent<HTMLDivElement>, column: ResultColumn) => {
    event.preventDefault()
    event.stopPropagation()
    resizeRef.current = {
      columnKey: column.key,
      pointerId: event.pointerId,
      startWidth: columnWidths[column.key] ?? defaultColumnWidth(column),
      startX: event.clientX,
    }
    event.currentTarget.setPointerCapture?.(event.pointerId)
    setResizingColumn(column.key)
  }

  const moveColumnResize = (event: React.PointerEvent<HTMLDivElement>) => {
    const resize = resizeRef.current
    if (!resize || resize.pointerId !== event.pointerId) return
    event.preventDefault()
    const width = clampColumnWidth(resize.startWidth + event.clientX - resize.startX)
    setColumnWidths((current) => ({ ...current, [resize.columnKey]: width }))
  }

  const endColumnResize = (event: React.PointerEvent<HTMLDivElement>) => {
    const resize = resizeRef.current
    if (!resize || resize.pointerId !== event.pointerId) return
    resizeRef.current = null
    setResizingColumn(undefined)
  }

  const toggleAllRows = () => {
    const next = new Set(selectedRowIds)
    for (const row of result.rows) {
      if (allRowsSelected) next.delete(row.id)
      else next.add(row.id)
    }
    onSelectedRowsChange?.(next)
  }

  const toggleRow = (rowId: string) => {
    const next = new Set(selectedRowIds)
    if (next.has(rowId)) next.delete(rowId)
    else next.add(rowId)
    onSelectedRowsChange?.(next)
  }

  const applyDragSelection = (rowId: string) => {
    const drag = dragSelectionRef.current
    if (!drag || drag.visitedIds.has(rowId)) return
    drag.visitedIds.add(rowId)
    suppressRowClickRef.current.add(rowId)
    if (drag.selecting) drag.selectedIds.add(rowId)
    else drag.selectedIds.delete(rowId)
    onSelectedRowsChange?.(new Set(drag.selectedIds))
  }

  const startDragSelection = (event: React.PointerEvent<HTMLLabelElement>, rowId: string) => {
    if (event.button !== 0 || event.pointerType === 'touch') return
    event.preventDefault()
    const selectedIds = new Set(selectedRowIds)
    dragSelectionRef.current = {
      pointerId: event.pointerId,
      selecting: !selectedIds.has(rowId),
      selectedIds,
      visitedIds: new Set(),
    }
    setDragSelecting(true)
    applyDragSelection(rowId)
  }

  const extendDragSelection = (event: React.PointerEvent<HTMLLabelElement>, rowId: string) => {
    const drag = dragSelectionRef.current
    if (!drag || drag.pointerId !== event.pointerId || (event.buttons & 1) !== 1) return
    event.preventDefault()
    applyDragSelection(rowId)
  }

  return (
    <div className={`result-grid-wrap${dragSelecting ? ' is-drag-selecting' : ''}`}>
      <div className="result-grid-scroll" onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}>
        <div className="result-grid-header" style={{ gridTemplateColumns: gridTemplate }}>
          <label className="result-grid-select-header" title="选择当前已加载的全部行">
            <input
              aria-label="选择当前已加载的全部行"
              checked={allRowsSelected}
              onChange={toggleAllRows}
              ref={selectAllRef}
              type="checkbox"
            />
          </label>
          {result.columns.map((column) => {
            const width = columnWidths[column.key] ?? defaultColumnWidth(column)
            return (
              <div
                className={`result-grid-header-cell${resizingColumn === column.key ? ' is-resizing' : ''}`}
                key={column.key}
              >
                <button className="result-column-sort" onClick={() => changeSort(column)}>
                  <span>{column.label}</span>
                  <small>{column.kind}</small>
                  {sort.key === column.key && <ChevronDown className={sort.direction === 'asc' ? 'sort-up' : ''} size={12} />}
                </button>
                <div
                  aria-label={`调整 ${column.label} 列宽`}
                  aria-orientation="vertical"
                  aria-valuemax={MAX_COLUMN_WIDTH}
                  aria-valuemin={MIN_COLUMN_WIDTH}
                  aria-valuenow={width}
                  className="result-column-resizer"
                  onDoubleClick={() => setColumnWidths((current) => ({
                    ...current,
                    [column.key]: defaultColumnWidth(column),
                  }))}
                  onKeyDown={(event) => {
                    if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight') return
                    event.preventDefault()
                    resizeColumnBy(column, event.key === 'ArrowLeft' ? -10 : 10)
                  }}
                  onPointerCancel={endColumnResize}
                  onPointerDown={(event) => startColumnResize(event, column)}
                  onPointerMove={moveColumnResize}
                  onPointerUp={endColumnResize}
                  role="separator"
                  tabIndex={0}
                />
              </div>
            )
          })}
        </div>
        <div className="result-grid-body" style={{ height: rows.length * ROW_HEIGHT, minWidth: gridWidth }}>
          {visibleRows.map((row, visibleIndex) => (
            <div
              className="result-grid-row"
              key={row.id}
              style={{ gridTemplateColumns: gridTemplate, transform: `translateY(${(start + visibleIndex) * ROW_HEIGHT}px)` }}
            >
              <label
                className="result-grid-select-cell"
                onClickCapture={(event) => {
                  if (!suppressRowClickRef.current.has(row.id)) return
                  event.preventDefault()
                  event.stopPropagation()
                }}
                onPointerDown={(event) => startDragSelection(event, row.id)}
                onPointerEnter={(event) => extendDragSelection(event, row.id)}
              >
                <input
                  aria-label={`选择结果行 ${row.sourceIndex ?? row.id}`}
                  checked={selectedRowIds.has(row.id)}
                  onChange={() => toggleRow(row.id)}
                  type="checkbox"
                />
              </label>
              {result.columns.map((column) => {
                const value = row.cells[column.key]
                const displayText = resultCellDisplayText(value)
                return (
                  <span
                    key={column.key}
                    className={`result-cell result-cell--${value?.kind ?? 'null'}`}
                    title={displayText}
                  >
                    {displayText}
                  </span>
                )
              })}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

type ExportStatus = {
  sessionId: string
  tone: 'success' | 'error'
  text: string
}

function safeExportError(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error)
  if (message.includes('RESULT_UNAVAILABLE') || message.includes('QUERY_NOT_FOUND')) {
    return '查询结果已过期，请重新执行查询后再导出。'
  }
  if (message.includes('QUERY_RESULT_EXPORT_EMPTY_SELECTION')) return '请先选择要导出的行。'
  return '导出失败，请稍后重试。'
}

export function ResultChart({ result }: { result: QueryResult }) {
  const chartRef = useRef<HTMLDivElement>(null)
  const [allowApproximate, setAllowApproximate] = useState(false)
  const projection = useMemo(
    () => buildChartProjection(result, allowApproximate),
    [allowApproximate, result],
  )

  useEffect(() => {
    setAllowApproximate(false)
  }, [result.sessionId, result.seriesName, result.statementId])

  useEffect(() => {
    if (!chartRef.current || projection.status === 'blocked' || !projection.xAxis || !projection.yAxis) return
    const xAxis = projection.xAxis
    const yAxis = projection.yAxis
    let disposed = false
    let cleanup = () => undefined
    void Promise.all([
      import('echarts/core'),
      import('echarts/charts'),
      import('echarts/components'),
      import('echarts/renderers'),
    ]).then(([core, charts, components, renderers]) => {
      if (disposed || !chartRef.current) return
      core.use([charts.LineChart, components.GridComponent, components.TooltipComponent, renderers.CanvasRenderer])
      const chart = core.init(chartRef.current, undefined, { renderer: 'canvas' })
      chart.setOption({
        animationDuration: 280,
        grid: { left: 52, right: 22, top: 24, bottom: 42 },
        tooltip: {
          trigger: 'axis',
          confine: true,
          formatter: (items: unknown) => {
            const list = Array.isArray(items) ? items : [items]
            const item = list[0] as { dataIndex?: number } | undefined
            return formatChartTooltip(result, projection, item?.dataIndex ?? -1)
          },
        },
        xAxis: {
          type: 'value',
          axisLabel: {
            color: '#788491',
            hideOverlap: true,
            formatter: (value: number) => formatProjectedAxisValue(xAxis, value),
          },
          axisLine: { lineStyle: { color: '#d7dde2' } },
          axisTick: { show: false },
        },
        yAxis: {
          type: 'value',
          axisLabel: {
            color: '#788491',
            hideOverlap: true,
            formatter: (value: number) => formatProjectedAxisValue(yAxis, value),
          },
          splitLine: { lineStyle: { color: '#edf0f2' } },
        },
        series: [
          {
            type: 'line',
            name: yAxis.column.label,
            showSymbol: false,
            smooth: 0.12,
            lineStyle: { color: '#087f79', width: 2 },
            areaStyle: { color: 'rgba(8, 127, 121, 0.08)' },
            data: projection.points.map((point) => [point.x, point.y]),
          },
        ],
      })
      const observer = new ResizeObserver(() => chart.resize())
      observer.observe(chartRef.current)
      cleanup = () => {
        observer.disconnect()
        chart.dispose()
      }
    })
    return () => {
      disposed = true
      cleanup()
    }
  }, [projection, result])

  const precisionToggle = (projection.approximateAvailable || allowApproximate) ? (
    <label className="chart-precision-toggle">
      <input
        type="checkbox"
        checked={allowApproximate}
        onChange={(event) => setAllowApproximate(event.currentTarget.checked)}
      />
      <span>近似投影</span>
    </label>
  ) : null

  if (projection.status === 'blocked') {
    const unsafeRange = projection.reason === 'UNSAFE_X_DELTA' || projection.reason === 'UNSAFE_Y_DELTA'
    return (
      <div className="chart-precision-guard" role="status">
        <AlertTriangle size={22} />
        <strong>{unsafeRange ? '数值范围超出精确绘图边界' : '当前结果无法生成数值图表'}</strong>
        <span>{unsafeRange ? '图表已停止，原始 decimalText 未被转换。' : '请选择同时包含数值横轴与纵轴的结果。'}</span>
        {precisionToggle}
      </div>
    )
  }

  return (
    <div className={`result-chart-shell${projection.status === 'approximate' ? ' result-chart-shell--warning' : ''}`}>
      {projection.status === 'approximate' && (
        <div className="chart-precision-warning" role="status">
          <AlertTriangle size={14} />
          <span>近似投影已启用，坐标可能合并；悬浮值始终显示原始 decimalText。</span>
          {precisionToggle}
        </div>
      )}
      <div className="result-chart" ref={chartRef} aria-label="查询结果折线图" />
    </div>
  )
}

function EmptyResult({ state, error }: { state: string; error?: string }) {
  if (state === 'running') {
    return (
      <div className="result-empty result-empty--loading">
        <span className="query-loader" />
        <strong>正在执行查询</strong>
        <span>等待第一个数据分片…</span>
      </div>
    )
  }
  if (error) {
    return (
      <div className="result-empty result-empty--error">
        <AlertCircle size={22} />
        <strong>查询未执行</strong>
        <span>{error}</span>
      </div>
    )
  }
  return (
    <div className="result-empty">
      <Rows3 size={22} />
      <strong>准备就绪</strong>
      <span>执行查询后，结果会显示在这里。</span>
    </div>
  )
}

export function ResultPane() {
  const result = useWorkbenchStore((state) => state.result)
  const queryState = useWorkbenchStore((state) => state.queryState)
  const queryError = useWorkbenchStore((state) => state.queryError)
  const [view, setView] = useState<'table' | 'chart' | 'messages'>('table')
  const [rowSelection, setRowSelection] = useState<{ sessionId: string; ids: Set<string> }>({
    sessionId: '',
    ids: new Set(),
  })
  const [exporting, setExporting] = useState(false)
  const [exportStatus, setExportStatus] = useState<ExportStatus>()
  const selectedRowIds = rowSelection.sessionId === result?.sessionId ? rowSelection.ids : EMPTY_ROW_SELECTION
  const selectedRows = result?.rows.filter((row) => selectedRowIds.has(row.id)) ?? []
  const visibleExportStatus = exportStatus?.sessionId === result?.sessionId ? exportStatus : undefined

  useEffect(() => {
    if (!visibleExportStatus) return
    const timer = window.setTimeout(() => setExportStatus(undefined), 3500)
    return () => window.clearTimeout(timer)
  }, [visibleExportStatus])

  const exportRows = async (allRows: boolean) => {
    if (!result || exporting || !allRows && selectedRows.length === 0) return
    setExporting(true)
    setExportStatus(undefined)
    try {
      let rowCount: string
      if (bridge.isNative()) {
        if (!result.seriesId) throw new Error('QUERY_RESULT_EXPORT_SERIES_NOT_FOUND')
        const response = await bridge.exportQueryResult({
          sessionId: result.sessionId,
          statementId: result.statementId,
          seriesId: result.seriesId,
          allRows,
          rowIndexes: allRows ? undefined : selectedRows.map((row) => row.sourceIndex ?? '0'),
          suggestedName: queryResultFilename(result.seriesName),
        })
        rowCount = response.rowCount
      } else {
        const rows = allRows ? result.rows : selectedRows
        downloadQueryResultCSV(result, rows)
        rowCount = String(rows.length)
      }
      setExportStatus({ sessionId: result.sessionId, tone: 'success', text: `已导出 ${rowCount} 行查询结果。` })
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error)
      if (!message.includes('QUERY_RESULT_EXPORT_CANCELED')) {
        setExportStatus({ sessionId: result.sessionId, tone: 'error', text: safeExportError(error) })
      }
    } finally {
      setExporting(false)
    }
  }

  return (
    <section className="result-pane" aria-label="查询结果">
      <div className="result-tabs-bar">
        <div className="result-tabs" role="tablist">
          <button className={view === 'table' ? 'is-active' : ''} onClick={() => setView('table')}>
            <Table2 size={14} /> 数据
            {result && <span>{result.rows.length}</span>}
          </button>
          <button className={view === 'chart' ? 'is-active' : ''} onClick={() => setView('chart')}>
            <BarChart3 size={14} /> 图表
          </button>
          <button className={view === 'messages' ? 'is-active' : ''} onClick={() => setView('messages')}>
            <AlertCircle size={14} /> 消息
            {queryError && <i />}
          </button>
        </div>
        <div className="result-actions">
          {result && (
            <>
              {bridge.isNative() && (
                <span className="result-window-disclosure" title="当前桥接只加载首个 series 的首个结果页">
                  S{result.statementId} · 首个 series: {result.seriesName} · 首页 ≤5000 行
                </span>
              )}
              <span className="result-stat"><Clock3 size={12} />{result.elapsedMs} ms</span>
              <span className="result-stat">{(result.scannedBytes / 1024).toFixed(1)} KiB</span>
              <IconButton label="筛选结果"><Filter size={15} /></IconButton>
              <IconButton label="复制结果"><Copy size={15} /></IconButton>
              <DropdownMenu.Root>
                <DropdownMenu.Trigger asChild>
                  <button aria-label="导出结果" className="icon-button" disabled={exporting} title="导出结果" type="button">
                    {exporting ? <LoaderCircle className="spin" size={15} /> : <Download size={15} />}
                  </button>
                </DropdownMenu.Trigger>
                <DropdownMenu.Portal>
                  <DropdownMenu.Content align="end" className="result-export-menu" sideOffset={5}>
                    <DropdownMenu.Label className="result-export-menu-label">导出范围</DropdownMenu.Label>
                    <DropdownMenu.Item
                      className="result-export-menu-item"
                      disabled={selectedRows.length === 0 || exporting}
                      onSelect={() => void exportRows(false)}
                    >
                      <ListChecks size={15} />
                      <span><strong>导出所选行</strong><small>{selectedRows.length > 0 ? `${selectedRows.length} 行` : '请先勾选结果行'}</small></span>
                    </DropdownMenu.Item>
                    <DropdownMenu.Item
                      className="result-export-menu-item"
                      disabled={exporting}
                      onSelect={() => void exportRows(true)}
                    >
                      <Table2 size={15} />
                      <span><strong>导出全部查询结果</strong><small>{result.totalRows ?? String(result.rows.length)} 行</small></span>
                    </DropdownMenu.Item>
                  </DropdownMenu.Content>
                </DropdownMenu.Portal>
              </DropdownMenu.Root>
            </>
          )}
        </div>
      </div>
      {visibleExportStatus && (
        <div className={`result-export-status result-export-status--${visibleExportStatus.tone}`} role="status">
          {visibleExportStatus.tone === 'success' ? <CheckCircle2 size={14} /> : <AlertCircle size={14} />}
          <span>{visibleExportStatus.text}</span>
        </div>
      )}
      <div className="result-content">
        {!result ? <EmptyResult state={queryState} error={queryError} /> : view === 'table' ? (
          <ResultGrid
            key={result.sessionId}
            onSelectedRowsChange={(ids) => setRowSelection({ sessionId: result.sessionId, ids })}
            result={result}
            selectedRowIds={selectedRowIds}
          />
        ) : view === 'chart' ? (
          <ResultChart result={result} />
        ) : (
          <div className="messages-view">
            <div><span className="message-level message-level--success">SUCCESS</span><strong>查询完成</strong><time>{result.elapsedMs} ms</time></div>
            <p>Statement 0 返回 {result.rows.length} 行，结果完整。</p>
            <p className="message-muted">精确整数与纳秒时间以 tagged decimal 形式保留。</p>
          </div>
        )}
      </div>
    </section>
  )
}
