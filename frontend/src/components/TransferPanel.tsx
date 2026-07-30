import {
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  Download,
  FileUp,
  FolderOpen,
  LoaderCircle,
  Plus,
  RefreshCw,
  RotateCcw,
  ShieldAlert,
  Trash2,
  X,
} from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { bridge } from '../bridge'
import { useWorkbenchStore } from '../store'
import { TaskEventSynchronizer } from '../taskEventSync'
import type {
  CSVFieldKind,
  CSVMapping,
  ExportJob,
  ImportAfterResolution,
  ImportFormat,
  ImportIncidentDecision,
  ImportJob,
  ImportRunPreview,
  ImportRunRequest,
  ImportSourceInspection,
  ProtectionState,
  SchemaDatabase,
  TransferTask,
} from '../types'
import { IconButton } from './IconButton'

type PanelMode = 'overview' | 'import' | 'export'
type Selection = { kind: 'import' | 'export'; id: string }

const SAFE_ERRORS: Record<string, string> = {
  IMPORT_SOURCE_PATH_UNAVAILABLE: '当前桌面构建无法取得所选文件的本机路径，预检未执行。',
  IMPORT_SOURCE_INSPECTION_REQUIRES_NATIVE: '大文件必须由后端文件检查接口读取，浏览器不会加载整份文件。',
  IMPORT_SOURCE_DIGEST_UNAVAILABLE: '当前运行环境无法安全校验源文件，预检未执行。',
  TRANSFER_JOB_LIMIT_GLOBAL: '全局保留任务已达上限，请先清理已有任务。',
  TRANSFER_JOB_LIMIT_PROFILE: '当前连接的保留任务已达上限。',
  PRIVATE_TRANSFER_QUOTA_EXCEEDED: '应用私有传输空间不足。',
  TARGET_VOLUME_LOW_SPACE: '目标卷可用空间不足。',
  REVISION_CONFLICT: '任务状态已变化，请刷新后重试。',
  IMPORT_STATE_CONFLICT: '当前任务状态不允许此操作。',
  EXPORT_STATE_CONFLICT: '当前导出状态不允许此操作。',
  IMPORT_RUN_GRANT_MISMATCH: '运行授权已失效，请重新预览。',
  IMPORT_RUN_GRANT_EXPIRED: '运行授权已过期，请重新预览。',
}

function safeError(error: unknown) {
  const message = error instanceof Error ? error.message : ''
  const code = Object.keys(SAFE_ERRORS).find((candidate) => message.includes(candidate))
  return code ? SAFE_ERRORS[code] : '任务请求未完成，请刷新任务快照后重试。'
}

function selectionCanceled(error: unknown) {
  return error instanceof Error && error.message.includes('_SELECTION_CANCELED')
}

function stateLabel(state: string) {
  return state.replaceAll('_', ' ')
}

function transferState(state: string, terminal: boolean): TransferTask['state'] {
  if (state.startsWith('NEEDS_')) return 'needs-decision'
  if (state.startsWith('PAUSED_') || state === 'READY' || state === 'QUEUED') return 'paused'
  if (terminal) return state === 'SUCCEEDED' ? 'completed' : 'failed'
  if (state === 'FAILED' || state === 'ABORTED') return 'failed'
  return 'running'
}

function summaries(imports: ImportJob[], exports: ExportJob[]): TransferTask[] {
  return [
    ...imports.map((job) => ({
      id: job.task.id,
      kind: 'import' as const,
      title: `导入 ${job.task.id.slice(0, 8)}`,
      subtitle: stateLabel(job.task.state),
      progress: job.task.state === 'SUCCEEDED' ? 100 : 0,
      processed: job.checkpoint.logicalOffset,
      total: '—',
      state: transferState(job.task.state, job.task.terminal),
      updatedAt: job.task.updatedAt,
      revision: job.task.stateRevision,
      publicSafeMessage: job.task.publicSafeMessage,
    })),
    ...exports.map((job) => ({
      id: job.task.id,
      kind: 'export' as const,
      title: `导出 ${job.task.id.slice(0, 8)}`,
      subtitle: stateLabel(job.task.state),
      progress: job.task.state === 'SUCCEEDED' ? 100 : 0,
      processed: String(job.fragments?.filter((fragment) => fragment.state === 'COMPLETE').length ?? 0),
      total: String(job.fragments?.length ?? 0),
      state: transferState(job.task.state, job.task.terminal),
      updatedAt: job.task.updatedAt,
      revision: job.task.stateRevision,
      publicSafeMessage: job.task.publicSafeMessage,
    })),
  ]
}

function ProtectionBanner({ protection }: { protection: ProtectionState }) {
  const unlocked = protection === 'unlocked'
  return (
    <div className={`transfer-protection transfer-protection--${protection}`}>
      <ShieldAlert size={15} />
      <span>
        <strong>{unlocked ? '写入保护已解锁' : protection === 'permanent-readonly' ? '连接永久只读' : '写入保护已锁定'}</strong>
        <small>{unlocked ? '可预览并签发一次性 ImportRunGrant' : '导出可用；启动、继续和重放导入需要先解锁'}</small>
      </span>
    </div>
  )
}

function TaskList({ imports, exports, selected, onSelect }: {
  imports: ImportJob[]
  exports: ExportJob[]
  selected?: Selection
  onSelect: (selection: Selection) => void
}) {
  const jobs = [
    ...imports.map((job) => ({ kind: 'import' as const, job, task: job.task })),
    ...exports.map((job) => ({ kind: 'export' as const, job, task: job.task })),
  ].sort((left, right) => right.task.updatedAt.localeCompare(left.task.updatedAt))
  if (jobs.length === 0) return <div className="transfer-empty">当前连接没有保留的传输任务</div>
  return (
    <div className="transfer-job-list">
      {jobs.map(({ kind, task }) => (
        <button
          key={`${kind}:${task.id}`}
          className={selected?.kind === kind && selected.id === task.id ? 'is-selected' : ''}
          onClick={() => onSelect({ kind, id: task.id })}
        >
          <span className={`transfer-job-icon transfer-job-icon--${kind}`}>
            {kind === 'import' ? <FileUp size={15} /> : <Download size={15} />}
          </span>
          <span className="transfer-job-copy">
            <strong>{kind === 'import' ? '数据导入' : '逻辑导出'}</strong>
            <small>{task.id}</small>
          </span>
          <span className={`transfer-state transfer-state--${transferState(task.state, task.terminal)}`}>{stateLabel(task.state)}</span>
          <code>rev {task.stateRevision}</code>
        </button>
      ))}
    </div>
  )
}

function TaskMetaStrip({ job }: { job: ImportJob | ExportJob }) {
  return (
    <dl className="transfer-meta-strip">
      <div><dt>状态</dt><dd>{stateLabel(job.task.state)}</dd></div>
      <div><dt>State revision</dt><dd>{job.task.stateRevision}</dd></div>
      <div><dt>Snapshot revision</dt><dd>{job.task.snapshotRevision}</dd></div>
      <div><dt>更新时间</dt><dd>{new Date(job.task.updatedAt).toLocaleString('zh-CN')}</dd></div>
    </dl>
  )
}

function RunPreview({ preview }: { preview: ImportRunPreview }) {
  return (
    <div className="transfer-preview">
      <CheckCircle2 size={15} />
      <span>
        <strong>授权预览已就绪</strong>
        <small>checkpoint {preview.logicalOffset} · 下一批最多 {preview.adaptiveMaxPoints} 点 / {preview.adaptiveMaxBytes} 字节</small>
      </span>
    </div>
  )
}

function ImportDetail({ job, profileId, protection, onChanged }: {
  job: ImportJob
  profileId: string
  protection: ProtectionState
  onChanged: (job: ImportJob) => void
}) {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string>()
  const [preview, setPreview] = useState<{ request: ImportRunRequest; value: ImportRunPreview }>()
  const incident = job.openIncident
  const [decision, setDecision] = useState<ImportIncidentDecision>('REPLAY_EXACT')
  const [afterResolution, setAfterResolution] = useState<ImportAfterResolution>('PAUSE')

  const needsGrant = (candidate: ImportIncidentDecision, after: ImportAfterResolution) =>
    candidate === 'REPLAY_EXACT' || ((candidate === 'ASSUME_COMMITTED' || candidate === 'ACCEPT_PARTIAL') && after === 'CONTINUE')

  const run = async (action: () => Promise<ImportJob>) => {
    setBusy(true)
    setError(undefined)
    try {
      onChanged(await action())
      setPreview(undefined)
    } catch (cause) {
      setError(safeError(cause))
    } finally {
      setBusy(false)
    }
  }

  const previewAction = async (action: 'START' | 'RESUME') => {
    if (protection !== 'unlocked') return setError('请先解锁写入保护，再签发运行授权。')
    const request: ImportRunRequest = {
      jobId: job.task.id,
      commandRequestId: globalThis.crypto.randomUUID(),
      expectedStateRevision: job.task.stateRevision,
      action,
    }
    setBusy(true)
    setError(undefined)
    try {
      const value = await bridge.previewImportRun(profileId, request)
      if (!value.executable || !value.importRunGrant?.token) throw new Error('IMPORT_RUN_GRANT_MISMATCH')
      setPreview({ request, value })
    } catch (cause) {
      setError(safeError(cause))
    } finally {
      setBusy(false)
    }
  }

  const startPreviewed = () => {
    const action = preview?.request.action
    if (!preview?.value.importRunGrant?.token ||
      (action !== 'START' && action !== 'RESUME')) return
    const request = preview.request
    const grant = preview.value.importRunGrant.token
    const authorized = {
      jobId: request.jobId,
      commandRequestId: request.commandRequestId,
      expectedStateRevision: request.expectedStateRevision,
      action,
      importRunGrant: grant,
    }
    setPreview(undefined)
    void run(() => authorized.action === 'START'
      ? bridge.startImport(profileId, authorized)
      : bridge.resumeImport(profileId, authorized))
  }

  const previewResolution = async () => {
    if (!incident) return
    if (needsGrant(decision, afterResolution) && protection !== 'unlocked') {
      return setError('该事故决策会继续发包，请先解锁写入保护。')
    }
    const request: ImportRunRequest = {
      jobId: job.task.id,
      commandRequestId: globalThis.crypto.randomUUID(),
      expectedStateRevision: job.task.stateRevision,
      action: 'RESOLVE',
      decision,
      ...(decision === 'ABORT' ? {} : { afterResolution }),
    }
    setBusy(true)
    setError(undefined)
    try {
      const value = await bridge.previewImportRun(profileId, request)
      if (!value.executable || (needsGrant(decision, afterResolution) && !value.importRunGrant?.token)) {
        throw new Error('IMPORT_RUN_GRANT_MISMATCH')
      }
      setPreview({ request, value })
    } catch (cause) {
      setError(safeError(cause))
    } finally {
      setBusy(false)
    }
  }

  const resolvePreviewed = () => {
    if (!incident || !preview || preview.request.action !== 'RESOLVE' || !preview.request.decision) return
    const request = preview.request
    const token = preview.value.importRunGrant?.token
    setPreview(undefined)
    void run(() => bridge.resolveImportBatch(profileId, {
      jobId: request.jobId,
      commandRequestId: request.commandRequestId,
      expectedStateRevision: request.expectedStateRevision,
      incidentId: incident.incidentId,
      parentCheckpointDigest: incident.parentCheckpointDigest,
      decision: request.decision!,
      afterResolution: request.afterResolution,
      importRunGrant: token,
    }))
  }

  const command = (kind: 'cancel' | 'cleanup') => void run(() => {
    const envelope = { commandRequestId: globalThis.crypto.randomUUID(), expectedStateRevision: job.task.stateRevision }
    return kind === 'cancel' ? bridge.cancelImport(job.task.id, envelope) : bridge.cleanupImport(job.task.id, envelope)
  })

  return (
    <div className="transfer-detail">
      <header><div><span className="eyebrow">IMPORT JOB</span><h3>{job.task.id}</h3></div></header>
      <TaskMetaStrip job={job} />
      <section className="transfer-detail-section">
        <h4>Checkpoint</h4>
        <dl className="transfer-definition-grid">
          <div><dt>Sequence</dt><dd>{job.checkpoint.sequence}</dd></div>
          <div><dt>Logical offset</dt><dd>{job.checkpoint.logicalOffset}</dd></div>
          <div><dt>Max points</dt><dd>{job.checkpoint.adaptiveMaxPoints}</dd></div>
          <div><dt>Max bytes</dt><dd>{job.checkpoint.adaptiveMaxBytes}</dd></div>
        </dl>
      </section>

      {incident && (
        <section className="transfer-detail-section transfer-incident">
          <h4><AlertTriangle size={14} />待人工决策 · {incident.kind}</h4>
          <p>Batch {incident.batchId} · offset {incident.startOffset} → {incident.endOffset}</p>
          <div className="decision-grid" role="group" aria-label="事故决策">
            {(['REPLAY_EXACT', 'ASSUME_COMMITTED', 'ACCEPT_PARTIAL', 'ABORT'] as const).map((item) => {
              const allowed = item === 'REPLAY_EXACT' || item === 'ABORT' ||
                (item === 'ASSUME_COMMITTED' && incident.kind === 'UNKNOWN') ||
                (item === 'ACCEPT_PARTIAL' && incident.kind === 'PARTIAL')
              return <button key={item} disabled={!allowed} className={decision === item ? 'is-active' : ''} onClick={() => { setDecision(item); setPreview(undefined) }}>{item}</button>
            })}
          </div>
          {decision !== 'ABORT' && (
            <div className="transfer-segmented" aria-label="决策后动作">
              <button className={afterResolution === 'PAUSE' ? 'is-active' : ''} onClick={() => { setAfterResolution('PAUSE'); setPreview(undefined) }}>处理后暂停</button>
              <button className={afterResolution === 'CONTINUE' ? 'is-active' : ''} onClick={() => { setAfterResolution('CONTINUE'); setPreview(undefined) }}>处理后继续</button>
            </div>
          )}
          {preview?.request.action === 'RESOLVE' ? <RunPreview preview={preview.value} /> : null}
          <div className="transfer-actions">
            <button className="secondary-button" disabled={busy} onClick={() => void previewResolution()}>预览处置</button>
            <button className="primary-button" disabled={busy || preview?.request.action !== 'RESOLVE'} onClick={resolvePreviewed}>提交处置</button>
          </div>
        </section>
      )}

      {!incident && (job.task.state === 'READY' || job.task.state === 'PAUSED_SAFE') && (
        <section className="transfer-detail-section">
          <h4>运行区段授权</h4>
          {preview ? <RunPreview preview={preview.value} /> : <p>启动或继续前将重新校验 lease、revision 和 checkpoint。</p>}
          <div className="transfer-actions">
            <button className="secondary-button" disabled={busy || protection !== 'unlocked'} onClick={() => void previewAction(job.task.state === 'READY' ? 'START' : 'RESUME')}>预览运行</button>
            <button className="primary-button" disabled={busy || !preview} onClick={startPreviewed}>{job.task.state === 'READY' ? '启动导入' : '继续导入'}</button>
          </div>
        </section>
      )}

      {job.task.state === 'RUNNING' && <p className="transfer-pending"><LoaderCircle size={14} />正在按持久 checkpoint 连续导入…</p>}

      <div className="transfer-actions transfer-actions--footer">
        {!job.task.terminal && bridge.supportsTransferMethod('CancelImport') && <button className="secondary-button" disabled={busy} onClick={() => command('cancel')}>取消任务</button>}
        {job.task.terminal && bridge.supportsTransferMethod('CleanupImport') && <button className="secondary-button" disabled={busy} onClick={() => command('cleanup')}><Trash2 size={14} />清理文件</button>}
      </div>
      {busy && <p className="transfer-pending"><LoaderCircle size={14} />正在提交任务状态…</p>}
      {error && <p className="transfer-error">{error}</p>}
      {job.task.publicSafeMessage && <p className="transfer-safe-message">{job.task.publicSafeMessage}</p>}
    </div>
  )
}

function ExportDetail({ job, onChanged }: { job: ExportJob; onChanged: (job: ExportJob) => void }) {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string>()
  const command = async (kind: 'restart' | 'cancel' | 'cleanup') => {
    const envelope = { commandRequestId: globalThis.crypto.randomUUID(), expectedStateRevision: job.task.stateRevision }
    setBusy(true)
    setError(undefined)
    try {
      const next = kind === 'restart'
        ? await bridge.restartExport(job.task.id, envelope)
        : kind === 'cancel'
          ? await bridge.cancelExport(job.task.id, envelope)
          : await bridge.cleanupExport(job.task.id, envelope)
      onChanged(next)
    } catch (cause) {
      setError(safeError(cause))
    } finally {
      setBusy(false)
    }
  }
  const complete = job.fragments?.filter((fragment) => fragment.state === 'COMPLETE').length ?? 0
  return (
    <div className="transfer-detail">
      <header><div><span className="eyebrow">EXPORT JOB</span><h3>{job.task.id}</h3></div></header>
      <TaskMetaStrip job={job} />
      <section className="transfer-detail-section">
        <h4>逻辑导出分片</h4>
        <dl className="transfer-definition-grid">
          <div><dt>Complete</dt><dd>{String(complete)}</dd></div>
          <div><dt>Total</dt><dd>{String(job.fragments?.length ?? 0)}</dd></div>
          <div><dt>Retained</dt><dd>{job.retained ? 'yes' : 'no'}</dd></div>
          <div><dt>Generation</dt><dd>{job.connectionGeneration}</dd></div>
        </dl>
      </section>
      <div className="transfer-actions transfer-actions--footer">
        {job.task.state === 'PAUSED_RESTARTABLE' && <button className="primary-button" disabled={busy} onClick={() => void command('restart')}><RotateCcw size={14} />重新开始</button>}
        {!job.task.terminal && <button className="secondary-button" disabled={busy} onClick={() => void command('cancel')}>取消导出</button>}
        {(job.task.terminal || job.task.state === 'PAUSED_RESTARTABLE') && <button className="secondary-button" disabled={busy} onClick={() => void command('cleanup')}><Trash2 size={14} />清理任务文件</button>}
      </div>
      {busy && <p className="transfer-pending"><LoaderCircle size={14} />正在提交任务状态…</p>}
      {error && <p className="transfer-error">{error}</p>}
      {job.task.publicSafeMessage && <p className="transfer-safe-message">{job.task.publicSafeMessage}</p>}
    </div>
  )
}

interface CSVRow { id: string; source: string; role: 'tag' | 'field'; target: string; kind: CSVFieldKind }

function ImportForm({ profileId, schema, native, onCreated, onBack }: {
  profileId: string
  schema: SchemaDatabase[]
  native: boolean
  onCreated: (job: ImportJob) => void
  onBack: () => void
}) {
  const [file, setFile] = useState<File>()
  const [sourcePath, setSourcePath] = useState('')
  const [selectedSource, setSelectedSource] = useState<ImportSourceInspection>()
  const [format, setFormat] = useState<ImportFormat>('LP')
  const [database, setDatabase] = useState(schema[0]?.name ?? '')
  const [retentionPolicy, setRetentionPolicy] = useState(schema[0]?.retentionPolicies[0] ?? '')
  const [measurementMode, setMeasurementMode] = useState<'static' | 'column'>('static')
  const [measurement, setMeasurement] = useState('')
  const [timestampColumn, setTimestampColumn] = useState('time')
  const [rows, setRows] = useState<CSVRow[]>([{ id: globalThis.crypto.randomUUID(), source: '', role: 'field', target: '', kind: 'float64' }])
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string>()

  const createCSVMapping = (): CSVMapping | undefined => {
    if (format !== 'CSV') return undefined
    const tagColumns: Record<string, string> = {}
    const fieldColumns: CSVMapping['fieldColumns'] = {}
    for (const row of rows) {
      if (!row.source.trim() || !row.target.trim()) return undefined
      if (row.role === 'tag') tagColumns[row.source.trim()] = row.target.trim()
      else fieldColumns[row.source.trim()] = { target: row.target.trim(), kind: row.kind }
    }
    if (!timestampColumn.trim() || !measurement.trim() || Object.keys(fieldColumns).length === 0) return undefined
    return {
      timestampColumn: timestampColumn.trim(),
      ...(measurementMode === 'static' ? { staticMeasurement: measurement.trim() } : { measurementColumn: measurement.trim() }),
      tagColumns,
      fieldColumns,
    }
  }

  const submit = async () => {
    const csvMapping = createCSVMapping()
    if ((!file && !selectedSource) || !database.trim() || (format === 'CSV' && !csvMapping)) {
      return setError('请选择文件、目标 database，并完成 CSV 的显式列映射。')
    }
    setBusy(true)
    setError(undefined)
    try {
      const identity = selectedSource?.sourcePath === sourcePath
        ? selectedSource
        : file
          ? await bridge.inspectImportFile(file, sourcePath)
          : undefined
      if (!identity) throw new Error('IMPORT_SOURCE_PATH_UNAVAILABLE')
      const response = await bridge.preflightImport({
        clientRequestId: globalThis.crypto.randomUUID(),
        profileId,
        sourcePath: identity.sourcePath,
        source: { sha256: identity.sha256, sizeBytes: identity.sizeBytes },
        format,
        target: { database: database.trim(), retentionPolicy: retentionPolicy.trim() || undefined },
        csvMapping,
      })
      onCreated(response.job)
    } catch (cause) {
      setError(safeError(cause))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="transfer-form">
      <header><button className="transfer-back" onClick={onBack}><ArrowLeft size={15} />返回任务</button><div><span className="eyebrow">NEW IMPORT</span><h3>导入预检</h3></div></header>
      <section className="transfer-form-section">
        <h4>源文件</h4>
        <div className="transfer-form-grid">
          {native ? (
            <div className="transfer-native-picker transfer-field--wide">
              <button className="secondary-button" type="button" disabled={busy} onClick={() => void (async () => {
                setError(undefined)
                try {
                  const selected = await bridge.selectImportSource()
                  setFile(undefined)
                  setSelectedSource(selected)
                  setSourcePath(selected.sourcePath)
                } catch (cause) {
                  if (!selectionCanceled(cause)) setError(safeError(cause))
                }
              })()}><FolderOpen size={14} />选择文件</button>
              <span>{selectedSource?.displayName ?? '尚未选择'}</span>
            </div>
          ) : (
            <label className="transfer-field transfer-field--wide">文件<input type="file" accept=".lp,.txt,.gz,.jsonl,.csv" onChange={(event) => {
              setSelectedSource(undefined)
              setFile(event.target.files?.[0])
            }} /></label>
          )}
          <label className="transfer-field">格式<select value={format} onChange={(event) => setFormat(event.target.value as ImportFormat)}><option value="LP">LP</option><option value="TXT">TXT</option><option value="LP_GZ">LP.GZ</option><option value="TYPED_JSONL">Typed JSONL</option><option value="CSV">CSV</option></select></label>
          <label className="transfer-field">Database<input value={database} list="transfer-databases" onChange={(event) => setDatabase(event.target.value)} /><datalist id="transfer-databases">{schema.map((item) => <option key={item.name} value={item.name} />)}</datalist></label>
          <label className="transfer-field">Retention policy<input value={retentionPolicy} onChange={(event) => setRetentionPolicy(event.target.value)} /></label>
        </div>
      </section>
      {format === 'CSV' && (
        <section className="transfer-form-section">
          <h4>CSV 显式映射</h4>
          <div className="transfer-inline-fields">
            <label>Timestamp column<input value={timestampColumn} onChange={(event) => setTimestampColumn(event.target.value)} /></label>
            <label>Measurement<select value={measurementMode} onChange={(event) => setMeasurementMode(event.target.value as 'static' | 'column')}><option value="static">固定名称</option><option value="column">来源列</option></select></label>
            <label>{measurementMode === 'static' ? 'Measurement name' : 'Measurement column'}<input value={measurement} onChange={(event) => setMeasurement(event.target.value)} /></label>
          </div>
          <div className="csv-mapping-table">
            <div className="csv-mapping-heading"><span>来源列</span><span>角色</span><span>目标键</span><span>类型</span><span /></div>
            {rows.map((row) => (
              <div className="csv-mapping-row" key={row.id}>
                <input aria-label="CSV 来源列" value={row.source} onChange={(event) => setRows((current) => current.map((item) => item.id === row.id ? { ...item, source: event.target.value } : item))} />
                <select aria-label="CSV 列角色" value={row.role} onChange={(event) => setRows((current) => current.map((item) => item.id === row.id ? { ...item, role: event.target.value as 'tag' | 'field' } : item))}><option value="tag">Tag</option><option value="field">Field</option></select>
                <input aria-label="CSV 目标键" value={row.target} onChange={(event) => setRows((current) => current.map((item) => item.id === row.id ? { ...item, target: event.target.value } : item))} />
                <select aria-label="CSV 字段类型" value={row.kind} disabled={row.role === 'tag'} onChange={(event) => setRows((current) => current.map((item) => item.id === row.id ? { ...item, kind: event.target.value as CSVFieldKind } : item))}><option value="string">string</option><option value="boolean">boolean</option><option value="int64">int64</option><option value="uint64">uint64</option><option value="float64">float64</option></select>
                <IconButton label="删除映射" disabled={rows.length === 1} onClick={() => setRows((current) => current.filter((item) => item.id !== row.id))}><X size={13} /></IconButton>
              </div>
            ))}
          </div>
          <button className="inline-add" onClick={() => setRows((current) => [...current, { id: globalThis.crypto.randomUUID(), source: '', role: 'field', target: '', kind: 'float64' }])}><Plus size={13} />添加映射</button>
        </section>
      )}
      <div className="transfer-actions transfer-actions--footer"><button className="secondary-button" onClick={onBack}>取消</button><button className="primary-button" disabled={busy || !profileId} onClick={() => void submit()}>{busy ? <LoaderCircle size={14} /> : <FileUp size={14} />}开始预检</button></div>
      {error && <p className="transfer-error">{error}</p>}
    </div>
  )
}

const int64Pattern = /^(?:0|-[1-9][0-9]*|[1-9][0-9]*)$/
function validRange(start: string, end: string) {
  if (!int64Pattern.test(start) || !int64Pattern.test(end)) return false
  const minimum = -(1n << 63n)
  const maximum = (1n << 63n) - 1n
  const left = BigInt(start)
  const right = BigInt(end)
  return left >= minimum && right <= maximum && left < right
}

function ExportForm({ profileId, schema, native, onCreated, onBack }: {
  profileId: string
  schema: SchemaDatabase[]
  native: boolean
  onCreated: (job: ExportJob) => void
  onBack: () => void
}) {
  const [database, setDatabase] = useState(schema[0]?.name ?? '')
  const databaseSchema = schema.find((item) => item.name === database)
  const [retentionPolicy, setRetentionPolicy] = useState(databaseSchema?.retentionPolicies[0] ?? '')
  const [measurements, setMeasurements] = useState<string[]>([])
  const [manualMeasurements, setManualMeasurements] = useState('')
  const [targetDirectory, setTargetDirectory] = useState('')
  const [startNs, setStartNs] = useState('0')
  const [endNs, setEndNs] = useState('1')
  const [strict, setStrict] = useState(true)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string>()
  const available = databaseSchema?.measurements ?? []

  const submit = async () => {
    const selected = available.length > 0 ? measurements : manualMeasurements.split(',').map((item) => item.trim()).filter(Boolean)
    if (!profileId || !database.trim() || !targetDirectory.trim() || selected.length === 0 || !validRange(startNs, endNs)) {
      return setError('请完成目标目录、measurement 和有效的 int64 纳秒半开区间。')
    }
    setBusy(true)
    setError(undefined)
    try {
      onCreated(await bridge.startExport({
        clientRequestId: globalThis.crypto.randomUUID(),
        profileId,
        targetDirectory: targetDirectory.trim(),
        database: database.trim(),
        retentionPolicy: retentionPolicy.trim() || undefined,
        measurements: selected,
        startNs,
        endNs,
        strict,
      }))
    } catch (cause) {
      setError(safeError(cause))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="transfer-form">
      <header><button className="transfer-back" onClick={onBack}><ArrowLeft size={15} />返回任务</button><div><span className="eyebrow">NEW EXPORT</span><h3>创建逻辑导出</h3></div></header>
      <section className="transfer-form-section">
        <h4>范围与目标</h4>
        <div className="transfer-form-grid">
          <label className="transfer-field">Database<select value={database} onChange={(event) => { setDatabase(event.target.value); setMeasurements([]) }}>{schema.map((item) => <option key={item.name} value={item.name}>{item.name}</option>)}</select></label>
          <label className="transfer-field">Retention policy<input value={retentionPolicy} onChange={(event) => setRetentionPolicy(event.target.value)} /></label>
          {native ? (
            <div className="transfer-native-picker transfer-field--wide">
              <button className="secondary-button" type="button" disabled={busy} onClick={() => void (async () => {
                setError(undefined)
                try {
                  setTargetDirectory(await bridge.selectExportDirectory())
                } catch (cause) {
                  if (!selectionCanceled(cause)) setError(safeError(cause))
                }
              })()}><FolderOpen size={14} />选择目录</button>
              <span>{targetDirectory || '尚未选择'}</span>
            </div>
          ) : <label className="transfer-field transfer-field--wide">目标目录<input value={targetDirectory} onChange={(event) => setTargetDirectory(event.target.value)} placeholder="D:\exports" /></label>}
          <label className="transfer-field">Start ns<input value={startNs} onChange={(event) => setStartNs(event.target.value)} /></label>
          <label className="transfer-field">End ns<input value={endNs} onChange={(event) => setEndNs(event.target.value)} /></label>
        </div>
      </section>
      <section className="transfer-form-section">
        <h4>Measurements</h4>
        {available.length > 0 ? <div className="measurement-picker">{available.map((item) => <label key={item.name}><input type="checkbox" checked={measurements.includes(item.name)} onChange={(event) => setMeasurements((current) => event.target.checked ? [...current, item.name] : current.filter((name) => name !== item.name))} /><span>{item.name}</span></label>)}</div> : <label className="transfer-field">Measurement 列表<input value={manualMeasurements} onChange={(event) => setManualMeasurements(event.target.value)} placeholder="cpu, memory" /></label>}
        <label className="transfer-check"><input type="checkbox" checked={strict} onChange={(event) => setStrict(event.target.checked)} /><span><strong>严格类型模式</strong><small>Schema 漂移、重名或类型不确定时停止</small></span></label>
      </section>
      <div className="transfer-actions transfer-actions--footer"><button className="secondary-button" onClick={onBack}>取消</button><button className="primary-button" disabled={busy || !profileId} onClick={() => void submit()}>{busy ? <LoaderCircle size={14} /> : <Download size={14} />}创建导出</button></div>
      {error && <p className="transfer-error">{error}</p>}
    </div>
  )
}

export function TransferPanel({ embedded = false }: { embedded?: boolean }) {
  const open = useWorkbenchStore((state) => state.taskDrawerOpen)
  const setOpen = useWorkbenchStore((state) => state.setTaskDrawerOpen)
  const profileId = useWorkbenchStore((state) => state.activeConnectionId)
  const protection = useWorkbenchStore((state) => state.protection)
  const schema = useWorkbenchStore((state) => state.schema)
  const replaceTransferTasks = useWorkbenchStore((state) => state.replaceTransferTasks)
  const [mode, setMode] = useState<PanelMode>('overview')
  const [imports, setImports] = useState<ImportJob[]>([])
  const [exports, setExports] = useState<ExportJob[]>([])
  const [selected, setSelected] = useState<Selection>()
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string>()
  const jobsRef = useRef<{ imports: ImportJob[]; exports: ExportJob[] }>({ imports: [], exports: [] })
  const eventSyncRef = useRef<TaskEventSynchronizer | undefined>(undefined)
  const eventSyncSupported = bridge.supportsTaskEventSync()
  const visible = embedded || open

  const applyFullSnapshot = useCallback((nextImports: ImportJob[], nextExports: ExportJob[]) => {
    jobsRef.current = { imports: nextImports, exports: nextExports }
    setImports(nextImports)
    setExports(nextExports)
    setSelected((current) => {
      if (current && (current.kind === 'import' ? nextImports : nextExports).some((job) => job.task.id === current.id)) return current
      if (nextImports[0]) return { kind: 'import', id: nextImports[0].task.id }
      if (nextExports[0]) return { kind: 'export', id: nextExports[0].task.id }
      return undefined
    })
  }, [])

  const applyImportSnapshot = useCallback((job: ImportJob) => {
    if (job.profileId !== profileId) return
    setImports((current) => {
      const index = current.findIndex((item) => item.task.id === job.task.id)
      const next = index === -1
        ? [job, ...current]
        : current.map((item) => item.task.id === job.task.id ? job : item)
      jobsRef.current = { ...jobsRef.current, imports: next }
      return next
    })
  }, [profileId])

  const applyExportSnapshot = useCallback((job: ExportJob) => {
    if (job.profileId !== profileId) return
    setExports((current) => {
      const index = current.findIndex((item) => item.task.id === job.task.id)
      const next = index === -1
        ? [job, ...current]
        : current.map((item) => item.task.id === job.task.id ? job : item)
      jobsRef.current = { ...jobsRef.current, exports: next }
      return next
    })
  }, [profileId])

  const refresh = useCallback(async () => {
    if (!profileId) return
    setLoading(true)
    setError(undefined)
    try {
      if (eventSyncSupported && eventSyncRef.current) {
        await eventSyncRef.current.resync()
        return
      }
      const [nextImports, nextExports] = await Promise.all([
        bridge.listRecoverableImports(profileId, 100),
        bridge.listExports(profileId, '', 100),
      ])
      applyFullSnapshot(nextImports, nextExports)
    } catch (cause) {
      setError(safeError(cause))
    } finally {
      setLoading(false)
    }
  }, [applyFullSnapshot, eventSyncSupported, profileId])

  useEffect(() => {
    replaceTransferTasks(summaries(imports, exports))
  }, [exports, imports, replaceTransferTasks])

  useEffect(() => {
    if (!profileId || !eventSyncSupported) return
    jobsRef.current = { imports: [], exports: [] }
    setImports([])
    setExports([])
    setLoading(true)
    setError(undefined)

    const synchronizer = new TaskEventSynchronizer({
      subscribe: (listener) => bridge.subscribeTaskChanges(listener),
      getHead: () => bridge.getTaskEventHead(),
      getChanges: (after, limit) => bridge.getTaskChanges(after, limit),
      loadAll: async () => {
        const [nextImports, nextExports] = await Promise.all([
          bridge.listRecoverableImports(profileId, 100),
          bridge.listExports(profileId, '', 100),
        ])
        return { imports: nextImports, exports: nextExports }
      },
      loadOne: async (kind, id) => kind === 'IMPORT'
        ? { kind, job: await bridge.getImportJob(id) }
        : { kind, job: await bridge.getExportJob(id) },
      hasActiveTasks: () => [
        ...jobsRef.current.imports,
        ...jobsRef.current.exports,
      ].some((job) => !job.task.terminal),
    }, {
      onFullSnapshot: applyFullSnapshot,
      onResourceSnapshot: (snapshot) => {
        if (snapshot.kind === 'IMPORT') applyImportSnapshot(snapshot.job)
        else applyExportSnapshot(snapshot.job)
      },
      onError: (cause) => setError(safeError(cause)),
    })
    eventSyncRef.current = synchronizer
    let active = true
    void synchronizer.start()
      .catch((cause) => { if (active) setError(safeError(cause)) })
      .finally(() => { if (active) setLoading(false) })

    return () => {
      active = false
      synchronizer.stop()
      if (eventSyncRef.current === synchronizer) eventSyncRef.current = undefined
    }
  }, [applyExportSnapshot, applyFullSnapshot, applyImportSnapshot, eventSyncSupported, profileId])

  useEffect(() => {
    if (eventSyncSupported || !visible) return
    void refresh()
    const timer = window.setInterval(() => void refresh(), 5000)
    return () => window.clearInterval(timer)
  }, [eventSyncSupported, refresh, visible])

  const selectedImport = selected?.kind === 'import' ? imports.find((job) => job.task.id === selected.id) : undefined
  const selectedExport = selected?.kind === 'export' ? exports.find((job) => job.task.id === selected.id) : undefined
  const updateImport = (job: ImportJob) => {
    applyImportSnapshot(job)
    if (!eventSyncSupported) void refresh()
  }
  const updateExport = (job: ExportJob) => {
    applyExportSnapshot(job)
    if (!eventSyncSupported) void refresh()
  }
  const createdImport = (job: ImportJob) => { applyImportSnapshot(job); setSelected({ kind: 'import', id: job.task.id }); setMode('overview'); if (!eventSyncSupported) void refresh() }
  const createdExport = (job: ExportJob) => { applyExportSnapshot(job); setSelected({ kind: 'export', id: job.task.id }); setMode('overview'); if (!eventSyncSupported) void refresh() }

  if (!visible) return null

  const commands = (
    <div className="drawer-commands">
      <button className="primary-button" disabled={!profileId} onClick={() => setMode('import')}><FileUp size={15} />新建导入</button>
      <button className="secondary-button" disabled={!profileId} onClick={() => setMode('export')}><Download size={15} />新建导出</button>
    </div>
  )
  const workspace = mode === 'import'
    ? <ImportForm profileId={profileId} schema={schema} native={bridge.isNative()} onCreated={createdImport} onBack={() => setMode('overview')} />
    : mode === 'export'
      ? <ExportForm profileId={profileId} schema={schema} native={bridge.isNative()} onCreated={createdExport} onBack={() => setMode('overview')} />
      : (
        <div className="transfer-overview">
          <div className="transfer-list-pane">
            <div className="task-filter-row"><span>当前连接</span><strong>{imports.length + exports.length}</strong>{loading ? <LoaderCircle size={13} /> : null}</div>
            <TaskList imports={imports} exports={exports} selected={selected} onSelect={setSelected} />
          </div>
          <div className="transfer-detail-pane">
            {selectedImport ? <ImportDetail key={`${selectedImport.task.id}:${selectedImport.task.stateRevision}`} job={selectedImport} profileId={profileId} protection={protection} onChanged={updateImport} /> : null}
            {selectedExport ? <ExportDetail key={`${selectedExport.task.id}:${selectedExport.task.stateRevision}`} job={selectedExport} onChanged={updateExport} /> : null}
            {!selectedImport && !selectedExport ? <div className="transfer-empty">选择任务查看 checkpoint、revision 和可用命令</div> : null}
          </div>
        </div>
      )

  if (embedded) {
    return (
      <aside className="page-surface transfer-page transfer-panel transfer-panel--embedded" aria-label="导入导出任务">
        <header className="page-header">
          <div><span className="eyebrow">TRANSFERS</span><h1>数据传输</h1><p>逻辑导入、导出与人工决策</p></div>
          {commands}
        </header>
        <ProtectionBanner protection={protection} />
        {workspace}
        {error ? <div className="transfer-global-error">{error}</div> : null}
      </aside>
    )
  }

  return (
    <>
      <button className="drawer-scrim" aria-label="关闭任务抽屉" onClick={() => setOpen(false)} />
      <aside className="task-drawer transfer-panel" aria-label="导入导出任务">
        <header className="drawer-heading">
          <div><span className="eyebrow">TRANSFERS</span><h2>数据传输</h2></div>
          <div className="transfer-heading-actions">
            <IconButton label="刷新任务" disabled={loading} onClick={() => void refresh()}><RefreshCw size={16} /></IconButton>
            <IconButton label="关闭" onClick={() => setOpen(false)}><X size={18} /></IconButton>
          </div>
        </header>
        {commands}
        <ProtectionBanner protection={protection} />
        {workspace}
        {error ? <div className="transfer-global-error">{error}</div> : null}
      </aside>
    </>
  )
}
