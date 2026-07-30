import { mockConnections, mockResult, mockSchema } from './mockData'
import type {
  AuthorizedImportResolveRequest,
  AuthorizedImportRunRequest,
  BridgeQueryResponse,
  ConnectionProfile,
  ExportJob,
  ImportBatchResult,
  ImportJob,
  ImportPreflightRequest,
  ImportPreflightResponse,
  ImportRunPreview,
  ImportRunRequest,
  ImportSourceInspection,
  MutationExecuteRequest,
  MutationOperationResult,
  MutationPreview,
  MutationPreviewRequest,
  ProtectionState,
  QueryRequest,
  QueryResult,
  QueryResultExportRequest,
  QueryResultExportResult,
  QueryState,
  ResultColumn,
  ResultRow,
  ScalarKind,
  SchemaDatabase,
  SchemaMeasurement,
  StartExportRequest,
  TaskCommandEnvelope,
  TaskEvent,
  TypedScalar,
} from './types'

type WailsMethod = (...args: unknown[]) => Promise<unknown>
type QueryStartedHandler = (sessionId: string, stateRevision: string) => void

declare global {
  interface Window {
    go?: {
      main?: {
        App?: Record<string, WailsMethod>
      }
    }
    runtime?: {
      EventsOnMultiple?: (
        eventName: string,
        callback: (...data: unknown[]) => void,
        maxCallbacks: number,
      ) => (() => void) | void
    }
  }
}

export interface ConnectionDraft {
  id?: string
  expectedRevision?: string
  name: string
  baseUrl: string
  defaultDatabase: string
  username?: string
  secret?: string
  environment?: ConnectionProfile['environment']
  authMode?: 'NONE' | 'BASIC' | 'BEARER'
  protectionMode?: 'PermanentReadOnly' | 'ProtectedLocked'
}

export interface ProtectionSnapshot {
  connectionId: string
  connectionGeneration: string
  protectionRevision: string
  mode: 'PermanentReadOnly' | 'ProtectedLocked' | 'ProtectedUnlocked'
  leaseId?: string
  unlockedUntil?: string
}

export interface NativeConnectionSnapshot {
  connectionId: string
  connectionGeneration: string
  profileId: string
  profileRevision: string
  version?: string
  protection: ProtectionSnapshot
}

interface NativeProfile {
  id: string
  revision: string
  name: string
  baseUrl: string
  defaultDatabase: string
  environment: ConnectionProfile['environment']
  authMode: 'NONE' | 'BASIC' | 'BEARER'
  username?: string
  protectionMode: 'PermanentReadOnly' | 'ProtectedLocked'
}

function profileView(profile: NativeProfile): ConnectionProfile {
  return {
    id: profile.id,
    revision: profile.revision,
    name: profile.name,
    url: profile.baseUrl,
    defaultDatabase: profile.defaultDatabase,
    authMode: profile.authMode,
    username: profile.username,
    environment: profile.environment,
    protectionMode: profile.protectionMode,
    state: 'disconnected',
  }
}

function draftAuthMode(draft: ConnectionDraft): 'NONE' | 'BASIC' | 'BEARER' {
  if (draft.authMode === 'BEARER') return 'BEARER'
  return draft.username?.trim() || draft.secret ? 'BASIC' : 'NONE'
}

const delay = (milliseconds: number) => new Promise((resolve) => window.setTimeout(resolve, milliseconds))

function method(name: string): WailsMethod | undefined {
  return window.go?.main?.App?.[name]
}

async function invokeIdempotent(native: WailsMethod, args: unknown[]): Promise<unknown> {
  try {
    return await native(...args)
  } catch {
    return native(...args)
  }
}

interface NativeQuerySession {
  id: string
  state: string
  terminal: boolean
  stateRevision: string
  createdAt: string
  updatedAt: string
  publicErrorCode?: string
  publicSafeMessage?: string
  resultAvailable: boolean
  complete: boolean
}

interface NativeSeriesSummary {
  id: string
  statementId: number
  measurement: string
  columns: string[]
  rows?: string
}

interface NativeResultPage {
  rows: TypedScalar[][]
  nextCursor?: string
  eof: boolean
}

interface NativeOperation {
  task: {
    id: string
    state: string
    terminal: boolean
    publicErrorCode?: string
    publicSafeMessage?: string
  }
}

function mockTask(
  id: string,
  kind: 'IMPORT' | 'EXPORT',
  state: string,
  terminal = false,
  revision = '1',
) {
  const now = new Date().toISOString()
  return {
    id,
    kind,
    state,
    terminal,
    snapshotRevision: revision,
    stateRevision: revision,
    lastEventSeq: revision,
    createdAt: now,
    updatedAt: now,
    archived: false,
    resultAvailable: false,
  }
}

const mockImportJobs: ImportJob[] = [{
  task: mockTask('import-demo-incident', 'IMPORT', 'NEEDS_UNKNOWN_DECISION', false, '7'),
  profileId: 'prod-east',
  checkpointDigest: 'demo-checkpoint-digest',
  checkpoint: {
    sequence: '4',
    logicalOffset: '67108864',
    adaptiveMaxPoints: '5000',
    adaptiveMaxBytes: '5242880',
    sourceSha256: 'demo-source',
    stagingSha256: 'demo-staging',
    normalizationVersion: 'canonical-lp-v1',
    specDigest: 'demo-spec',
    targetDigest: 'demo-target',
    lossy: false,
  },
  pauseRequested: false,
  openIncident: {
    incidentId: 'incident-demo-unknown',
    kind: 'UNKNOWN',
    status: 'OPEN',
    parentCheckpointDigest: 'demo-checkpoint-digest',
    runSegmentId: 'segment-demo',
    batchId: 'batch-demo',
    attemptId: 'attempt-demo',
    startOffset: '67108864',
    endOffset: '68157440',
    payloadDigest: 'demo-payload',
  },
}]

const mockExportJobs: ExportJob[] = [{
  task: mockTask('export-demo-complete', 'EXPORT', 'SUCCEEDED', true, '12'),
  profileId: 'prod-east',
  profileRevision: '1',
  connectionId: 'prod-east',
  connectionGeneration: '1',
  specDigest: 'demo-export-spec',
  targetDigest: 'demo-export-target',
  targetVolumeId: 'demo-volume',
  cancelRequested: false,
  retained: false,
  fragments: [{
    fragmentId: 'fragment-demo',
    jobId: 'export-demo-complete',
    ordinal: '0',
    kind: 'DATA',
    state: 'COMPLETE',
    compressedSize: '1048576',
    uncompressedSize: '4194304',
    checksumSha256: 'demo-checksum',
    reusable: true,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  }],
}]

function replaceMockImport(job: ImportJob) {
  const index = mockImportJobs.findIndex((candidate) => candidate.task.id === job.task.id)
  if (index === -1) mockImportJobs.unshift(job)
  else mockImportJobs[index] = job
}

function replaceMockExport(job: ExportJob) {
  const index = mockExportJobs.findIndex((candidate) => candidate.task.id === job.task.id)
  if (index === -1) mockExportJobs.unshift(job)
  else mockExportJobs[index] = job
}

function nextMockTask(task: ImportJob['task'], state: string, terminal = false) {
  const revision = (BigInt(task.stateRevision) + 1n).toString()
  return {
    ...task,
    state,
    terminal,
    snapshotRevision: revision,
    stateRevision: revision,
    lastEventSeq: revision,
    updatedAt: new Date().toISOString(),
    terminalAt: terminal ? new Date().toISOString() : undefined,
  }
}

function queryState(state: string): QueryState {
  if (state === 'QUEUED') return 'queued'
  if (state === 'RUNNING' || state === 'CANCEL_REQUESTED') return 'running'
  if (state === 'CANCELED') return 'canceled'
  if (state === 'SUCCEEDED' || state === 'SUCCEEDED_WITH_ERRORS' || state === 'TRUNCATED') return 'succeeded'
  return 'failed'
}

async function waitForQuery(
  session: NativeQuerySession,
  onSnapshot?: QueryStartedHandler,
): Promise<NativeQuerySession> {
  let current = session
  while (!current.terminal) {
    const getSession = method('GetQuerySession')
    if (!getSession) throw new Error('GetQuerySession is unavailable')
    await delay(80)
    current = (await getSession(current.id)) as NativeQuerySession
    onSnapshot?.(current.id, current.stateRevision)
  }
  return current
}

async function invokeQueryCommand(
  name: 'CancelQuery' | 'CloseQuery',
  sessionId: string,
  expectedStateRevision: string,
): Promise<NativeQuerySession | undefined> {
  const native = method(name)
  if (!native) return undefined
  const envelope = {
    commandRequestId: globalThis.crypto.randomUUID(),
    expectedStateRevision,
  }
  try {
    return (await native(sessionId, envelope)) as NativeQuerySession
  } catch {
    return (await native(sessionId, envelope)) as NativeQuerySession
  }
}

async function waitForOperation(operation: NativeOperation): Promise<NativeOperation> {
  let current = operation
  while (!current.task.terminal) {
    const getOperation = method('GetOperation')
    if (!getOperation) throw new Error('GetOperation is unavailable')
    await delay(100)
    current = (await getOperation(current.task.id)) as NativeOperation
  }
  return current
}

function mutationOperationResult(operation: NativeOperation): MutationOperationResult {
  return {
    id: operation.task.id,
    state: operation.task.state,
    terminal: operation.task.terminal,
    publicErrorCode: operation.task.publicErrorCode,
    publicSafeMessage: operation.task.publicSafeMessage,
  }
}

function mockMutationPreview(
  request: MutationPreviewRequest,
  protection: ProtectionState | undefined,
): MutationPreview {
  const canonicalQuery = request.query.trim()
  const command = canonicalQuery.match(/^([a-z]+)(?:\s+([a-z]+))?/i)
  if (!command || !/^(delete|drop|create|alter|grant|revoke|kill)$/i.test(command[1])) {
    throw new Error('MUTATION_PREVIEW_UNAVAILABLE')
  }
  const quotedTargets = [...canonicalQuery.matchAll(/"((?:[^"]|"")+)"/g)]
  const quotedTarget = quotedTargets.at(-1)?.[1].replaceAll('""', '"')
  const fallbackTarget = canonicalQuery.match(/\s([^\s;]+)\s*;?$/)?.[1]
  const target = quotedTarget ?? fallbackTarget
  const destructive = /^(delete|drop|alter|revoke|kill)\b/i.test(canonicalQuery)
  const executable = protection === 'unlocked'
  return {
    canonicalQuery,
    operationKind: [command[1], command[2]].filter(Boolean).join('_').toUpperCase(),
    target,
    confirmationRequired: destructive,
    executable,
    token: executable ? `mock-preview-${globalThis.crypto.randomUUID()}` : undefined,
    expiresAt: executable ? new Date(Date.now() + 120_000).toISOString() : undefined,
  }
}

function inferColumns(names: string[], rows: TypedScalar[][]): ResultColumn[] {
  return names.map((name, index) => {
    const kinds = new Set<ScalarKind>()
    for (const row of rows) {
      const scalar = row[index]
      if (scalar && scalar.kind !== 'null') kinds.add(scalar.kind)
    }
    const kind = kinds.size === 1 ? [...kinds][0] : kinds.size === 0 ? 'null' : 'mixed'
    return { key: name, label: name, kind, width: name === 'time' ? 220 : 140 }
  })
}

async function loadNativeResult(session: NativeQuerySession): Promise<QueryResult | undefined> {
  if (!session.resultAvailable) return undefined
  const listSeries = method('ListResultSeries')
  const getPage = method('GetResultPage')
  if (!listSeries || !getPage) return undefined
  const series = (await listSeries(session.id, 0)) as NativeSeriesSummary[]
  if (series.length === 0) return undefined
  const selected = series[0]
  const page = (await getPage({
    sessionId: session.id,
    statementId: selected.statementId,
    seriesId: selected.id,
    cursor: '',
    limit: 5000,
  })) as NativeResultPage
  const rows: ResultRow[] = page.rows.map((values, rowIndex) => ({
    id: `${selected.id}-${rowIndex}`,
    sourceIndex: String(rowIndex),
    cells: Object.fromEntries(selected.columns.map((column, index) => [column, values[index] ?? { kind: 'null' }])),
  }))
  const elapsedMs = Math.max(0, new Date(session.updatedAt).getTime() - new Date(session.createdAt).getTime())
  return {
    sessionId: session.id,
    statementId: selected.statementId,
    seriesId: selected.id,
    seriesName: selected.measurement,
    columns: inferColumns(selected.columns, page.rows),
    rows,
    totalRows: selected.rows ?? String(rows.length),
    complete: session.complete && page.eof,
    elapsedMs,
    scannedBytes: 0,
  }
}

export const bridge = {
  isNative(): boolean {
    return Boolean(window.go?.main?.App)
  },

  supportsTransferMethod(name: string): boolean {
    return !this.isNative() || Boolean(method(name))
  },

  supportsTaskEventSync(): boolean {
    if (!this.isNative()) return false
    return Boolean(
      window.runtime?.EventsOnMultiple &&
      method('GetTaskEventHead') &&
      method('GetTaskChanges') &&
      method('GetImportJob') &&
      method('GetExportJob'),
    )
  },

  subscribeTaskChanges(listener: (event: TaskEvent) => void): () => void {
    if (!this.isNative()) return () => undefined
    const runtime = window.runtime
    if (!runtime?.EventsOnMultiple) throw new Error('TASK_EVENT_RUNTIME_UNAVAILABLE')
    const unsubscribe = runtime.EventsOnMultiple('task.changed.v1', (event) => {
      listener(event as TaskEvent)
    }, -1)
    return typeof unsubscribe === 'function' ? unsubscribe : () => undefined
  },

  async getTaskEventHead(): Promise<string> {
    const native = method('GetTaskEventHead')
    if (!native) throw new Error('TASK_EVENT_HEAD_UNAVAILABLE')
    return (await native()) as string
  },

  async getTaskChanges(after: string, limit = 1000): Promise<TaskEvent[]> {
    const native = method('GetTaskChanges')
    if (!native) throw new Error('TASK_EVENT_CHANGES_UNAVAILABLE')
    return (await native(after, limit)) as TaskEvent[]
  },

  async loadSchema(profileId: string): Promise<SchemaDatabase[]> {
    const native = method('GetSchemaSnapshot')
    if (native) return (await native(profileId)) as SchemaDatabase[]
    await delay(180)
    return mockSchema
  },

  async getMeasurementSchema(
    profileId: string,
    database: string,
    measurement: string,
  ): Promise<SchemaMeasurement> {
    const native = method('GetMeasurementSchema')
    if (native) return (await native(profileId, database, measurement)) as SchemaMeasurement
    if (this.isNative()) throw new Error('GetMeasurementSchema is unavailable')
    await delay(80)
    const detail = mockSchema
      .find((candidate) => candidate.name === database)
      ?.measurements.find((candidate) => candidate.name === measurement)
    if (!detail) throw new Error('MEASUREMENT_SCHEMA_NOT_FOUND')
    return {
      name: detail.name,
      fields: detail.fields.map((field) => ({ ...field })),
      tags: [...detail.tags],
    }
  },

  async listProfiles(): Promise<ConnectionProfile[]> {
    const native = method('ListProfiles')
    if (!native) return mockConnections
    const profiles = (await native()) as NativeProfile[]
    return profiles.map(profileView)
  },

  async saveProfile(draft: ConnectionDraft): Promise<ConnectionProfile> {
    const native = method('SaveProfile')
    if (!native) {
      return {
        id: draft.id ?? globalThis.crypto.randomUUID(),
        revision: draft.expectedRevision
          ? (BigInt(draft.expectedRevision) + 1n).toString()
          : '1',
        name: draft.name,
        url: draft.baseUrl,
        defaultDatabase: draft.defaultDatabase,
        authMode: draftAuthMode(draft),
        username: draft.username,
        environment: draft.environment ?? 'development',
        protectionMode: draft.protectionMode ?? 'ProtectedLocked',
        state: 'disconnected',
      }
    }
    const authMode = draftAuthMode(draft)
    const profile = (await native({
      id: draft.id,
      expectedRevision: draft.expectedRevision,
      name: draft.name,
      baseUrl: draft.baseUrl,
      defaultDatabase: draft.defaultDatabase,
      environment: draft.environment ?? 'development',
      authMode,
      username: authMode === 'BASIC' ? draft.username?.trim() ?? '' : '',
      protectionMode: draft.protectionMode ?? 'ProtectedLocked',
      secret: authMode === 'NONE' ? undefined : draft.secret || undefined,
    })) as NativeProfile
    return profileView(profile)
  },

  async closeConnection(profileId: string): Promise<void> {
    const native = method('CloseConnection')
    if (native) await native(profileId)
  },

  async deleteProfile(profileId: string, expectedRevision: string): Promise<void> {
    const native = method('DeleteProfile')
    if (native) await native(profileId, expectedRevision)
  },

  async openConnection(profileId: string): Promise<NativeConnectionSnapshot> {
    const get = method('GetConnectionState')
    if (get) {
      try {
        return (await get(profileId)) as NativeConnectionSnapshot
      } catch {
        // The profile is not open yet.
      }
    }
    const native = method('OpenConnection')
    if (native) return (await native(profileId)) as NativeConnectionSnapshot
    return {
      connectionId: profileId,
      connectionGeneration: '1',
      profileId,
      profileRevision: '1',
      version: '1.12.4',
      protection: {
        connectionId: profileId,
        connectionGeneration: '1',
        protectionRevision: '1',
        mode: 'ProtectedLocked',
      },
    }
  },

  async setProtection(snapshot: NativeConnectionSnapshot, unlock: boolean): Promise<ProtectionSnapshot> {
    const native = method(unlock ? 'UnlockProtection' : 'LockProtection')
    if (!native) {
      return {
        ...snapshot.protection,
        protectionRevision: (BigInt(snapshot.protection.protectionRevision) + 1n).toString(),
        mode: unlock ? 'ProtectedUnlocked' : 'ProtectedLocked',
      }
    }
    const result = (await native({
      connectionId: snapshot.connectionId,
      commandRequestId: globalThis.crypto.randomUUID(),
      expectedConnectionGeneration: snapshot.connectionGeneration,
      ...(unlock ? { expectedProtectionRevision: snapshot.protection.protectionRevision } : {}),
    })) as { snapshot: ProtectionSnapshot }
    return result.snapshot
  },

  async testConnection(profileId: string): Promise<{ ok: boolean; latencyMs: number; version: string }> {
    const native = method('TestSavedConnection')
    if (native) return (await native(profileId)) as { ok: boolean; latencyMs: number; version: string }
    await delay(650)
    return { ok: true, latencyMs: 42, version: '1.12.4' }
  },

  async testDraft(draft: ConnectionDraft): Promise<{ ok: boolean; latencyMs: number; version: string }> {
    const native = method('TestConnection')
    if (native) {
      const authMode = draftAuthMode(draft)
      return (await native({
        baseUrl: draft.baseUrl,
        authMode,
        username: authMode === 'BASIC' ? draft.username?.trim() ?? '' : '',
        secret: authMode === 'NONE' ? '' : draft.secret ?? '',
      })) as { ok: boolean; latencyMs: number; version: string }
    }
    await delay(650)
    return { ok: true, latencyMs: 42, version: '1.12.4' }
  },

  async startReadQuery(
    request: QueryRequest,
    onStarted?: QueryStartedHandler,
    onSnapshot?: QueryStartedHandler,
  ): Promise<BridgeQueryResponse> {
    const native = method('StartReadQuery')
    if (native) {
      const started = (await native(request)) as NativeQuerySession
      onStarted?.(started.id, started.stateRevision)
      const session = await waitForQuery(started, onSnapshot)
      return {
        sessionId: session.id,
        state: queryState(session.state),
        result: await loadNativeResult(session),
        publicErrorCode: session.publicErrorCode,
        publicSafeMessage: session.publicSafeMessage,
      }
    }
    onStarted?.(request.clientRequestId, '1')
    await delay(900)
    if (/\b(delete|drop|create|alter|grant|revoke|into)\b/i.test(request.query)) {
      return {
        sessionId: request.clientRequestId,
        state: 'failed',
        publicErrorCode: 'READ_ONLY_STATEMENT_REQUIRED',
        publicSafeMessage: '当前执行入口只接受只读 InfluxQL。',
      }
    }
    return { sessionId: request.clientRequestId, state: 'succeeded', result: mockResult() }
  },

  async cancelQuery(sessionId: string, expectedStateRevision: string): Promise<void> {
    await invokeQueryCommand('CancelQuery', sessionId, expectedStateRevision)
  },

  async previewMutation(
    profileId: string,
    request: MutationPreviewRequest,
    mockProtection?: ProtectionState,
  ): Promise<MutationPreview> {
    const native = method('PreviewMutation')
    if (native) return (await native(profileId, request)) as MutationPreview
    await delay(180)
    return mockMutationPreview(request, mockProtection)
  },

  async executeMutation(
    profileId: string,
    request: MutationExecuteRequest,
  ): Promise<MutationOperationResult> {
    const native = method('ExecuteMutation')
    if (native) {
      const started = (await native(profileId, request)) as NativeOperation
      return mutationOperationResult(await waitForOperation(started))
    }
    if (!request.previewToken.startsWith('mock-preview-')) throw new Error('MUTATION_PREVIEW_EXPIRED')
    await delay(360)
    return {
      id: `operation-${globalThis.crypto.randomUUID()}`,
      state: 'SUCCEEDED',
      terminal: true,
    }
  },

  async inspectImportFile(file: File, explicitPath = ''): Promise<{
    sourcePath: string
    sha256: string
    sizeBytes: string
  }> {
    const fileWithPath = file as File & { path?: string }
    const sourcePath = (fileWithPath.path ?? explicitPath).trim()
    if (this.isNative()) {
      if (!sourcePath) throw new Error('IMPORT_SOURCE_PATH_UNAVAILABLE')
      const native = method('InspectImportSource')
      if (!native) throw new Error('IMPORT_SOURCE_INSPECTION_REQUIRES_NATIVE')
      return (await native(sourcePath)) as ImportSourceInspection
    }
    if (file.size > 64 * 1024 * 1024) throw new Error('IMPORT_SOURCE_INSPECTION_REQUIRES_NATIVE')
    if (!globalThis.crypto?.subtle) throw new Error('IMPORT_SOURCE_DIGEST_UNAVAILABLE')
    const digest = await globalThis.crypto.subtle.digest('SHA-256', await file.arrayBuffer())
    const sha256 = Array.from(new Uint8Array(digest), (value) => value.toString(16).padStart(2, '0')).join('')
    return {
      sourcePath: sourcePath || file.name,
      sha256,
      sizeBytes: String(file.size),
    }
  },

  async selectImportSource(): Promise<ImportSourceInspection> {
    const native = method('SelectImportSource')
    if (!native) throw new Error('IMPORT_SOURCE_SELECTION_UNAVAILABLE')
    return (await native()) as ImportSourceInspection
  },

  async selectExportDirectory(): Promise<string> {
    const native = method('SelectExportDirectory')
    if (!native) throw new Error('EXPORT_TARGET_SELECTION_UNAVAILABLE')
    return (await native()) as string
  },

  async exportQueryResult(request: QueryResultExportRequest): Promise<QueryResultExportResult> {
    const native = method('ExportQueryResultCSV')
    if (!native) throw new Error('QUERY_RESULT_EXPORT_REQUIRES_NATIVE')
    return (await native(request)) as QueryResultExportResult
  },

  async listRecoverableImports(profileId: string, limit = 100): Promise<ImportJob[]> {
    const native = method('ListRecoverableImports')
    if (native) return (await native(profileId, limit)) as ImportJob[]
    if (this.isNative()) throw new Error('LISTRECOVERABLEIMPORTS_UNAVAILABLE')
    return mockImportJobs.filter((job) => !profileId || job.profileId === profileId)
  },

  async getImportJob(jobId: string): Promise<ImportJob> {
    const native = method('GetImportJob')
    if (native) return (await native(jobId)) as ImportJob
    if (this.isNative()) throw new Error('GETIMPORTJOB_UNAVAILABLE')
    const job = mockImportJobs.find((candidate) => candidate.task.id === jobId)
    if (!job) throw new Error('TASK_NOT_FOUND')
    return job
  },

  async preflightImport(request: ImportPreflightRequest): Promise<ImportPreflightResponse> {
    const native = method('PreflightImport')
    if (native) return (await invokeIdempotent(native, [request])) as ImportPreflightResponse
    if (this.isNative()) throw new Error('PREFLIGHTIMPORT_UNAVAILABLE')
    const id = `import-${globalThis.crypto.randomUUID()}`
    const job: ImportJob = {
      task: mockTask(id, 'IMPORT', 'READY'),
      profileId: request.profileId,
      checkpointDigest: `mock-checkpoint-${id}`,
      checkpoint: {
        sequence: '0',
        logicalOffset: '0',
        adaptiveMaxPoints: '5000',
        adaptiveMaxBytes: '5242880',
        sourceSha256: request.source.sha256,
        stagingSha256: `mock-staging-${id}`,
        normalizationVersion: 'canonical-lp-v1',
        specDigest: `mock-spec-${id}`,
        targetDigest: `mock-target-${id}`,
        lossy: false,
      },
      pauseRequested: false,
    }
    replaceMockImport(job)
    return { job, replayed: false, ready: true }
  },

  async previewImportRun(profileId: string, request: ImportRunRequest): Promise<ImportRunPreview> {
    const native = method('PreviewImportRun')
    if (native) return (await native(profileId, request)) as ImportRunPreview
    if (this.isNative()) throw new Error('PREVIEWIMPORTRUN_UNAVAILABLE')
    const job = await this.getImportJob(request.jobId)
    const requiresGrant = request.action !== 'RESOLVE' || request.decision === 'REPLAY_EXACT' ||
      ((request.decision === 'ASSUME_COMMITTED' || request.decision === 'ACCEPT_PARTIAL') &&
        request.afterResolution === 'CONTINUE')
    return {
      jobId: job.task.id,
      state: job.task.state,
      checkpointDigest: job.checkpointDigest,
      logicalOffset: job.checkpoint.logicalOffset,
      adaptiveMaxPoints: job.checkpoint.adaptiveMaxPoints,
      adaptiveMaxBytes: job.checkpoint.adaptiveMaxBytes,
      targetDigest: job.checkpoint.targetDigest,
      executable: true,
      importRunGrant: requiresGrant
        ? { token: `mock-grant-${globalThis.crypto.randomUUID()}`, expiresAt: new Date(Date.now() + 120_000).toISOString() }
        : undefined,
    }
  },

  async startImport(profileId: string, request: AuthorizedImportRunRequest): Promise<ImportJob> {
    const native = method('StartImport')
    if (native) return (await invokeIdempotent(native, [profileId, request])) as ImportJob
    if (this.isNative()) throw new Error('STARTIMPORT_UNAVAILABLE')
    const job = await this.getImportJob(request.jobId)
    const next = { ...job, task: nextMockTask(job.task, 'RUNNING'), activeRunSegmentId: `segment-${request.commandRequestId}` }
    replaceMockImport(next)
    return next
  },

  async resumeImport(profileId: string, request: AuthorizedImportRunRequest): Promise<ImportJob> {
    const native = method('ResumeImport')
    if (native) return (await invokeIdempotent(native, [profileId, request])) as ImportJob
    if (this.isNative()) throw new Error('RESUMEIMPORT_UNAVAILABLE')
    return this.startImport(profileId, request)
  },

  async resolveImportBatch(profileId: string, request: AuthorizedImportResolveRequest): Promise<ImportJob> {
    const native = method('ResolveImportBatch')
    if (native) return (await invokeIdempotent(native, [profileId, request])) as ImportJob
    if (this.isNative()) throw new Error('RESOLVEIMPORTBATCH_UNAVAILABLE')
    const job = await this.getImportJob(request.jobId)
    const continuing = request.decision !== 'ABORT' && request.afterResolution === 'CONTINUE'
    const state = request.decision === 'ABORT' ? 'ABORTED' : continuing ? 'RUNNING' : 'PAUSED_SAFE'
    const terminal = request.decision === 'ABORT'
    const next = {
      ...job,
      task: nextMockTask(job.task, state, terminal),
      activeRunSegmentId: continuing ? `segment-${request.commandRequestId}` : undefined,
      openIncident: undefined,
    }
    replaceMockImport(next)
    return next
  },

  async runImportNext(
    profileId: string,
    jobId: string,
    database: string,
    retentionPolicy = '',
  ): Promise<ImportBatchResult> {
    const native = method('RunImportNext')
    if (native) return (await native(profileId, jobId, database, retentionPolicy)) as ImportBatchResult
    if (this.isNative()) throw new Error('RUNIMPORTNEXT_UNAVAILABLE')
    const job = await this.getImportJob(jobId)
    const next = {
      ...job,
      task: nextMockTask(job.task, 'SUCCEEDED', true),
      checkpoint: { ...job.checkpoint, logicalOffset: '68157440', lastDisposition: 'ACKED' },
      activeRunSegmentId: undefined,
    }
    replaceMockImport(next)
    return { job: next, outcome: 'SUCCEEDED', startOffset: job.checkpoint.logicalOffset, endOffset: '68157440', pointCount: '5000' }
  },

  async cancelImport(jobId: string, envelope: TaskCommandEnvelope): Promise<ImportJob> {
    const native = method('CancelImport')
    if (native) return (await invokeIdempotent(native, [jobId, envelope])) as ImportJob
    if (this.isNative()) throw new Error('CANCELIMPORT_UNAVAILABLE')
    const job = await this.getImportJob(jobId)
    const next = { ...job, task: nextMockTask(job.task, 'CANCELED', true), activeRunSegmentId: undefined }
    replaceMockImport(next)
    return next
  },

  async cleanupImport(jobId: string, envelope: TaskCommandEnvelope): Promise<ImportJob> {
    const native = method('CleanupImport')
    if (native) return (await invokeIdempotent(native, [jobId, envelope])) as ImportJob
    if (this.isNative()) throw new Error('CLEANUPIMPORT_UNAVAILABLE')
    const job = await this.getImportJob(jobId)
    const next = { ...job, task: nextMockTask(job.task, job.task.state, job.task.terminal) }
    replaceMockImport(next)
    return next
  },

  async listExports(profileId: string, state = '', limit = 100): Promise<ExportJob[]> {
    const native = method('ListExports')
    if (native) return (await native({ profileId, state, limit })) as ExportJob[]
    if (this.isNative()) throw new Error('LISTEXPORTS_UNAVAILABLE')
    return mockExportJobs.filter((job) => (!profileId || job.profileId === profileId) && (!state || job.task.state === state))
  },

  async getExportJob(jobId: string): Promise<ExportJob> {
    const native = method('GetExportJob')
    if (native) return (await native(jobId)) as ExportJob
    if (this.isNative()) throw new Error('GETEXPORTJOB_UNAVAILABLE')
    const job = mockExportJobs.find((candidate) => candidate.task.id === jobId)
    if (!job) throw new Error('TASK_NOT_FOUND')
    return job
  },

  async startExport(request: StartExportRequest): Promise<ExportJob> {
    const native = method('StartExport')
    if (native) return (await invokeIdempotent(native, [request])) as ExportJob
    if (this.isNative()) throw new Error('STARTEXPORT_UNAVAILABLE')
    const id = `export-${globalThis.crypto.randomUUID()}`
    const job: ExportJob = {
      task: mockTask(id, 'EXPORT', 'QUEUED'),
      profileId: request.profileId,
      profileRevision: '1',
      connectionId: request.profileId,
      connectionGeneration: '1',
      specDigest: `mock-export-spec-${id}`,
      targetDigest: `mock-export-target-${id}`,
      targetVolumeId: 'mock-volume',
      cancelRequested: false,
      retained: true,
      fragments: [],
    }
    replaceMockExport(job)
    return job
  },

  async restartExport(jobId: string, envelope: TaskCommandEnvelope): Promise<ExportJob> {
    const native = method('RestartExport')
    if (native) return (await invokeIdempotent(native, [jobId, envelope])) as ExportJob
    if (this.isNative()) throw new Error('RESTARTEXPORT_UNAVAILABLE')
    const job = await this.getExportJob(jobId)
    const next = { ...job, task: nextMockTask(job.task, 'QUEUED'), cancelRequested: false }
    replaceMockExport(next)
    return next
  },

  async cancelExport(jobId: string, envelope: TaskCommandEnvelope): Promise<ExportJob> {
    const native = method('CancelExport')
    if (native) return (await invokeIdempotent(native, [jobId, envelope])) as ExportJob
    if (this.isNative()) throw new Error('CANCELEXPORT_UNAVAILABLE')
    const job = await this.getExportJob(jobId)
    const next = { ...job, task: nextMockTask(job.task, 'CANCELED', true), cancelRequested: true }
    replaceMockExport(next)
    return next
  },

  async cleanupExport(jobId: string, envelope: TaskCommandEnvelope): Promise<ExportJob> {
    const native = method('CleanupExport')
    if (native) return (await invokeIdempotent(native, [jobId, envelope])) as ExportJob
    if (this.isNative()) throw new Error('CLEANUPEXPORT_UNAVAILABLE')
    const job = await this.getExportJob(jobId)
    const next = { ...job, task: nextMockTask(job.task, 'CANCELED', true), retained: false }
    replaceMockExport(next)
    return next
  },
}
