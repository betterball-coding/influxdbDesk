export type ScalarKind =
  | 'null'
  | 'string'
  | 'boolean'
  | 'timestamp_ns'
  | 'int64'
  | 'uint64'
  | 'float64'
  | 'numeric_text'

export type TypedScalar =
  | { kind: 'null' }
  | { kind: 'string'; value: string }
  | { kind: 'boolean'; value: boolean }
  | {
      kind: Exclude<ScalarKind, 'null' | 'string' | 'boolean'>
      decimalText: string
    }

export type ConnectionState = 'connected' | 'disconnected' | 'testing'
export type ProtectionState = 'permanent-readonly' | 'locked' | 'unlocked'

export interface ConnectionProfile {
  id: string
  revision?: string
  name: string
  url: string
  defaultDatabase?: string
  authMode?: 'NONE' | 'BASIC' | 'BEARER'
  username?: string
  allowInsecureAuth?: boolean
  environment: 'production' | 'staging' | 'development'
  protectionMode?: 'PermanentReadOnly' | 'ProtectedLocked'
  state: ConnectionState
  version?: string
  latencyMs?: number
}

export interface SchemaMeasurement {
  name: string
  fields: Array<{ name: string; type: 'float' | 'integer' | 'unsigned' | 'string' | 'boolean' }>
  tags: string[]
  detailLoaded?: boolean
}

export interface SchemaDatabase {
  name: string
  retentionPolicies: string[]
  measurements: SchemaMeasurement[]
}

export interface MeasurementSelection {
  database: string
  measurement: string
}

export interface QueryTab {
  id: string
  title: string
  query: string
  database: string
  dirty: boolean
}

export type QueryState = 'idle' | 'queued' | 'running' | 'succeeded' | 'canceled' | 'failed'

export interface ResultColumn {
  key: string
  label: string
  kind: ScalarKind | 'mixed'
  width?: number
}

export interface ResultRow {
  id: string
  sourceIndex?: string
  cells: Record<string, TypedScalar>
}

export interface QueryResult {
  sessionId: string
  statementId: number
  seriesId?: string
  seriesName: string
  columns: ResultColumn[]
  rows: ResultRow[]
  totalRows?: string
  complete: boolean
  elapsedMs: number
  scannedBytes: number
}

export interface QueryResultExportRequest {
  sessionId: string
  statementId: number
  seriesId: string
  allRows: boolean
  rowIndexes?: string[]
  suggestedName?: string
}

export interface QueryResultExportResult {
  path: string
  rowCount: string
}

export type TaskKind = 'import' | 'export'
export type TaskState = 'running' | 'paused' | 'completed' | 'failed' | 'needs-decision'

export interface TransferTask {
  id: string
  kind: TaskKind
  title: string
  subtitle: string
  progress: number
  processed: string
  total: string
  state: TaskState
  updatedAt: string
  revision: string
  speed?: string
  publicSafeMessage?: string
}

export interface QueryRequest {
  clientRequestId: string
  profileId: string
  database: string
  retentionPolicy?: string
  query: string
}

export interface BridgeQueryResponse {
  sessionId: string
  state: QueryState
  result?: QueryResult
  publicErrorCode?: string
  publicSafeMessage?: string
}

export interface MutationPreviewRequest {
  database: string
  retentionPolicy?: string
  query: string
}

export interface MutationPreview {
  canonicalQuery: string
  operationKind: string
  target?: string
  confirmationRequired: boolean
  executable: boolean
  token?: string
  expiresAt?: string
}

export interface MutationExecuteRequest {
  clientRequestId: string
  previewToken: string
  confirmation?: string
}

export interface MutationOperationResult {
  id: string
  state: string
  terminal: boolean
  publicErrorCode?: string
  publicSafeMessage?: string
}

export type MutationPhase = 'idle' | 'previewing' | 'preview' | 'executing' | 'complete' | 'error'

export interface TaskMeta {
  id: string
  kind: string
  state: string
  terminal: boolean
  snapshotRevision: string
  stateRevision: string
  lastEventSeq: string
  createdAt: string
  updatedAt: string
  terminalAt?: string
  publicErrorCode?: string
  publicSafeMessage?: string
  archived: boolean
  resultAvailable: boolean
}

export interface TaskEvent {
  schemaVersion: number
  seq: string
  kind: string
  id: string
  resourceRevision: string
  state: string
  changeType: string
  occurredAt: string
}

export interface ImportCheckpoint {
  sequence: string
  parentDigest?: string
  logicalOffset: string
  lastSettledBatchId?: string
  lastDisposition?: string
  adaptiveMaxPoints: string
  adaptiveMaxBytes: string
  sourceSha256: string
  stagingSha256: string
  normalizationVersion: string
  specDigest: string
  targetDigest: string
  lossy: boolean
}

export interface ImportIncident {
  incidentId: string
  kind: 'PARTIAL' | 'UNKNOWN'
  status: 'OPEN' | 'RESOLVED' | 'ABORTED'
  parentCheckpointDigest: string
  runSegmentId: string
  batchId: string
  attemptId: string
  startOffset: string
  endOffset: string
  payloadDigest: string
  replayParentIncidentId?: string
  resolution?: string
  resolvedAt?: string
  resolutionCommandRequestId?: string
}

export interface ImportJob {
  task: TaskMeta
  profileId: string
  checkpointDigest: string
  checkpoint: ImportCheckpoint
  activeRunSegmentId?: string
  pauseRequested: boolean
  openIncident?: ImportIncident
}

export type ImportFormat = 'LP' | 'TXT' | 'LP_GZ' | 'TYPED_JSONL' | 'CSV'
export type CSVFieldKind = 'string' | 'boolean' | 'int64' | 'uint64' | 'float64'

export interface CSVFieldMapping {
  target: string
  kind: CSVFieldKind
}

export interface CSVMapping {
  staticMeasurement?: string
  measurementColumn?: string
  timestampColumn: string
  tagColumns?: Record<string, string>
  fieldColumns: Record<string, CSVFieldMapping>
}

export interface ImportPreflightRequest {
  clientRequestId: string
  profileId: string
  sourcePath: string
  source: {
    sha256: string
    sizeBytes: string
  }
  format: ImportFormat
  target: {
    database: string
    retentionPolicy?: string
  }
  csvMapping?: CSVMapping
  numericTextMappings?: Array<{
    measurement: string
    field: string
    kind: 'int64' | 'uint64' | 'float64'
  }>
}

export interface ImportPreflightResponse {
  job: ImportJob
  replayed: boolean
  ready: boolean
}

export interface ImportSourceInspection {
  sourcePath: string
  displayName: string
  sha256: string
  sizeBytes: string
}

export type ImportRunAction = 'START' | 'RESUME' | 'RESOLVE'
export type ImportIncidentDecision = 'REPLAY_EXACT' | 'ASSUME_COMMITTED' | 'ACCEPT_PARTIAL' | 'ABORT'
export type ImportAfterResolution = 'CONTINUE' | 'PAUSE'

export interface ImportRunRequest {
  jobId: string
  commandRequestId: string
  expectedStateRevision: string
  action: ImportRunAction
  decision?: ImportIncidentDecision
  afterResolution?: ImportAfterResolution
}

export interface ImportRunPreview {
  jobId: string
  state: string
  checkpointDigest: string
  logicalOffset: string
  adaptiveMaxPoints: string
  adaptiveMaxBytes: string
  targetDigest: string
  executable: boolean
  importRunGrant?: {
    token: string
    expiresAt: string
  }
}

export interface AuthorizedImportRunRequest {
  jobId: string
  commandRequestId: string
  expectedStateRevision: string
  action: 'START' | 'RESUME'
  importRunGrant: string
}

export interface AuthorizedImportResolveRequest {
  jobId: string
  commandRequestId: string
  expectedStateRevision: string
  incidentId: string
  parentCheckpointDigest: string
  decision: ImportIncidentDecision
  afterResolution?: ImportAfterResolution
  importRunGrant?: string
}

export interface ImportBatchResult {
  job: ImportJob
  outcome: string
  attemptId?: string
  batchId?: string
  startOffset?: string
  endOffset?: string
  pointCount?: string
}

export interface ExportFragment {
  fragmentId: string
  jobId: string
  ordinal: string
  kind: string
  state: string
  startNs?: string
  endNs?: string
  compressedSize?: string
  uncompressedSize?: string
  checksumSha256?: string
  reusable: boolean
  createdAt: string
  updatedAt: string
}

export interface ExportJob {
  task: TaskMeta
  profileId: string
  profileRevision: string
  connectionId: string
  connectionGeneration: string
  specDigest: string
  targetDigest: string
  targetVolumeId: string
  cancelRequested: boolean
  retained: boolean
  cleanupFinalState?: string
  fragments?: ExportFragment[]
}

export interface StartExportRequest {
  clientRequestId: string
  profileId: string
  targetDirectory: string
  database: string
  retentionPolicy?: string
  measurements: string[]
  startNs: string
  endNs: string
  strict: boolean
}

export interface TaskCommandEnvelope {
  commandRequestId: string
  expectedStateRevision: string
}
