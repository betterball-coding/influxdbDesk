import type {
  ConnectionProfile,
  QueryResult,
  SchemaDatabase,
  TransferTask,
  TypedScalar,
} from './types'

const exact = (
  kind: 'timestamp_ns' | 'int64' | 'uint64' | 'float64',
  decimalText: string,
): TypedScalar => ({ kind, decimalText })

export const mockConnections: ConnectionProfile[] = [
  {
    id: 'prod-east',
    revision: '1',
    name: 'Production East',
    url: 'https://influx.example.net:8086',
    defaultDatabase: 'telemetry',
    authMode: 'BASIC',
    username: 'operator',
    environment: 'production',
    protectionMode: 'ProtectedLocked',
    state: 'connected',
    version: '1.12.4',
    latencyMs: 38,
  },
  {
    id: 'lab-local',
    revision: '1',
    name: 'Local Lab',
    url: 'http://127.0.0.1:8086',
    defaultDatabase: 'operations',
    authMode: 'NONE',
    environment: 'development',
    protectionMode: 'ProtectedLocked',
    state: 'disconnected',
    version: '1.8.10',
  },
]

export const mockSchema: SchemaDatabase[] = [
  {
    name: 'telemetry',
    retentionPolicies: ['autogen', 'thirty_days', 'one_year'],
    measurements: [
      {
        name: 'cpu',
        fields: [
          { name: 'usage_system', type: 'float' },
          { name: 'usage_user', type: 'float' },
          { name: 'usage_idle', type: 'float' },
        ],
        tags: ['host', 'region', 'service'],
      },
      {
        name: 'http_requests',
        fields: [
          { name: 'duration_ms', type: 'float' },
          { name: 'status', type: 'integer' },
          { name: 'bytes', type: 'unsigned' },
        ],
        tags: ['host', 'method', 'path'],
      },
      {
        name: 'disk',
        fields: [
          { name: 'free', type: 'unsigned' },
          { name: 'used_percent', type: 'float' },
        ],
        tags: ['host', 'device'],
      },
    ],
  },
  {
    name: 'operations',
    retentionPolicies: ['autogen', 'audit_90d'],
    measurements: [
      {
        name: 'job_runtime',
        fields: [
          { name: 'elapsed_ms', type: 'integer' },
          { name: 'success', type: 'boolean' },
        ],
        tags: ['job', 'worker'],
      },
    ],
  },
]

const baseTimestamp = 1700000000123456788n

export const mockResult = (): QueryResult => ({
  sessionId: `session-${Date.now()}`,
  statementId: 0,
  seriesId: 'mock-cpu',
  seriesName: 'cpu',
  columns: [
    { key: 'time', label: 'time', kind: 'timestamp_ns', width: 220 },
    { key: 'host', label: 'host', kind: 'string', width: 140 },
    { key: 'region', label: 'region', kind: 'string', width: 110 },
    { key: 'usage_user', label: 'usage_user', kind: 'float64', width: 130 },
    { key: 'requests', label: 'requests', kind: 'int64', width: 150 },
  ],
  rows: Array.from({ length: 180 }, (_, index) => {
    const time = baseTimestamp + BigInt(index) * 15_000_000_000n
    const requests = 9007199254740992n + BigInt(index)
    return {
      id: `row-${index}`,
      sourceIndex: String(index),
      cells: {
        time: exact('timestamp_ns', time.toString()),
        host: { kind: 'string', value: `api-${String((index % 6) + 1).padStart(2, '0')}` },
        region: { kind: 'string', value: ['sh-east', 'sh-west', 'bj-core'][index % 3] },
        usage_user: exact('float64', (31 + Math.sin(index / 7) * 13 + (index % 5)).toFixed(4)),
        requests: exact('int64', requests.toString()),
      },
    }
  }),
  totalRows: '180',
  complete: true,
  elapsedMs: 184,
  scannedBytes: 428_901,
})

export const mockTasks: TransferTask[] = [
  {
    id: 'import-2407',
    kind: 'import',
    title: 'metrics-july.lp.gz',
    subtitle: 'telemetry / autogen',
    progress: 64,
    processed: '6,400,000',
    total: '10,000,000',
    speed: '84k points/s',
    state: 'running',
    updatedAt: '刚刚',
    revision: '18',
  },
  {
    id: 'export-2371',
    kind: 'export',
    title: 'cpu · 过去 24 小时',
    subtitle: '逻辑导出 · LP.GZ',
    progress: 100,
    processed: '2.8 GB',
    total: '2.8 GB',
    state: 'completed',
    updatedAt: '12 分钟前',
    revision: '44',
  },
]
