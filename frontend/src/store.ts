import { create } from 'zustand'
import { bridge } from './bridge'
import type { ConnectionDraft, NativeConnectionSnapshot } from './bridge'
import { defaultMeasurementQuery } from './influxql'
import { mockConnections, mockSchema, mockTasks } from './mockData'
import type {
  ConnectionProfile,
  MutationOperationResult,
  MutationPhase,
  MutationPreview,
  MeasurementSelection,
  ProtectionState,
  QueryResult,
  QueryState,
  QueryTab,
  SchemaDatabase,
  SchemaMeasurement,
  TransferTask,
} from './types'

const initialQuery = `SELECT mean("usage_user") AS "usage_user",
       sum("requests") AS "requests"
FROM "telemetry"."autogen"."cpu"
WHERE time >= now() - 6h
  AND "region" = 'sh-east'
GROUP BY time(5m), "host" fill(null)`

const makeId = (prefix: string) => `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`
const makeRequestId = () => globalThis.crypto.randomUUID()
let measurementSchemaRequest = 0
let schemaLoadRequest = 0
const measurementSchemaPromises = new Map<string, Promise<SchemaMeasurement>>()

interface WorkbenchState {
  theme: 'light' | 'dark'
  activeView: 'query' | 'connections' | 'tasks' | 'settings'
  assistantOpen: boolean
  taskDrawerOpen: boolean
  connectionDialogOpen: boolean
  connectionDialogProfileId?: string
  protection: ProtectionState
  unlockUntil?: number
  connections: ConnectionProfile[]
  activeConnectionId: string
  activeConnection?: NativeConnectionSnapshot
  schema: SchemaDatabase[]
  schemaLoading: boolean
  selectedMeasurement?: MeasurementSelection
  measurementSchemaLoading: boolean
  measurementSchemaError?: string
  tabs: QueryTab[]
  activeTabId: string
  queryState: QueryState
  queryStartedAt?: number
  activeQueryRequestId?: string
  activeSessionId?: string
  activeSessionStateRevision?: string
  result?: QueryResult
  queryError?: string
  mutationDialogOpen: boolean
  mutationPhase: MutationPhase
  mutationFlowRevision: number
  mutationPreview?: MutationPreview
  mutationResult?: MutationOperationResult
  mutationSafeMessage?: string
  tasks: TransferTask[]
  initialize: () => Promise<void>
  setTheme: (theme: 'light' | 'dark') => void
  setActiveView: (view: WorkbenchState['activeView']) => void
  toggleAssistant: () => void
  setTaskDrawerOpen: (open: boolean) => void
  setConnectionDialogOpen: (open: boolean, profileId?: string) => void
  setActiveConnection: (id: string) => void
  loadSchema: () => Promise<void>
  selectMeasurement: (database: string, measurement: string) => Promise<void>
  updateQuery: (query: string) => void
  updateQueryDatabase: (database: string) => void
  addQueryTab: () => void
  closeQueryTab: (id: string) => void
  setActiveTab: (id: string) => void
  executeQuery: () => Promise<void>
  cancelQuery: () => Promise<void>
  previewMutation: () => Promise<void>
  executeMutation: (confirmation: string) => Promise<void>
  closeMutationDialog: () => void
  toggleProtection: () => Promise<void>
  saveConnection: (draft: ConnectionDraft) => Promise<boolean>
  deleteConnection: (id: string) => Promise<void>
  addTask: (kind: 'import' | 'export') => void
  toggleTaskPause: (id: string) => void
  replaceTransferTasks: (tasks: TransferTask[]) => void
}

export const useWorkbenchStore = create<WorkbenchState>((set, get) => ({
  theme: 'light',
  activeView: 'query',
  assistantOpen: false,
  taskDrawerOpen: false,
  connectionDialogOpen: false,
  protection: 'locked',
  connections: bridge.isNative() ? [] : mockConnections,
  activeConnectionId: bridge.isNative() ? '' : mockConnections[0].id,
  schema: bridge.isNative() ? [] : mockSchema,
  schemaLoading: false,
  selectedMeasurement: bridge.isNative() ? undefined : { database: 'telemetry', measurement: 'cpu' },
  measurementSchemaLoading: false,
  tabs: [
    {
      id: 'query-1',
      title: 'CPU 使用率',
      query: initialQuery,
      database: 'telemetry',
      dirty: false,
    },
  ],
  activeTabId: 'query-1',
  queryState: 'idle',
  mutationDialogOpen: false,
  mutationPhase: 'idle',
  mutationFlowRevision: 0,
  tasks: bridge.isNative() ? [] : mockTasks,

  initialize: async () => {
    if (!bridge.isNative()) return
    const request = ++schemaLoadRequest
    set({ tasks: [] })
    try {
      const connections = await bridge.listProfiles()
      if (request !== schemaLoadRequest) return
      if (connections.length === 0) {
        measurementSchemaRequest += 1
        set({
          connections, activeConnectionId: '', activeView: 'connections', schema: [],
          selectedMeasurement: undefined, measurementSchemaLoading: false,
          measurementSchemaError: undefined,
        })
        return
      }
      const activeConnectionId = connections[0].id
      set({ connections, activeConnectionId, schema: [], schemaLoading: true })
      const activeConnection = await bridge.openConnection(activeConnectionId)
      if (request !== schemaLoadRequest || get().activeConnectionId !== activeConnectionId) return
      const schema = await bridge.loadSchema(activeConnectionId)
      if (request !== schemaLoadRequest || get().activeConnectionId !== activeConnectionId) return
      const database = preferredDatabase(connections[0], schema)
      set((state) => ({
        activeConnection,
        schema,
        schemaLoading: false,
        protection: protectionState(activeConnection),
        unlockUntil: activeConnection.protection.unlockedUntil
          ? new Date(activeConnection.protection.unlockedUntil).getTime()
          : undefined,
        tabs: database ? setActiveTabDatabase(state.tabs, state.activeTabId, database) : state.tabs,
        connections: connections.map((item) =>
          item.id === activeConnectionId
            ? { ...item, state: 'connected', version: activeConnection.version }
            : item,
        ),
      }))
    } catch {
      if (request !== schemaLoadRequest) return
      set({ activeView: 'connections', schemaLoading: false, queryError: '无法打开已保存的连接。' })
    }
  },

  setTheme: (theme) => {
    document.documentElement.dataset.theme = theme
    set({ theme })
  },
  setActiveView: (activeView) => set({ activeView }),
  toggleAssistant: () => set((state) => ({ assistantOpen: !state.assistantOpen })),
  setTaskDrawerOpen: (taskDrawerOpen) => set({ taskDrawerOpen }),
  setConnectionDialogOpen: (connectionDialogOpen, connectionDialogProfileId) => set({
    connectionDialogOpen,
    connectionDialogProfileId: connectionDialogOpen ? connectionDialogProfileId : undefined,
  }),
  setActiveConnection: (activeConnectionId) => {
    const schemaRequest = ++schemaLoadRequest
    measurementSchemaRequest += 1
    set({
      activeConnectionId, schema: [], result: undefined, queryState: 'idle',
      schemaLoading: true,
      selectedMeasurement: undefined, measurementSchemaLoading: false,
      measurementSchemaError: undefined,
    })
    void (async () => {
      try {
        const activeConnection = await bridge.openConnection(activeConnectionId)
        if (schemaRequest !== schemaLoadRequest || get().activeConnectionId !== activeConnectionId) return
        const schema = await bridge.loadSchema(activeConnectionId)
        if (schemaRequest !== schemaLoadRequest || get().activeConnectionId !== activeConnectionId) return
        const profile = get().connections.find((item) => item.id === activeConnectionId)
        const database = preferredDatabase(profile, schema)
        set((state) => ({
          activeConnection,
          schema,
          schemaLoading: false,
          activeView: 'query',
          protection: protectionState(activeConnection),
          unlockUntil: activeConnection.protection.unlockedUntil
            ? new Date(activeConnection.protection.unlockedUntil).getTime()
            : undefined,
          tabs: database ? setActiveTabDatabase(state.tabs, state.activeTabId, database) : state.tabs,
          connections: state.connections.map((item) =>
            item.id === activeConnectionId
              ? { ...item, state: 'connected', version: activeConnection.version }
              : item,
          ),
        }))
      } catch {
        if (schemaRequest !== schemaLoadRequest || get().activeConnectionId !== activeConnectionId) return
        set((state) => ({
          schemaLoading: false,
          queryError: '无法打开所选连接。',
          connections: state.connections.map((item) =>
            item.id === activeConnectionId ? { ...item, state: 'disconnected' } : item,
          ),
        }))
      }
    })()
  },
  loadSchema: async () => {
    if (get().schemaLoading) return
    const profileId = get().activeConnectionId
    if (!profileId) return
    const request = ++schemaLoadRequest
    measurementSchemaRequest += 1
    set({
      schemaLoading: true, selectedMeasurement: undefined,
      measurementSchemaLoading: false, measurementSchemaError: undefined,
    })
    try {
      const schema = await bridge.loadSchema(profileId)
      if (request === schemaLoadRequest && get().activeConnectionId === profileId) set({ schema })
    } finally {
      if (request === schemaLoadRequest && get().activeConnectionId === profileId) set({ schemaLoading: false })
    }
  },
  selectMeasurement: async (database, measurement) => {
    const profileId = get().activeConnectionId
    const request = ++measurementSchemaRequest
    const cached = get().schema
      .find((candidate) => candidate.name === database)
      ?.measurements.find((candidate) => candidate.name === measurement)
    set((state) => ({
      selectedMeasurement: { database, measurement },
      measurementSchemaLoading: !cached?.detailLoaded,
      measurementSchemaError: undefined,
      tabs: state.tabs.map((tab) => tab.id === state.activeTabId ? {
        ...tab,
        database,
        query: defaultMeasurementQuery(measurement),
        dirty: true,
      } : tab),
    }))
    if (!profileId) {
      set({ measurementSchemaLoading: false, measurementSchemaError: '未选择连接。' })
      return
    }
    if (cached?.detailLoaded) return
    try {
      const generation = get().activeConnection?.connectionGeneration ?? ''
      const key = `${profileId}\u0000${generation}\u0000${database}\u0000${measurement}`
      let pending = measurementSchemaPromises.get(key)
      if (!pending) {
        pending = bridge.getMeasurementSchema(profileId, database, measurement)
        measurementSchemaPromises.set(key, pending)
        void pending.finally(() => {
          if (measurementSchemaPromises.get(key) === pending) measurementSchemaPromises.delete(key)
        }).catch(() => undefined)
      }
      const detail = await pending
      const current = get()
      if (current.activeConnectionId !== profileId ||
          (current.activeConnection?.connectionGeneration ?? '') !== generation) return
      set((state) => ({
        schema: state.schema.map((databaseSchema) => databaseSchema.name === database ? {
          ...databaseSchema,
          measurements: databaseSchema.measurements.map((candidate) => candidate.name === measurement ? {
            name: candidate.name,
            fields: detail.fields.map((field) => ({ ...field })),
            tags: [...detail.tags],
            detailLoaded: true,
          } : candidate),
        } : databaseSchema),
        ...(request === measurementSchemaRequest &&
          state.selectedMeasurement?.database === database &&
          state.selectedMeasurement.measurement === measurement
          ? { measurementSchemaLoading: false }
          : {}),
      }))
    } catch {
      const current = get()
      if (request !== measurementSchemaRequest || current.activeConnectionId !== profileId ||
          current.selectedMeasurement?.database !== database ||
          current.selectedMeasurement.measurement !== measurement) return
      set({ measurementSchemaLoading: false, measurementSchemaError: '无法读取所选 Measurement 的字段与 Tag。' })
    }
  },
  updateQuery: (query) =>
    set((state) => ({
      tabs: state.tabs.map((tab) => (tab.id === state.activeTabId ? { ...tab, query, dirty: true } : tab)),
    })),
  updateQueryDatabase: (database) =>
    set((state) => ({
      tabs: state.tabs.map((tab) => tab.id === state.activeTabId ? { ...tab, database, dirty: true } : tab),
    })),
  addQueryTab: () => {
    const id = makeId('query')
    set((state) => ({
      tabs: [
        ...state.tabs,
        {
          id,
          title: `查询 ${state.tabs.length + 1}`,
          query: 'SELECT *\nFROM "telemetry"."autogen"."cpu"\nWHERE time >= now() - 1h\nLIMIT 100',
          database: 'telemetry',
          dirty: false,
        },
      ],
      activeTabId: id,
      result: undefined,
      queryState: 'idle',
    }))
  },
  closeQueryTab: (id) =>
    set((state) => {
      if (state.tabs.length === 1) return state
      const next = state.tabs.filter((tab) => tab.id !== id)
      return {
        tabs: next,
        activeTabId: state.activeTabId === id ? next[Math.max(0, next.length - 1)].id : state.activeTabId,
      }
    }),
  setActiveTab: (activeTabId) => set({ activeTabId, result: undefined, queryState: 'idle', queryError: undefined }),
  executeQuery: async () => {
    const state = get()
    const tab = state.tabs.find((candidate) => candidate.id === state.activeTabId)
    if (!tab || !tab.query.trim() || !state.activeConnectionId || state.queryState === 'running') return
    const clientRequestId = makeRequestId()
    set({
      queryState: 'running',
      queryStartedAt: Date.now(),
      activeQueryRequestId: clientRequestId,
      activeSessionId: undefined,
      activeSessionStateRevision: undefined,
      queryError: undefined,
      result: undefined,
    })
    try {
      const response = await bridge.startReadQuery(
        {
          clientRequestId,
          profileId: state.activeConnectionId,
          database: tab.database,
          query: tab.query,
        },
        (sessionId, stateRevision) => {
          const current = get()
          if (current.activeQueryRequestId !== clientRequestId) {
            void bridge.cancelQuery(sessionId, stateRevision).catch(() => undefined)
            return
          }
          set({ activeSessionId: sessionId, activeSessionStateRevision: stateRevision })
          if (current.queryState === 'canceled') {
            void bridge.cancelQuery(sessionId, stateRevision).catch(() => undefined)
          }
        },
        (sessionId, stateRevision) => {
          const current = get()
          if (current.activeQueryRequestId === clientRequestId && current.activeSessionId === sessionId) {
            set({ activeSessionStateRevision: stateRevision })
          }
        },
      )
      if (get().activeQueryRequestId !== clientRequestId || get().queryState === 'canceled') return
      set({
        queryState: response.state,
        activeSessionId: response.sessionId,
        result: response.result,
        queryError: response.publicSafeMessage,
      })
    } catch {
      set({ queryState: 'failed', queryError: '查询服务暂时不可用，请检查连接后重试。' })
    }
  },
  cancelQuery: async () => {
    const { activeSessionId: sessionId, activeSessionStateRevision: stateRevision } = get()
    set({ queryState: 'canceled' })
    if (sessionId && stateRevision) await bridge.cancelQuery(sessionId, stateRevision)
  },
  previewMutation: async () => {
    const state = get()
    const tab = state.tabs.find((candidate) => candidate.id === state.activeTabId)
    if (!tab || !tab.query.trim() || !state.activeConnectionId || state.queryState === 'running') return
    const flowRevision = state.mutationFlowRevision + 1
    set({
      mutationDialogOpen: true,
      mutationPhase: 'previewing',
      mutationFlowRevision: flowRevision,
      mutationPreview: undefined,
      mutationResult: undefined,
      mutationSafeMessage: undefined,
    })
    try {
      const preview = await bridge.previewMutation(
        state.activeConnectionId,
        { database: tab.database, query: tab.query },
        state.protection,
      )
      const current = get()
      if (current.mutationFlowRevision !== flowRevision || !current.mutationDialogOpen) return
      if (preview.executable && !preview.token) {
        set({ mutationPhase: 'error', mutationSafeMessage: '变更预览授权无效，请关闭后重新预览。' })
        return
      }
      set({ mutationPhase: 'preview', mutationPreview: preview })
    } catch {
      if (get().mutationFlowRevision !== flowRevision) return
      set({ mutationPhase: 'error', mutationSafeMessage: '无法生成变更预览，请确认语句、连接和保护状态。' })
    }
  },
  executeMutation: async (confirmation) => {
    const state = get()
    const preview = state.mutationPreview
    if (!state.mutationDialogOpen || state.mutationPhase !== 'preview' || !preview?.executable || !preview.token) return
    if (preview.confirmationRequired && (!preview.target || confirmation !== preview.target)) return
    const flowRevision = state.mutationFlowRevision
    const previewToken = preview.token
    const { token: _removedToken, ...previewWithoutToken } = preview
    set({
      mutationPhase: 'executing',
      mutationPreview: previewWithoutToken,
      mutationResult: undefined,
      mutationSafeMessage: undefined,
    })
    try {
      const result = await bridge.executeMutation(state.activeConnectionId, {
        clientRequestId: makeRequestId(),
        previewToken,
        confirmation: confirmation || undefined,
      })
      const current = get()
      if (current.mutationFlowRevision !== flowRevision || !current.mutationDialogOpen) return
      set({ mutationPhase: 'complete', mutationResult: result })
    } catch {
      const current = get()
      if (current.mutationFlowRevision !== flowRevision || !current.mutationDialogOpen) return
      set({ mutationPhase: 'error', mutationSafeMessage: '变更请求未被接受，请刷新保护状态并重新预览。' })
    }
  },
  closeMutationDialog: () =>
    set((state) => ({
      mutationDialogOpen: false,
      mutationPhase: 'idle',
      mutationFlowRevision: state.mutationFlowRevision + 1,
      mutationPreview: undefined,
      mutationResult: undefined,
      mutationSafeMessage: undefined,
    })),
  toggleProtection: async () => {
    const state = get()
    if (state.protection === 'permanent-readonly') return
    if (bridge.isNative() && state.activeConnection) {
      try {
        const protection = await bridge.setProtection(state.activeConnection, state.protection === 'locked')
        set({
          activeConnection: { ...state.activeConnection, protection },
          protection: protectionState({ ...state.activeConnection, protection }),
          unlockUntil: protection.unlockedUntil ? new Date(protection.unlockedUntil).getTime() : undefined,
        })
      } catch {
        set({ queryError: '保护状态已变化，请刷新后重试。' })
      }
      return
    }
    set((current) =>
      current.protection === 'locked'
        ? { protection: 'unlocked', unlockUntil: Date.now() + 10 * 60 * 1000 }
        : { protection: 'locked', unlockUntil: undefined },
    )
  },
  saveConnection: async (draft) => {
    const previous = draft.id ? get().connections.find((item) => item.id === draft.id) : undefined
    const wasOpen = Boolean(previous && (
      previous.state === 'connected' || get().activeConnection?.profileId === previous.id
    ))
    if (wasOpen && previous) await bridge.closeConnection(previous.id)

    let profile: ConnectionProfile
    try {
      profile = await bridge.saveProfile(draft)
    } catch (error) {
      if (wasOpen && previous) {
        try {
          const restored = await bridge.openConnection(previous.id)
          set((state) => ({
            ...(state.activeConnectionId === previous.id ? {
              activeConnection: restored,
              protection: protectionState(restored),
              unlockUntil: restored.protection.unlockedUntil
                ? new Date(restored.protection.unlockedUntil).getTime()
                : undefined,
            } : {}),
            connections: state.connections.map((item) =>
              item.id === previous.id ? { ...item, state: 'connected', version: restored.version } : item,
            ),
          }))
        } catch {
          set((state) => ({
            connections: state.connections.map((item) =>
              item.id === previous.id ? { ...item, state: 'disconnected' } : item,
            ),
          }))
        }
      }
      throw error
    }

    const schemaRequest = ++schemaLoadRequest
    set((state) => ({
      connections: state.connections.some((item) => item.id === profile.id)
        ? state.connections.map((item) => item.id === profile.id ? profile : item)
        : [profile, ...state.connections],
      activeConnectionId: profile.id,
      schemaLoading: true,
    }))

    try {
      const activeConnection = await bridge.openConnection(profile.id)
      if (schemaRequest !== schemaLoadRequest || get().activeConnectionId !== profile.id) return true
      const schema = await bridge.loadSchema(profile.id)
      if (schemaRequest !== schemaLoadRequest || get().activeConnectionId !== profile.id) return true
      const database = preferredDatabase(profile, schema)
      measurementSchemaRequest += 1
      set((state) => ({
        activeConnection,
        schema,
        schemaLoading: false,
        selectedMeasurement: undefined,
        measurementSchemaLoading: false,
        measurementSchemaError: undefined,
        connectionDialogOpen: false,
        connectionDialogProfileId: undefined,
        activeView: 'query',
        protection: protectionState(activeConnection),
        unlockUntil: activeConnection.protection.unlockedUntil
          ? new Date(activeConnection.protection.unlockedUntil).getTime()
          : undefined,
        tabs: database ? setActiveTabDatabase(state.tabs, state.activeTabId, database) : state.tabs,
        connections: state.connections.map((item) =>
          item.id === profile.id ? { ...item, state: 'connected', version: activeConnection.version } : item,
        ),
      }))
      return true
    } catch {
      if (schemaRequest !== schemaLoadRequest || get().activeConnectionId !== profile.id) return false
      set((state) => ({
        schemaLoading: false,
        connectionDialogOpen: false,
        connectionDialogProfileId: undefined,
        activeView: 'connections',
        queryError: '连接配置已保存，但无法连接到服务器。',
        connections: state.connections.map((item) =>
          item.id === profile.id ? { ...item, state: 'disconnected' } : item,
        ),
      }))
      return false
    }
  },
  deleteConnection: async (id) => {
    const state = get()
    const profile = state.connections.find((item) => item.id === id)
    if (!profile?.revision) throw new Error('PROFILE_REVISION_REQUIRED')
    const wasOpen = profile.state === 'connected' || state.activeConnection?.profileId === id
    if (wasOpen) await bridge.closeConnection(id)
    try {
      await bridge.deleteProfile(id, profile.revision)
    } catch (error) {
      if (wasOpen) {
        try {
          const restored = await bridge.openConnection(id)
          set((current) => ({
            ...(current.activeConnectionId === id ? {
              activeConnection: restored,
              protection: protectionState(restored),
              unlockUntil: restored.protection.unlockedUntil
                ? new Date(restored.protection.unlockedUntil).getTime()
                : undefined,
            } : {}),
            connections: current.connections.map((item) =>
              item.id === id ? { ...item, state: 'connected', version: restored.version } : item,
            ),
          }))
        } catch {
          set((current) => ({
            connections: current.connections.map((item) =>
              item.id === id ? { ...item, state: 'disconnected' } : item,
            ),
          }))
        }
      }
      throw error
    }

    const remaining = get().connections.filter((item) => item.id !== id)
    const deletedActive = get().activeConnectionId === id
    if (deletedActive) schemaLoadRequest += 1
    measurementSchemaRequest += 1
    set({
      connections: remaining,
      connectionDialogOpen: false,
      connectionDialogProfileId: undefined,
      ...(deletedActive ? {
        activeConnectionId: '',
        activeConnection: undefined,
        schema: [],
        selectedMeasurement: undefined,
        measurementSchemaLoading: false,
        measurementSchemaError: undefined,
        result: undefined,
        queryState: 'idle' as const,
        protection: 'locked' as const,
        unlockUntil: undefined,
        activeView: remaining.length === 0 ? 'connections' as const : get().activeView,
      } : {}),
    })
    if (deletedActive && remaining.length > 0) get().setActiveConnection(remaining[0].id)
  },
  addTask: (kind) => {
    if (bridge.isNative()) return
    const task: TransferTask = {
      id: makeId(kind),
      kind,
      title: kind === 'import' ? '待选择的数据文件' : 'cpu · 自定义范围',
      subtitle: kind === 'import' ? '等待预检' : '逻辑导出 · LP.GZ',
      progress: 0,
      processed: '0',
      total: '—',
      state: 'paused',
      updatedAt: '刚刚',
      revision: '1',
    }
    set((state) => ({ tasks: [task, ...state.tasks], taskDrawerOpen: true }))
  },
  toggleTaskPause: (id) => {
    if (bridge.isNative()) return
    set((state) => ({
      tasks: state.tasks.map((task) =>
        task.id === id
          ? { ...task, state: task.state === 'running' ? 'paused' : 'running', updatedAt: '刚刚' }
          : task,
      ),
    }))
  },
  replaceTransferTasks: (tasks) => set({ tasks }),
}))

function protectionState(connection: NativeConnectionSnapshot): ProtectionState {
  switch (connection.protection.mode) {
    case 'PermanentReadOnly':
      return 'permanent-readonly'
    case 'ProtectedUnlocked':
      return 'unlocked'
    default:
      return 'locked'
  }
}

function preferredDatabase(profile: ConnectionProfile | undefined, schema: SchemaDatabase[]): string {
  if (profile?.defaultDatabase && schema.some((database) => database.name === profile.defaultDatabase)) {
    return profile.defaultDatabase
  }
  return schema.find((database) => !database.name.startsWith('_'))?.name ?? schema[0]?.name ?? ''
}

function setActiveTabDatabase(tabs: QueryTab[], activeTabId: string, database: string): QueryTab[] {
  return tabs.map((tab) => tab.id === activeTabId ? { ...tab, database } : tab)
}
