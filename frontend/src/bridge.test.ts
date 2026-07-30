import { afterEach, describe, expect, it, vi } from 'vitest'
import { bridge } from './bridge'
import { useWorkbenchStore } from './store'

const initialWorkbenchState = useWorkbenchStore.getState()

afterEach(() => {
  delete window.go
  delete window.runtime
  useWorkbenchStore.setState(initialWorkbenchState, true)
})

describe('native bridge', () => {
  it('does not touch the Wails runtime in browser mock mode', () => {
    let runtimeReads = 0
    Object.defineProperty(window, 'runtime', {
      configurable: true,
      get: () => {
        runtimeReads += 1
        throw new Error('browser mock must not read window.runtime')
      },
    })

    expect(bridge.isNative()).toBe(false)
    expect(bridge.supportsTaskEventSync()).toBe(false)
    const unsubscribe = bridge.subscribeTaskChanges(() => undefined)
    unsubscribe()
    expect(runtimeReads).toBe(0)
  })

  it('preserves exact scalar text through session and page APIs', async () => {
    window.go = {
      main: {
        App: {
          StartReadQuery: vi.fn(async () => ({
            id: 'session-1',
            state: 'SUCCEEDED',
            terminal: true,
            stateRevision: '3',
            createdAt: '2026-07-23T00:00:00Z',
            updatedAt: '2026-07-23T00:00:00.125Z',
            resultAvailable: true,
            complete: true,
          })),
          ListResultSeries: vi.fn(async () => ([{
            id: 'series-1',
            statementId: 0,
            measurement: 'cpu',
            columns: ['time', 'value'],
          }])),
          GetResultPage: vi.fn(async () => ({
            rows: [[
              { kind: 'timestamp_ns', decimalText: '1700000000000000001' },
              { kind: 'int64', decimalText: '9007199254740993' },
            ]],
            eof: true,
          })),
        },
      },
    }

    const response = await bridge.startReadQuery({
      clientRequestId: '550e8400-e29b-41d4-a716-446655440000',
      profileId: 'profile-1',
      database: 'metrics',
      query: 'SELECT value FROM cpu',
    })

    expect(response.state).toBe('succeeded')
    expect(response.result?.rows[0].cells.value).toEqual({
      kind: 'int64',
      decimalText: '9007199254740993',
    })
    expect(response.result?.elapsedMs).toBe(125)
  })

  it('maps persisted profiles without exposing credential references', async () => {
    window.go = {
      main: {
        App: {
          ListProfiles: vi.fn(async () => ([{
            id: 'profile-1',
            revision: '7',
            name: 'Production',
            baseUrl: 'https://influx.example.test',
            defaultDatabase: 'metrics',
            environment: 'production',
            authMode: 'BASIC',
            username: 'operator',
            protectionMode: 'ProtectedLocked',
            credentialRef: 'must-not-cross-ui-boundary',
          }])),
        },
      },
    }

    const profiles = await bridge.listProfiles()
    expect(profiles).toEqual([{
      id: 'profile-1',
      revision: '7',
      name: 'Production',
      url: 'https://influx.example.test',
      defaultDatabase: 'metrics',
      authMode: 'BASIC',
      environment: 'production',
      username: 'operator',
      protectionMode: 'ProtectedLocked',
      state: 'disconnected',
    }])
    expect(JSON.stringify(profiles)).not.toContain('credentialRef')
  })

  it('saves direct connection fields with automatic Basic authentication', async () => {
    const saveProfile = vi.fn(async (input: unknown) => {
      const value = input as Record<string, string | undefined>
      return {
        id: value.id ?? 'profile-new',
        revision: value.expectedRevision ? '8' : '1',
        name: value.name,
        baseUrl: value.baseUrl,
        defaultDatabase: value.defaultDatabase,
        environment: value.environment,
        authMode: value.authMode,
        username: value.username,
        protectionMode: value.protectionMode,
      }
    })
    window.go = { main: { App: { SaveProfile: saveProfile } } }

    const profile = await bridge.saveProfile({
      id: 'profile-1',
      expectedRevision: '7',
      name: 'Data engine',
      baseUrl: 'http://192.168.2.6:8086',
      defaultDatabase: 'data_engine',
      username: 'operator',
      environment: 'production',
      authMode: 'BASIC',
      protectionMode: 'ProtectedLocked',
    })

    expect(saveProfile).toHaveBeenCalledWith({
      id: 'profile-1',
      expectedRevision: '7',
      name: 'Data engine',
      baseUrl: 'http://192.168.2.6:8086',
      defaultDatabase: 'data_engine',
      environment: 'production',
      authMode: 'BASIC',
      username: 'operator',
      protectionMode: 'ProtectedLocked',
      secret: undefined,
    })
    expect(profile).toMatchObject({
      id: 'profile-1', revision: '8', defaultDatabase: 'data_engine', authMode: 'BASIC',
    })
  })

  it('closes a connection before deleting its exact profile revision', async () => {
    const closeConnection = vi.fn(async () => undefined)
    const deleteProfile = vi.fn(async () => undefined)
    window.go = { main: { App: { CloseConnection: closeConnection, DeleteProfile: deleteProfile } } }

    await bridge.closeConnection('profile-1')
    await bridge.deleteProfile('profile-1', '7')

    expect(closeConnection).toHaveBeenCalledWith('profile-1')
    expect(deleteProfile).toHaveBeenCalledWith('profile-1', '7')
  })

  it('orchestrates close, edit, reconnect, and the persisted default database', async () => {
    const closeConnection = vi.fn(async () => undefined)
    const saveProfile = vi.fn(async () => ({
      id: 'profile-1', revision: '8', name: 'Data engine',
      baseUrl: 'http://192.168.2.6:8086', defaultDatabase: 'data_engine',
      environment: 'production', authMode: 'BASIC', username: 'operator',
      protectionMode: 'ProtectedLocked',
    }))
    const openConnection = vi.fn(async () => ({
      connectionId: 'profile-1', connectionGeneration: '2', profileId: 'profile-1',
      profileRevision: '8', version: '1.12.2',
      protection: {
        connectionId: 'profile-1', connectionGeneration: '2', protectionRevision: '1',
        mode: 'ProtectedLocked',
      },
    }))
    const getSchemaSnapshot = vi.fn(async () => ([
      { name: '_internal', retentionPolicies: ['monitor'], measurements: [] },
      { name: 'data_engine', retentionPolicies: ['autogen'], measurements: [] },
    ]))
    window.go = { main: { App: {
      CloseConnection: closeConnection,
      SaveProfile: saveProfile,
      OpenConnection: openConnection,
      GetSchemaSnapshot: getSchemaSnapshot,
    } } }
    useWorkbenchStore.setState({
      activeConnectionId: 'profile-1',
      activeConnection: {
        connectionId: 'profile-1', connectionGeneration: '1', profileId: 'profile-1',
        profileRevision: '7', version: '1.12.2',
        protection: {
          connectionId: 'profile-1', connectionGeneration: '1', protectionRevision: '1',
          mode: 'ProtectedLocked',
        },
      },
      connections: [{
        id: 'profile-1', revision: '7', name: 'Data engine',
        url: 'http://192.168.2.6:8086', defaultDatabase: 'old_database',
        username: 'operator', authMode: 'BASIC', environment: 'production',
        protectionMode: 'ProtectedLocked', state: 'connected',
      }],
      tabs: [{ id: 'query-1', title: 'Query', query: '', database: '_internal', dirty: false }],
      activeTabId: 'query-1',
    })

    await expect(useWorkbenchStore.getState().saveConnection({
      id: 'profile-1', expectedRevision: '7', name: 'Data engine',
      baseUrl: 'http://192.168.2.6:8086', defaultDatabase: 'data_engine',
      username: 'operator', authMode: 'BASIC', environment: 'production',
      protectionMode: 'ProtectedLocked',
    })).resolves.toBe(true)

    expect(closeConnection).toHaveBeenCalledWith('profile-1')
    expect(closeConnection.mock.invocationCallOrder[0]).toBeLessThan(saveProfile.mock.invocationCallOrder[0])
    expect(openConnection).toHaveBeenCalledWith('profile-1')
    expect(useWorkbenchStore.getState().tabs[0].database).toBe('data_engine')
    expect(useWorkbenchStore.getState().connections[0]).toMatchObject({ revision: '8', state: 'connected' })
  })

  it('closes and removes an active connection only after profile deletion succeeds', async () => {
    const closeConnection = vi.fn(async () => undefined)
    const deleteProfile = vi.fn(async () => undefined)
    window.go = { main: { App: { CloseConnection: closeConnection, DeleteProfile: deleteProfile } } }
    useWorkbenchStore.setState({
      activeConnectionId: 'profile-1',
      activeConnection: {
        connectionId: 'profile-1', connectionGeneration: '1', profileId: 'profile-1',
        profileRevision: '7', protection: {
          connectionId: 'profile-1', connectionGeneration: '1', protectionRevision: '1',
          mode: 'ProtectedLocked',
        },
      },
      connections: [{
        id: 'profile-1', revision: '7', name: 'Data engine',
        url: 'http://192.168.2.6:8086', defaultDatabase: 'data_engine',
        username: 'operator', authMode: 'BASIC', environment: 'production',
        protectionMode: 'ProtectedLocked', state: 'connected',
      }],
    })

    await useWorkbenchStore.getState().deleteConnection('profile-1')

    expect(closeConnection.mock.invocationCallOrder[0]).toBeLessThan(deleteProfile.mock.invocationCallOrder[0])
    expect(deleteProfile).toHaveBeenCalledWith('profile-1', '7')
    expect(useWorkbenchStore.getState().connections).toEqual([])
    expect(useWorkbenchStore.getState().activeConnectionId).toBe('')
  })

  it('loads a selected measurement schema through the typed native API', async () => {
    const getMeasurementSchema = vi.fn(async () => ({
      name: 'cpu',
      fields: [
        { name: 'usage_user', type: 'float' },
        { name: 'requests', type: 'integer' },
      ],
      tags: ['host', 'region'],
    }))
    window.go = { main: { App: { GetMeasurementSchema: getMeasurementSchema } } }

    await expect(bridge.getMeasurementSchema('profile-1', 'metrics', 'cpu')).resolves.toEqual({
      name: 'cpu',
      fields: [
        { name: 'usage_user', type: 'float' },
        { name: 'requests', type: 'integer' },
      ],
      tags: ['host', 'region'],
    })
    expect(getMeasurementSchema).toHaveBeenCalledWith('profile-1', 'metrics', 'cpu')
  })

  it('stores the native session id before canceling a running query', async () => {
    let finishPoll: ((session: unknown) => void) | undefined
    const getSession = vi.fn(() => new Promise((resolve) => {
      finishPoll = resolve
    }))
    const cancelQuery = vi.fn(async () => undefined)
    window.go = {
      main: {
        App: {
          StartReadQuery: vi.fn(async () => ({
            id: 'session-real',
            state: 'RUNNING',
            terminal: false,
            stateRevision: '2',
            createdAt: '2026-07-23T00:00:00Z',
            updatedAt: '2026-07-23T00:00:00Z',
            resultAvailable: false,
            complete: false,
          })),
          GetQuerySession: getSession,
          CancelQuery: cancelQuery,
        },
      },
    }
    useWorkbenchStore.setState({
      activeConnectionId: 'profile-1',
      activeTabId: 'query-1',
      tabs: [{
        id: 'query-1',
        title: 'Query',
        query: 'SELECT value FROM cpu',
        database: 'metrics',
        dirty: false,
      }],
      queryState: 'idle',
      activeQueryRequestId: undefined,
      activeSessionId: undefined,
    })

    const execution = useWorkbenchStore.getState().executeQuery()
    await vi.waitFor(() => {
      expect(useWorkbenchStore.getState().activeSessionId).toBe('session-real')
    })
    await useWorkbenchStore.getState().cancelQuery()
    expect(cancelQuery).toHaveBeenCalledWith('session-real', {
      commandRequestId: expect.any(String),
      expectedStateRevision: '2',
    })

    await vi.waitFor(() => expect(getSession).toHaveBeenCalled())
    finishPoll?.({
      id: 'session-real',
      state: 'CANCELED',
      terminal: true,
      stateRevision: '4',
      createdAt: '2026-07-23T00:00:00Z',
      updatedAt: '2026-07-23T00:00:00.100Z',
      resultAvailable: false,
      complete: false,
    })
    await execution
    expect(useWorkbenchStore.getState().queryState).toBe('canceled')
    expect(useWorkbenchStore.getState().activeSessionStateRevision).toBe('4')
  })

  it('retries a lost cancel response with the same command envelope', async () => {
    const cancelQuery = vi.fn()
      .mockRejectedValueOnce(new Error('response lost'))
      .mockResolvedValueOnce({
        id: 'session-real', state: 'CANCEL_REQUESTED', terminal: false, stateRevision: '3',
      })
    window.go = { main: { App: { CancelQuery: cancelQuery } } }

    await bridge.cancelQuery('session-real', '2')

    expect(cancelQuery).toHaveBeenCalledTimes(2)
    expect(cancelQuery.mock.calls[0]).toEqual(cancelQuery.mock.calls[1])
    expect(cancelQuery.mock.calls[0]).toEqual([
      'session-real',
      { commandRequestId: expect.any(String), expectedStateRevision: '2' },
    ])
  })

  it('previews a mutation and polls only public operation fields to terminal', async () => {
    const previewMutation = vi.fn(async () => ({
      canonicalQuery: 'DROP DATABASE "telemetry"',
      operationKind: 'DROP_DATABASE',
      target: 'telemetry',
      confirmationRequired: true,
      executable: true,
      token: 'opaque-preview-token',
      expiresAt: '2026-07-23T00:02:00Z',
    }))
    const executeMutation = vi.fn(async () => ({
      task: { id: 'operation-1', state: 'DISPATCHING', terminal: false },
      actionDigest: 'not-forwarded-to-store',
    }))
    const getOperation = vi.fn(async () => ({
      task: {
        id: 'operation-1',
        state: 'SUCCEEDED',
        terminal: true,
        publicSafeMessage: '变更已确认。',
      },
      rawServerError: 'server-error-canary-must-not-cross',
    }))
    window.go = {
      main: { App: { PreviewMutation: previewMutation, ExecuteMutation: executeMutation, GetOperation: getOperation } },
    }

    const preview = await bridge.previewMutation('profile-1', {
      database: 'telemetry',
      query: 'DROP DATABASE "telemetry"',
    })
    expect(preview.token).toBe('opaque-preview-token')
    expect(previewMutation).toHaveBeenCalledWith('profile-1', {
      database: 'telemetry',
      query: 'DROP DATABASE "telemetry"',
    })

    const result = await bridge.executeMutation('profile-1', {
      clientRequestId: '550e8400-e29b-41d4-a716-446655440000',
      previewToken: preview.token!,
      confirmation: 'telemetry',
    })
    expect(getOperation).toHaveBeenCalledWith('operation-1')
    expect(result).toEqual({
      id: 'operation-1',
      state: 'SUCCEEDED',
      terminal: true,
      publicErrorCode: undefined,
      publicSafeMessage: '变更已确认。',
    })
    expect(JSON.stringify(result)).not.toContain('actionDigest')
    expect(JSON.stringify(result)).not.toContain('server-error-canary')
  })

  it('clears the in-memory preview token when the mutation dialog closes', async () => {
    window.go = {
      main: {
        App: {
          PreviewMutation: vi.fn(async () => ({
            canonicalQuery: 'DROP DATABASE "telemetry"',
            operationKind: 'DROP_DATABASE',
            target: 'telemetry',
            confirmationRequired: true,
            executable: true,
            token: 'react-memory-token',
            expiresAt: '2026-07-23T00:02:00Z',
          })),
        },
      },
    }
    useWorkbenchStore.setState({
      activeConnectionId: 'profile-1',
      activeTabId: 'query-1',
      tabs: [{
        id: 'query-1', title: 'Mutation', query: 'DROP DATABASE "telemetry"',
        database: 'telemetry', dirty: false,
      }],
      protection: 'unlocked',
      queryState: 'idle',
    })

    await useWorkbenchStore.getState().previewMutation()
    expect(useWorkbenchStore.getState().mutationPreview?.token).toBe('react-memory-token')
    useWorkbenchStore.getState().closeMutationDialog()
    expect(useWorkbenchStore.getState().mutationDialogOpen).toBe(false)
    expect(useWorkbenchStore.getState().mutationPreview).toBeUndefined()
  })

  it('removes browser mock transfer tasks when native initialization begins', async () => {
    window.go = {
      main: { App: { ListProfiles: vi.fn(async () => []) } },
    }
    useWorkbenchStore.setState({
      tasks: [{
        id: 'mock-transfer', kind: 'import', title: 'Mock', subtitle: 'Mock', progress: 0,
        processed: '0', total: '0', state: 'paused', updatedAt: 'now', revision: '1',
      }],
    })

    await useWorkbenchStore.getState().initialize()
    expect(useWorkbenchStore.getState().tasks).toEqual([])
    useWorkbenchStore.getState().addTask('import')
    expect(useWorkbenchStore.getState().tasks).toEqual([])
  })

  it('does not let a slow previous connection overwrite the active Schema', async () => {
    const schemaResolvers = new Map<string, (value: unknown) => void>()
    const getSchemaSnapshot = vi.fn((profileId: unknown) => new Promise((resolve) => {
      schemaResolvers.set(String(profileId), resolve)
    }))
    const openConnection = vi.fn(async (profileId: unknown) => {
      const id = String(profileId)
      const generation = id === 'profile-a' ? '1' : '2'
      return {
        connectionId: id,
        connectionGeneration: generation,
        profileId: id,
        profileRevision: '1',
        version: '1.12.4',
        protection: {
          connectionId: id,
          connectionGeneration: generation,
          protectionRevision: '1',
          mode: 'ProtectedLocked',
        },
      }
    })
    window.go = { main: { App: { OpenConnection: openConnection, GetSchemaSnapshot: getSchemaSnapshot } } }
    useWorkbenchStore.setState({
      activeConnectionId: '',
      connections: [
        { id: 'profile-a', name: 'A', url: 'http://a', environment: 'production', state: 'disconnected' },
        { id: 'profile-b', name: 'B', url: 'http://b', environment: 'production', state: 'disconnected' },
      ],
      schema: [],
      schemaLoading: false,
    })

    useWorkbenchStore.getState().setActiveConnection('profile-a')
    await vi.waitFor(() => expect(getSchemaSnapshot).toHaveBeenCalledWith('profile-a'))
    useWorkbenchStore.getState().setActiveConnection('profile-b')
    await vi.waitFor(() => expect(getSchemaSnapshot).toHaveBeenCalledWith('profile-b'))

    schemaResolvers.get('profile-b')?.([{
      name: 'database-b', retentionPolicies: ['autogen'], measurements: [{ name: 'measurement-b', fields: [], tags: [] }],
    }])
    await vi.waitFor(() => expect(useWorkbenchStore.getState().schema[0]?.name).toBe('database-b'))

    schemaResolvers.get('profile-a')?.([{
      name: 'database-a', retentionPolicies: ['autogen'], measurements: [{ name: 'measurement-a', fields: [], tags: [] }],
    }])
    await Promise.resolve()
    await Promise.resolve()

    expect(useWorkbenchStore.getState()).toMatchObject({
      activeConnectionId: 'profile-b',
      schemaLoading: false,
      schema: [{ name: 'database-b' }],
    })
  })

  it('retries Import grant consumption with the same command identity and exact revision', async () => {
    const previewImportRun = vi.fn(async () => ({
      jobId: 'import-1',
      state: 'READY',
      checkpointDigest: 'checkpoint-1',
      logicalOffset: '9007199254740993',
      adaptiveMaxPoints: '5000',
      adaptiveMaxBytes: '5242880',
      targetDigest: 'target-1',
      executable: true,
      importRunGrant: { token: 'opaque-import-grant', expiresAt: '2026-07-23T00:02:00Z' },
    }))
    const startImport = vi.fn()
      .mockRejectedValueOnce(new Error('response lost'))
      .mockResolvedValueOnce({ task: { id: 'import-1', state: 'RUNNING', stateRevision: '9007199254740994' } })
    window.go = { main: { App: { PreviewImportRun: previewImportRun, StartImport: startImport } } }
    const request = {
      jobId: 'import-1',
      commandRequestId: '550e8400-e29b-41d4-a716-446655440000',
      expectedStateRevision: '9007199254740993',
      action: 'START' as const,
    }

    const preview = await bridge.previewImportRun('profile-1', request)
    await bridge.startImport('profile-1', { ...request, importRunGrant: preview.importRunGrant!.token })

    expect(preview.logicalOffset).toBe('9007199254740993')
    expect(startImport).toHaveBeenCalledTimes(2)
    expect(startImport.mock.calls[0]).toEqual(startImport.mock.calls[1])
    expect(startImport.mock.calls[0]).toEqual(['profile-1', {
      ...request,
      importRunGrant: 'opaque-import-grant',
    }])
  })

  it('passes Export command UUID and state revision without numeric coercion', async () => {
    const cancelExport = vi.fn(async () => ({
      task: { id: 'export-1', state: 'CANCELED', stateRevision: '18446744073709551615' },
    }))
    window.go = { main: { App: { CancelExport: cancelExport } } }
    const envelope = {
      commandRequestId: '550e8400-e29b-41d4-a716-446655440000',
      expectedStateRevision: '18446744073709551614',
    }

    await bridge.cancelExport('export-1', envelope)

    expect(cancelExport).toHaveBeenCalledWith('export-1', envelope)
    const calls = cancelExport.mock.calls as unknown as Array<[string, typeof envelope]>
    expect(typeof calls[0][1].expectedStateRevision).toBe('string')
  })

  it('uses lower-camel Export filters and omits backend-owned Preflight identity', async () => {
    const listExports = vi.fn(async (..._args: unknown[]) => [])
    const preflightImport = vi.fn(async (..._args: unknown[]) => ({
      job: { task: { id: 'import-1', state: 'READY' } }, replayed: false, ready: true,
    }))
    window.go = { main: { App: { ListExports: listExports, PreflightImport: preflightImport } } }
    const request = {
      clientRequestId: '550e8400-e29b-41d4-a716-446655440000',
      profileId: 'profile-1',
      sourcePath: 'C:\\data\\metrics.lp',
      source: { sha256: 'a'.repeat(64), sizeBytes: '9007199254740993' },
      format: 'LP' as const,
      target: { database: 'metrics', retentionPolicy: 'autogen' },
    }

    await bridge.listExports('profile-1', 'RUNNING', 17)
    await bridge.preflightImport(request)

    expect(listExports).toHaveBeenCalledWith({ profileId: 'profile-1', state: 'RUNNING', limit: 17 })
    expect(preflightImport).toHaveBeenCalledWith(request)
    expect(preflightImport.mock.calls[0][0]).not.toHaveProperty('profileRevision')
    expect(preflightImport.mock.calls[0][0]).not.toHaveProperty('connectionId')
    expect(preflightImport.mock.calls[0][0]).not.toHaveProperty('connectionGeneration')
  })

  it('does not send the Preview-only action field when resolving an Import incident', async () => {
    const resolveImportBatch = vi.fn(async (..._args: unknown[]) => ({
      task: { id: 'import-1', state: 'PAUSED_SAFE', stateRevision: '8' },
    }))
    window.go = { main: { App: { ResolveImportBatch: resolveImportBatch } } }
    const request = {
      jobId: 'import-1',
      commandRequestId: '550e8400-e29b-41d4-a716-446655440000',
      expectedStateRevision: '7',
      incidentId: 'incident-1',
      parentCheckpointDigest: 'checkpoint-1',
      decision: 'ASSUME_COMMITTED' as const,
      afterResolution: 'PAUSE' as const,
    }

    await bridge.resolveImportBatch('profile-1', request)

    expect(resolveImportBatch).toHaveBeenCalledWith('profile-1', request)
    expect(resolveImportBatch.mock.calls[0][1]).not.toHaveProperty('action')
  })

  it('refuses to read a large source file in the browser process', async () => {
    const file = {
      name: 'large.lp.gz',
      size: 64 * 1024 * 1024 + 1,
      arrayBuffer: vi.fn(),
    } as unknown as File

    await expect(bridge.inspectImportFile(file, 'C:\\data\\large.lp.gz'))
      .rejects.toThrow('IMPORT_SOURCE_INSPECTION_REQUIRES_NATIVE')
    expect(file.arrayBuffer).not.toHaveBeenCalled()
  })

  it('delegates native source identity and target selection to the backend', async () => {
    const inspect = vi.fn(async () => ({
      sourcePath: 'C:\\data\\large.lp.gz', displayName: 'large.lp.gz',
      sha256: 'a'.repeat(64), sizeBytes: '21474836480',
    }))
    const selectImport = vi.fn(async () => ({
      sourcePath: 'C:\\data\\selected.lp', displayName: 'selected.lp',
      sha256: 'b'.repeat(64), sizeBytes: '9007199254740993',
    }))
    const selectExport = vi.fn(async () => 'D:\\exports')
    window.go = { main: { App: {
      InspectImportSource: inspect,
      SelectImportSource: selectImport,
      SelectExportDirectory: selectExport,
    } } }
    const file = {
      name: 'large.lp.gz', size: 20 * 1024 * 1024 * 1024, arrayBuffer: vi.fn(),
    } as unknown as File

    const identity = await bridge.inspectImportFile(file, 'C:\\data\\large.lp.gz')
    expect(identity.sizeBytes).toBe('21474836480')
    expect(inspect).toHaveBeenCalledWith('C:\\data\\large.lp.gz')
    expect(file.arrayBuffer).not.toHaveBeenCalled()
    expect((await bridge.selectImportSource()).sizeBytes).toBe('9007199254740993')
    expect(await bridge.selectExportDirectory()).toBe('D:\\exports')
  })
})
