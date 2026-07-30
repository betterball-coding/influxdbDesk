import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useWorkbenchStore } from '../store'
import { TransferPanel } from './TransferPanel'

const initialState = useWorkbenchStore.getState()

function task(id: string, kind: string, state: string, revision: string, terminal = false) {
  return {
    id,
    kind,
    state,
    terminal,
    snapshotRevision: revision,
    stateRevision: revision,
    lastEventSeq: revision,
    createdAt: '2026-07-23T00:00:00Z',
    updatedAt: '2026-07-23T00:00:01Z',
    archived: false,
    resultAvailable: false,
  }
}

afterEach(() => {
  cleanup()
  delete window.go
  delete window.runtime
  useWorkbenchStore.setState(initialState, true)
})

describe('TransferPanel', () => {
  it('subscribes before native head/snapshots and coalesces live task refreshes', async () => {
    const order: string[] = []
    let eventListener: ((event: unknown) => void) | undefined
    const unsubscribe = vi.fn()
    const initial = {
      task: task('import-live', 'IMPORT', 'RUNNING', '10'),
      profileId: 'profile-1',
      checkpointDigest: 'checkpoint-10',
      checkpoint: {
        sequence: '1', logicalOffset: '10', adaptiveMaxPoints: '5000', adaptiveMaxBytes: '5242880',
        sourceSha256: 'source', stagingSha256: 'staging', normalizationVersion: 'canonical-lp-v1',
        specDigest: 'spec', targetDigest: 'target', lossy: false,
      },
      pauseRequested: false,
    }
    const revision11 = {
      ...initial,
      task: task('import-live', 'IMPORT', 'RUNNING', '11'),
      checkpointDigest: 'checkpoint-11',
    }
    const revision13 = {
      ...initial,
      task: task('import-live', 'IMPORT', 'PAUSED_SAFE', '13'),
      checkpointDigest: 'checkpoint-13',
    }
    const event = (seq: string) => ({
      schemaVersion: 1, seq, kind: 'IMPORT', id: 'import-live', resourceRevision: seq,
      state: 'RUNNING', changeType: 'PROGRESS', occurredAt: '2026-07-23T00:00:02Z',
    })
    const getImportJob = vi.fn()
      .mockResolvedValueOnce(revision11)
      .mockResolvedValueOnce(revision13)
    const getTaskChanges = vi.fn(async () => [event('12'), event('13')])
    window.runtime = {
      EventsOnMultiple: vi.fn((_name, listener) => {
        order.push('subscribe')
        eventListener = listener
        return unsubscribe
      }),
    }
    window.go = { main: { App: {
      GetTaskEventHead: vi.fn(async () => { order.push('head'); return '10' }),
      GetTaskChanges: getTaskChanges,
      GetImportJob: getImportJob,
      GetExportJob: vi.fn(),
      ListRecoverableImports: vi.fn(async () => { order.push('imports'); return [initial] }),
      ListExports: vi.fn(async () => { order.push('exports'); return [] }),
    } } }
    useWorkbenchStore.setState({ taskDrawerOpen: true, activeConnectionId: 'profile-1', schema: [] })

    const view = render(<TransferPanel />)
    await screen.findByText('rev 10')
    expect(order[0]).toBe('subscribe')
    expect(order[1]).toBe('head')
    expect(order.indexOf('head')).toBeLessThan(order.indexOf('imports'))
    expect(order.indexOf('head')).toBeLessThan(order.indexOf('exports'))

    await act(async () => {
      eventListener?.(event('11'))
      eventListener?.(event('11'))
      eventListener?.(event('10'))
    })
    await screen.findByText('rev 11')
    expect(getImportJob).toHaveBeenCalledTimes(1)

    await act(async () => { eventListener?.(event('13')) })
    await screen.findByText('rev 13')
    expect(getTaskChanges).toHaveBeenCalledWith('11', 1000)
    expect(getImportJob).toHaveBeenCalledTimes(2)

    view.unmount()
    expect(unsubscribe).toHaveBeenCalledTimes(1)
    await act(async () => { eventListener?.(event('14')) })
    expect(getImportJob).toHaveBeenCalledTimes(2)
  })

  it('shows all incident decisions and disables the decision invalid for UNKNOWN', async () => {
    window.go = { main: { App: {
      ListRecoverableImports: vi.fn(async () => ([{
        task: task('import-incident', 'IMPORT', 'NEEDS_UNKNOWN_DECISION', '9007199254740993'),
        profileId: 'profile-1',
        checkpointDigest: 'checkpoint-1',
        checkpoint: {
          sequence: '4', logicalOffset: '9007199254740993', adaptiveMaxPoints: '5000',
          adaptiveMaxBytes: '5242880', sourceSha256: 'source', stagingSha256: 'staging',
          normalizationVersion: 'canonical-lp-v1', specDigest: 'spec', targetDigest: 'target', lossy: false,
        },
        pauseRequested: false,
        openIncident: {
          incidentId: 'incident-1', kind: 'UNKNOWN', status: 'OPEN', parentCheckpointDigest: 'checkpoint-1',
          runSegmentId: 'segment-1', batchId: 'batch-1', attemptId: 'attempt-1',
          startOffset: '9007199254740993', endOffset: '9007199254741993', payloadDigest: 'payload',
        },
      }])),
      ListExports: vi.fn(async () => []),
    } } }
    useWorkbenchStore.setState({
      taskDrawerOpen: true,
      activeConnectionId: 'profile-1',
      protection: 'locked',
      schema: [],
    })

    render(<TransferPanel />)

    expect((await screen.findByRole('button', { name: 'REPLAY_EXACT' }) as HTMLButtonElement).disabled).toBe(false)
    expect((screen.getByRole('button', { name: 'ASSUME_COMMITTED' }) as HTMLButtonElement).disabled).toBe(false)
    expect((screen.getByRole('button', { name: 'ACCEPT_PARTIAL' }) as HTMLButtonElement).disabled).toBe(true)
    expect((screen.getByRole('button', { name: 'ABORT' }) as HTMLButtonElement).disabled).toBe(false)
    expect(screen.getAllByText('9007199254740993', { exact: true }).length).toBeGreaterThan(0)
    expect(screen.getByText('写入保护已锁定')).toBeTruthy()
  })

  it('submits Export cancellation with a fresh UUID and the current string revision', async () => {
    const exportJob = {
      task: task('export-queued', 'EXPORT', 'QUEUED', '18446744073709551614'),
      profileId: 'profile-1', profileRevision: '2', connectionId: 'connection-1',
      connectionGeneration: '7', specDigest: 'spec', targetDigest: 'target', targetVolumeId: 'volume',
      cancelRequested: false, retained: true, fragments: [],
    }
    const cancelExport = vi.fn(async () => ({
      ...exportJob,
      task: task('export-queued', 'EXPORT', 'CANCELED', '18446744073709551615', true),
      cancelRequested: true,
    }))
    window.go = { main: { App: {
      ListRecoverableImports: vi.fn(async () => []),
      ListExports: vi.fn(async () => [exportJob]),
      CancelExport: cancelExport,
    } } }
    useWorkbenchStore.setState({ taskDrawerOpen: true, activeConnectionId: 'profile-1', schema: [] })

    render(<TransferPanel />)
    fireEvent.click(await screen.findByRole('button', { name: '取消导出' }))

    await waitFor(() => expect(cancelExport).toHaveBeenCalledTimes(1))
    expect(cancelExport).toHaveBeenCalledWith('export-queued', {
      commandRequestId: expect.any(String),
      expectedStateRevision: '18446744073709551614',
    })
  })
})
