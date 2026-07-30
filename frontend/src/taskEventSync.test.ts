import { describe, expect, it, vi } from 'vitest'
import type { ExportJob, ImportJob, TaskEvent } from './types'
import {
  compareDecimal,
  incrementDecimal,
  TaskEventSynchronizer,
  type TaskEventSyncSource,
} from './taskEventSync'

function task(id: string, kind: string, revision: string, terminal = false) {
  return {
    id,
    kind,
    state: terminal ? 'SUCCEEDED' : 'RUNNING',
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

function importJob(id: string, revision: string): ImportJob {
  return {
    task: task(id, 'IMPORT', revision),
    profileId: 'profile-1',
    checkpointDigest: 'checkpoint',
    checkpoint: {
      sequence: '0', logicalOffset: '0', adaptiveMaxPoints: '5000', adaptiveMaxBytes: '5242880',
      sourceSha256: 'source', stagingSha256: 'staging', normalizationVersion: 'canonical-lp-v1',
      specDigest: 'spec', targetDigest: 'target', lossy: false,
    },
    pauseRequested: false,
  }
}

function exportJob(id: string, revision: string): ExportJob {
  return {
    task: task(id, 'EXPORT', revision),
    profileId: 'profile-1', profileRevision: '1', connectionId: 'connection-1',
    connectionGeneration: '1', specDigest: 'spec', targetDigest: 'target', targetVolumeId: 'volume',
    cancelRequested: false, retained: true, fragments: [],
  }
}

function event(seq: string, kind: 'IMPORT' | 'EXPORT' = 'IMPORT', id = 'job-1'): TaskEvent {
  return {
    schemaVersion: 1,
    seq,
    kind,
    id,
    resourceRevision: seq,
    state: 'RUNNING',
    changeType: 'PROGRESS',
    occurredAt: '2026-07-23T00:00:02Z',
  }
}

function setup(head: string) {
  let listener: ((value: TaskEvent) => void) | undefined
  const unsubscribe = vi.fn()
  const order: string[] = []
  const source: TaskEventSyncSource = {
    subscribe: vi.fn((next) => {
      order.push('subscribe')
      listener = next
      return unsubscribe
    }),
    getHead: vi.fn(async () => {
      order.push('head')
      return head
    }),
    getChanges: vi.fn(async () => []),
    loadAll: vi.fn(async () => {
      order.push('snapshots')
      return { imports: [], exports: [] }
    }),
    loadOne: vi.fn(async (kind, id) => kind === 'IMPORT'
      ? { kind, job: importJob(id, '2') }
      : { kind, job: exportJob(id, '2') }),
    hasActiveTasks: vi.fn(() => true),
  }
  const callbacks = {
    onFullSnapshot: vi.fn(),
    onResourceSnapshot: vi.fn(),
    onError: vi.fn(),
  }
  const synchronizer = new TaskEventSynchronizer(source, callbacks, { pollIntervalMs: 60_000 })
  return { source, callbacks, synchronizer, unsubscribe, order, emit: (value: TaskEvent) => listener?.(value) }
}

describe('exact decimal event sequence', () => {
  it('compares and increments beyond uint64 without Number conversion', () => {
    expect(compareDecimal('18446744073709551616', '9999999999999999999')).toBe(1)
    expect(incrementDecimal('999999999999999999999999999999')).toBe('1000000000000000000000000000000')
    expect(() => compareDecimal('01', '1')).toThrow('INVALID_EXACT_NUMBER')
  })
})

describe('TaskEventSynchronizer', () => {
  it('subscribes before head/snapshots and coalesces duplicate or out-of-order events', async () => {
    const current = '18446744073709551615'
    const harness = setup(current)
    await harness.synchronizer.start()
    expect(harness.order).toEqual(['subscribe', 'head', 'snapshots'])

    const next = '18446744073709551616'
    harness.emit(event(next))
    harness.emit(event(next))
    harness.emit(event(current))

    await vi.waitFor(() => expect(harness.source.loadOne).toHaveBeenCalledTimes(1))
    expect(harness.source.getChanges).not.toHaveBeenCalled()
    expect(harness.callbacks.onResourceSnapshot).toHaveBeenCalledTimes(1)
    expect(harness.synchronizer.currentSeq()).toBe(next)
    harness.synchronizer.stop()
  })

  it('fills a gap with GetTaskChanges(after, 1000) and refreshes each resource once', async () => {
    const harness = setup('100')
    vi.mocked(harness.source.getChanges).mockResolvedValueOnce([
      event('101', 'IMPORT', 'job-1'),
      event('102', 'IMPORT', 'job-1'),
    ])
    await harness.synchronizer.start()
    harness.emit(event('102', 'IMPORT', 'job-1'))

    await vi.waitFor(() => expect(harness.source.loadOne).toHaveBeenCalledTimes(1))
    expect(harness.source.getChanges).toHaveBeenCalledWith('100', 1000)
    expect(harness.synchronizer.currentSeq()).toBe('102')
    harness.synchronizer.stop()
  })

  it('falls back to one full snapshot when the event log read fails', async () => {
    const harness = setup('0')
    vi.mocked(harness.source.getChanges).mockRejectedValueOnce(new Error('event log expired'))
    vi.mocked(harness.source.getHead)
      .mockResolvedValueOnce('0')
      .mockResolvedValueOnce('2')
    await harness.synchronizer.start()
    harness.emit(event('2'))

    await vi.waitFor(() => expect(harness.source.loadAll).toHaveBeenCalledTimes(2))
    expect(harness.callbacks.onFullSnapshot).toHaveBeenCalledTimes(2)
    expect(harness.source.loadOne).not.toHaveBeenCalled()
    expect(harness.synchronizer.currentSeq()).toBe('2')
    harness.synchronizer.stop()
  })

  it('checks a changed head and cleans up subscription, focus listener, and timer', async () => {
    const harness = setup('5')
    vi.mocked(harness.source.getHead)
      .mockResolvedValueOnce('5')
      .mockResolvedValueOnce('7')
    vi.mocked(harness.source.getChanges).mockResolvedValueOnce([
      event('6', 'EXPORT', 'export-1'),
      event('7', 'EXPORT', 'export-1'),
    ])
    const addEventListener = vi.fn()
    const removeEventListener = vi.fn()
    const setInterval = vi.fn(() => 91)
    const clearInterval = vi.fn()
    const synchronizer = new TaskEventSynchronizer(harness.source, harness.callbacks, {
      pollIntervalMs: 5000,
      windowTarget: { addEventListener, removeEventListener, setInterval, clearInterval },
    })

    await synchronizer.start()
    const focus = addEventListener.mock.calls.find(([name]) => name === 'focus')?.[1] as (() => void)
    focus()
    await vi.waitFor(() => expect(harness.source.getChanges).toHaveBeenCalledWith('5', 1000))
    expect(synchronizer.currentSeq()).toBe('7')

    synchronizer.stop()
    expect(harness.unsubscribe).toHaveBeenCalledTimes(1)
    expect(removeEventListener).toHaveBeenCalledWith('focus', focus)
    expect(clearInterval).toHaveBeenCalledWith(91)
  })
})
