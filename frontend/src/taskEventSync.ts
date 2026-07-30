import type { ExportJob, ImportJob, TaskEvent } from './types'

type TransferKind = 'IMPORT' | 'EXPORT'
type TransferSnapshot =
  | { kind: 'IMPORT'; job: ImportJob }
  | { kind: 'EXPORT'; job: ExportJob }

export interface TaskEventSyncSource {
  subscribe: (listener: (event: TaskEvent) => void) => () => void
  getHead: () => Promise<string>
  getChanges: (after: string, limit: number) => Promise<TaskEvent[]>
  loadAll: () => Promise<{ imports: ImportJob[]; exports: ExportJob[] }>
  loadOne: (kind: TransferKind, id: string) => Promise<TransferSnapshot>
  hasActiveTasks: () => boolean
}

export interface TaskEventSyncCallbacks {
  onFullSnapshot: (imports: ImportJob[], exports: ExportJob[]) => void
  onResourceSnapshot: (snapshot: TransferSnapshot) => void
  onError?: (error: unknown) => void
}

export interface TaskEventSyncOptions {
  pollIntervalMs?: number
  windowTarget?: Pick<Window, 'addEventListener' | 'removeEventListener' | 'setInterval' | 'clearInterval'>
}

const canonicalDecimalPattern = /^(?:0|[1-9][0-9]*)$/

export function compareDecimal(left: string, right: string): number {
  if (!canonicalDecimalPattern.test(left) || !canonicalDecimalPattern.test(right)) {
    throw new Error('INVALID_EXACT_NUMBER')
  }
  if (left.length !== right.length) return left.length < right.length ? -1 : 1
  if (left === right) return 0
  return left < right ? -1 : 1
}

export function incrementDecimal(value: string): string {
  if (!canonicalDecimalPattern.test(value)) throw new Error('INVALID_EXACT_NUMBER')
  const digits = value.split('')
  for (let index = digits.length - 1; index >= 0; index -= 1) {
    if (digits[index] !== '9') {
      digits[index] = String(Number(digits[index]) + 1)
      return digits.join('')
    }
    digits[index] = '0'
  }
  return `1${digits.join('')}`
}

function validEvent(event: TaskEvent): boolean {
  return event?.schemaVersion === 1 &&
    canonicalDecimalPattern.test(event.seq) &&
    canonicalDecimalPattern.test(event.resourceRevision) &&
    typeof event.kind === 'string' &&
    typeof event.id === 'string' && event.id.length > 0
}

function maximumDecimal(left: string, right: string): string {
  return compareDecimal(left, right) >= 0 ? left : right
}

function transferKind(kind: string): TransferKind | undefined {
  if (kind === 'IMPORT' || kind === 'EXPORT') return kind
  return undefined
}

export class TaskEventSynchronizer {
  private readonly pollIntervalMs: number
  private readonly windowTarget: TaskEventSyncOptions['windowTarget']
  private unsubscribe?: () => void
  private intervalId?: number
  private stopped = false
  private started = false
  private initialized = false
  private lastSeq = '0'
  private pending: TaskEvent[] = []
  private drainQueued = false
  private headCheckQueued = false
  private serial: Promise<void> = Promise.resolve()

  constructor(
    private readonly source: TaskEventSyncSource,
    private readonly callbacks: TaskEventSyncCallbacks,
    options: TaskEventSyncOptions = {},
  ) {
    this.pollIntervalMs = options.pollIntervalMs ?? 5000
    this.windowTarget = options.windowTarget ?? window
  }

  start(): Promise<void> {
    if (this.started) return this.serial
    this.started = true

    // Wails events are subscribed synchronously before the first head or snapshot read.
    this.unsubscribe = this.source.subscribe((event) => this.receive(event))
    this.windowTarget?.addEventListener('focus', this.onFocus)
    this.intervalId = this.windowTarget?.setInterval(() => {
      if (this.source.hasActiveTasks()) this.scheduleHeadCheck()
    }, this.pollIntervalMs)

    return this.enqueue(async () => {
      await this.initialize()
    })
  }

  stop(): void {
    if (this.stopped) return
    this.stopped = true
    this.pending = []
    this.unsubscribe?.()
    this.unsubscribe = undefined
    this.windowTarget?.removeEventListener('focus', this.onFocus)
    if (this.intervalId !== undefined) this.windowTarget?.clearInterval(this.intervalId)
    this.intervalId = undefined
  }

  resync(): Promise<void> {
    return this.enqueue(async () => {
      if (!this.initialized) {
        await this.initialize()
        return
      }
      await this.recoverWithFullSnapshot(this.lastSeq)
      await this.drainPending()
    })
  }

  checkHead(): Promise<void> {
    return this.enqueue(async () => this.reconcileHead())
  }

  currentSeq(): string {
    return this.lastSeq
  }

  private readonly onFocus = () => {
    this.scheduleHeadCheck()
  }

  private enqueue(work: () => Promise<void>): Promise<void> {
    const next = this.serial.then(async () => {
      if (!this.stopped) await work()
    })
    this.serial = next.catch((error) => {
      if (!this.stopped) this.callbacks.onError?.(error)
    })
    return this.serial
  }

  private async initialize(): Promise<void> {
    if (this.initialized || this.stopped) return
    const head = this.canonicalHead(await this.source.getHead())
    if (this.stopped) return
    const snapshot = await this.source.loadAll()
    if (this.stopped) return
    this.callbacks.onFullSnapshot(snapshot.imports, snapshot.exports)
    this.lastSeq = head
    this.initialized = true
    await this.drainPending()
  }

  private receive(event: TaskEvent): void {
    if (this.stopped) return
    this.pending.push(event)
    if (this.drainQueued) return
    this.drainQueued = true
    queueMicrotask(() => {
      this.drainQueued = false
      void this.enqueue(async () => this.drainPending())
    })
  }

  private async drainPending(): Promise<void> {
    if (!this.initialized || this.pending.length === 0 || this.stopped) return

    const events = this.pending.splice(0)
    const valid = events.filter(validEvent)
    if (valid.length !== events.length) {
      await this.recoverWithFullSnapshot(this.lastSeq)
      return
    }
    valid.sort((left, right) => compareDecimal(left.seq, right.seq))

    const resources = new Set<string>()
    let catchUpTarget = this.lastSeq
    for (const event of valid) {
      if (compareDecimal(event.seq, this.lastSeq) <= 0) continue
      if (event.seq === incrementDecimal(this.lastSeq)) {
        this.lastSeq = event.seq
        this.addResource(resources, event)
        continue
      }
      catchUpTarget = maximumDecimal(catchUpTarget, event.seq)
    }

    if (compareDecimal(catchUpTarget, this.lastSeq) > 0) {
      const recovered = await this.catchUp(catchUpTarget, resources)
      if (recovered) return
    }
    await this.refreshResources(resources)

    if (this.pending.length > 0) await this.drainPending()
  }

  private async reconcileHead(): Promise<void> {
    if (!this.initialized) {
      await this.initialize()
      return
    }
    const head = this.canonicalHead(await this.source.getHead())
    const comparison = compareDecimal(head, this.lastSeq)
    if (comparison < 0) {
      await this.recoverWithFullSnapshot(head)
      return
    }
    if (comparison === 0) return

    const resources = new Set<string>()
    const recovered = await this.catchUp(head, resources)
    if (!recovered) await this.refreshResources(resources)
  }

  // Returns true when a full snapshot replaced incremental resource refreshes.
  private async catchUp(target: string, resources: Set<string>): Promise<boolean> {
    while (compareDecimal(this.lastSeq, target) < 0) {
      let changes: TaskEvent[]
      try {
        changes = await this.source.getChanges(this.lastSeq, 1000)
      } catch {
        await this.recoverWithFullSnapshot(target)
        return true
      }
      if (changes.length === 0) {
        await this.recoverWithFullSnapshot(target)
        return true
      }

      let expected = incrementDecimal(this.lastSeq)
      for (const event of changes) {
        if (!validEvent(event) || event.seq !== expected) {
          await this.recoverWithFullSnapshot(target)
          return true
        }
        this.lastSeq = event.seq
        expected = incrementDecimal(this.lastSeq)
        this.addResource(resources, event)
      }
    }
    return false
  }

  private async recoverWithFullSnapshot(knownHead: string): Promise<void> {
    let head = knownHead
    try {
      head = maximumDecimal(head, this.canonicalHead(await this.source.getHead()))
    } catch {
      // A known event/head still gives us a safe cursor after the later snapshot read.
    }
    for (const event of this.pending) {
      if (validEvent(event)) head = maximumDecimal(head, event.seq)
    }
    const snapshot = await this.source.loadAll()
    if (this.stopped) return
    this.callbacks.onFullSnapshot(snapshot.imports, snapshot.exports)
    this.lastSeq = head
  }

  private async refreshResources(resources: Set<string>): Promise<void> {
    if (resources.size === 0 || this.stopped) return
    const requests = [...resources].map(async (key) => {
      const separator = key.indexOf(':')
      const kind = key.slice(0, separator) as TransferKind
      const id = key.slice(separator + 1)
      return this.source.loadOne(kind, id)
    })
    const snapshots = await Promise.allSettled(requests)
    if (snapshots.some((result) => result.status === 'rejected')) {
      await this.recoverWithFullSnapshot(this.lastSeq)
      return
    }
    if (this.stopped) return
    for (const result of snapshots) {
      if (result.status === 'fulfilled') this.callbacks.onResourceSnapshot(result.value)
    }
  }

  private addResource(resources: Set<string>, event: TaskEvent): void {
    const kind = transferKind(event.kind)
    if (kind) resources.add(`${kind}:${event.id}`)
  }

  private canonicalHead(value: string): string {
    if (!canonicalDecimalPattern.test(value)) throw new Error('INVALID_EXACT_NUMBER')
    return value
  }

  private scheduleHeadCheck(): void {
    if (this.stopped || this.headCheckQueued) return
    this.headCheckQueued = true
    void this.enqueue(async () => {
      try {
        await this.reconcileHead()
      } finally {
        this.headCheckQueued = false
      }
    })
  }
}
