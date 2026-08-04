import { describe, expect, it } from 'vitest'
import { formatTimestampNs } from './timestampFormat'

describe('formatTimestampNs', () => {
  const timestamp = (BigInt(Date.UTC(2026, 6, 28, 3, 45, 0)) * 1_000_000n).toString()

  it('formats nanosecond timestamps in UTC+8 by default', () => {
    expect(formatTimestampNs(timestamp)).toBe('2026-07-28 11:45:00')
  })

  it('formats nanosecond timestamps in UTC when configured', () => {
    expect(formatTimestampNs(timestamp, 'utc')).toBe('2026-07-28 03:45:00')
  })

  it('preserves meaningful subsecond precision', () => {
    const timestampWithFraction = (BigInt(Date.UTC(2026, 6, 28, 3, 45, 0)) * 1_000_000n + 123_456_789n).toString()

    expect(formatTimestampNs(timestampWithFraction)).toBe('2026-07-28 11:45:00.123456789')
  })

  it('falls back to the source text when the value is invalid', () => {
    expect(formatTimestampNs('not-a-timestamp')).toBe('not-a-timestamp')
  })
})
