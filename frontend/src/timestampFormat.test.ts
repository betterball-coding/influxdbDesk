import { describe, expect, it } from 'vitest'
import { formatTimestampNs } from './timestampFormat'

describe('formatTimestampNs', () => {
  it('formats nanosecond timestamps as local date and time', () => {
    const localTime = new Date(2026, 6, 28, 11, 45, 0)
    const timestamp = (BigInt(localTime.getTime()) * 1_000_000n).toString()

    expect(formatTimestampNs(timestamp)).toBe('2026-07-28 11:45:00')
  })

  it('preserves meaningful subsecond precision', () => {
    const localTime = new Date(2026, 6, 28, 11, 45, 0)
    const timestamp = (BigInt(localTime.getTime()) * 1_000_000n + 123_456_789n).toString()

    expect(formatTimestampNs(timestamp)).toBe('2026-07-28 11:45:00.123456789')
  })

  it('falls back to the source text when the value is invalid', () => {
    expect(formatTimestampNs('not-a-timestamp')).toBe('not-a-timestamp')
  })
})
