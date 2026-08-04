const NANOSECONDS_PER_SECOND = 1_000_000_000n
const UTC_PLUS_8_MILLISECONDS = 8 * 60 * 60 * 1_000

export type QueryResultTimeZone = 'utc+8' | 'utc'

function pad(value: number, length = 2): string {
  return String(value).padStart(length, '0')
}

export function formatTimestampNs(decimalText: string, timeZone: QueryResultTimeZone = 'utc+8'): string {
  try {
    const nanoseconds = BigInt(decimalText)
    let seconds = nanoseconds / NANOSECONDS_PER_SECOND
    let subsecond = nanoseconds % NANOSECONDS_PER_SECOND
    if (subsecond < 0n) {
      seconds -= 1n
      subsecond += NANOSECONDS_PER_SECOND
    }

    const milliseconds = Number(seconds * 1_000n + subsecond / 1_000_000n)
    const date = new Date(milliseconds + (timeZone === 'utc+8' ? UTC_PLUS_8_MILLISECONDS : 0))
    if (!Number.isFinite(milliseconds) || Number.isNaN(date.getTime())) return decimalText

    const dateTime = [
      pad(date.getUTCFullYear(), 4),
      '-',
      pad(date.getUTCMonth() + 1),
      '-',
      pad(date.getUTCDate()),
      ' ',
      pad(date.getUTCHours()),
      ':',
      pad(date.getUTCMinutes()),
      ':',
      pad(date.getUTCSeconds()),
    ].join('')
    if (subsecond === 0n) return dateTime

    const fraction = subsecond.toString().padStart(9, '0').replace(/0+$/, '')
    return `${dateTime}.${fraction}`
  } catch {
    return decimalText
  }
}
