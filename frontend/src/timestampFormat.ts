const NANOSECONDS_PER_SECOND = 1_000_000_000n

function pad(value: number, length = 2): string {
  return String(value).padStart(length, '0')
}

export function formatTimestampNs(decimalText: string): string {
  try {
    const nanoseconds = BigInt(decimalText)
    let seconds = nanoseconds / NANOSECONDS_PER_SECOND
    let subsecond = nanoseconds % NANOSECONDS_PER_SECOND
    if (subsecond < 0n) {
      seconds -= 1n
      subsecond += NANOSECONDS_PER_SECOND
    }

    const milliseconds = Number(seconds * 1_000n + subsecond / 1_000_000n)
    const date = new Date(milliseconds)
    if (!Number.isFinite(milliseconds) || Number.isNaN(date.getTime())) return decimalText

    const dateTime = [
      pad(date.getFullYear(), 4),
      '-',
      pad(date.getMonth() + 1),
      '-',
      pad(date.getDate()),
      ' ',
      pad(date.getHours()),
      ':',
      pad(date.getMinutes()),
      ':',
      pad(date.getSeconds()),
    ].join('')
    if (subsecond === 0n) return dateTime

    const fraction = subsecond.toString().padStart(9, '0').replace(/0+$/, '')
    return `${dateTime}.${fraction}`
  } catch {
    return decimalText
  }
}
