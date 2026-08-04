import { scalarDisplayText } from './chartPrecision'
import { formatTimestampNs } from './timestampFormat'
import type { QueryResultTimeZone } from './timestampFormat'
import type { QueryResult, ResultRow, TypedScalar } from './types'

function cellText(value: TypedScalar | undefined, timeZone: QueryResultTimeZone): string {
  if (!value || value.kind === 'null') return ''
  if (value.kind === 'timestamp_ns') return formatTimestampNs(value.decimalText, timeZone)
  return scalarDisplayText(value)
}

function escapeCSV(value: string): string {
  if (!/[",\r\n]/.test(value)) return value
  return `"${value.replaceAll('"', '""')}"`
}

function neutralizeCSVFormula(value: string): string {
  return /^[=+\-@\t\r]/.test(value) ? `'${value}` : value
}

function cellCSVText(value: TypedScalar | undefined, timeZone: QueryResultTimeZone): string {
  const text = cellText(value, timeZone)
  return value?.kind === 'string' ? neutralizeCSVFormula(text) : text
}

export function buildQueryResultCSV(
  result: QueryResult,
  rows: ResultRow[],
  timeZone: QueryResultTimeZone = 'utc+8',
): string {
  const records = [
    result.columns.map((column) => escapeCSV(neutralizeCSVFormula(column.label))).join(','),
    ...rows.map((row) => result.columns
      .map((column) => escapeCSV(cellCSVText(row.cells[column.key], timeZone)))
      .join(',')),
  ]
  return `\ufeff${records.join('\r\n')}\r\n`
}

export function queryResultFilename(seriesName: string): string {
  const safeName = seriesName.trim().replace(/[<>:"/\\|?*\u0000-\u001f]/g, '_') || 'query-result'
  return `${safeName}-query-result.csv`
}

export function downloadQueryResultCSV(
  result: QueryResult,
  rows: ResultRow[],
  timeZone: QueryResultTimeZone = 'utc+8',
): void {
  const url = URL.createObjectURL(new Blob([buildQueryResultCSV(result, rows, timeZone)], { type: 'text/csv;charset=utf-8' }))
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = queryResultFilename(result.seriesName)
  anchor.click()
  window.setTimeout(() => URL.revokeObjectURL(url), 0)
}
