import type { QueryResult, ResultColumn, TypedScalar } from './types'

export const MAX_SAFE_BIGINT_DELTA = 9_007_199_254_740_991n

const SIGNED_64_MIN = -9_223_372_036_854_775_808n
const SIGNED_64_MAX = 9_223_372_036_854_775_807n
const UNSIGNED_64_MAX = 18_446_744_073_709_551_615n
const SIGNED_DECIMAL = /^(?:0|-?[1-9][0-9]*)$/
const UNSIGNED_DECIMAL = /^(?:0|[1-9][0-9]*)$/
const EXACT_KINDS = new Set(['timestamp_ns', 'int64', 'uint64'])
const CHART_KINDS = new Set(['timestamp_ns', 'int64', 'uint64', 'float64', 'numeric_text'])

export type ChartProjectionStatus = 'exact' | 'approximate' | 'blocked'

export interface ProjectedAxis {
  column: ResultColumn
  values: Array<number | null>
  bigintOrigin?: bigint
  approximate: boolean
}

export interface ChartProjectionPoint {
  rowIndex: number
  x: number
  y: number | null
}

export interface ChartProjection {
  status: ChartProjectionStatus
  reason?: 'NO_NUMERIC_AXES' | 'INVALID_X_VALUE' | 'INVALID_Y_VALUE' | 'UNSAFE_X_DELTA' | 'UNSAFE_Y_DELTA'
  approximateAvailable: boolean
  xAxis?: ProjectedAxis
  yAxis?: ProjectedAxis
  points: ChartProjectionPoint[]
}

interface AxisResult {
  axis?: ProjectedAxis
  reason?: ChartProjection['reason']
  approximateAvailable: boolean
}

function hasDecimalText(value: TypedScalar | undefined): value is Extract<TypedScalar, { decimalText: string }> {
  return Boolean(value && 'decimalText' in value)
}

export function scalarDisplayText(value: TypedScalar | undefined): string {
  if (!value || value.kind === 'null') return 'NULL'
  if (value.kind === 'string') return value.value
  if (value.kind === 'boolean') return value.value ? 'true' : 'false'
  return value.decimalText
}

function parseExactScalar(value: TypedScalar | undefined, expectedKind: ResultColumn['kind']): bigint | undefined {
  if (!value || !hasDecimalText(value) || value.kind !== expectedKind) return undefined
  const unsigned = value.kind === 'uint64'
  if (!(unsigned ? UNSIGNED_DECIMAL : SIGNED_DECIMAL).test(value.decimalText)) return undefined
  try {
    const parsed = BigInt(value.decimalText)
    if (unsigned) return parsed <= UNSIGNED_64_MAX ? parsed : undefined
    return parsed >= SIGNED_64_MIN && parsed <= SIGNED_64_MAX ? parsed : undefined
  } catch {
    return undefined
  }
}

function bigintOrigin(values: bigint[]): bigint {
  let minimum = values[0]
  let maximum = values[0]
  for (let index = 1; index < values.length; index += 1) {
    const value = values[index]
    if (value < minimum) minimum = value
    if (value > maximum) maximum = value
  }
  return minimum + (maximum - minimum) / 2n
}

function projectExactAxis(
  result: QueryResult,
  column: ResultColumn,
  allowNull: boolean,
  allowApproximate: boolean,
  axisName: 'X' | 'Y',
): AxisResult {
  const parsed: Array<bigint | null> = []
  for (const row of result.rows) {
    const scalar = row.cells[column.key]
    if (allowNull && (!scalar || scalar.kind === 'null')) {
      parsed.push(null)
      continue
    }
    const value = parseExactScalar(scalar, column.kind)
    if (value === undefined) {
      return { reason: `INVALID_${axisName}_VALUE`, approximateAvailable: false }
    }
    parsed.push(value)
  }
  const concrete = parsed.filter((value): value is bigint => value !== null)
  if (concrete.length === 0) {
    return { reason: `INVALID_${axisName}_VALUE`, approximateAvailable: false }
  }
  const origin = bigintOrigin(concrete)
  const deltas = parsed.map((value) => value === null ? null : value - origin)
  const safe = deltas.every((delta) => delta === null || (delta >= -MAX_SAFE_BIGINT_DELTA && delta <= MAX_SAFE_BIGINT_DELTA))
  if (!safe && !allowApproximate) {
    return { reason: `UNSAFE_${axisName}_DELTA`, approximateAvailable: true }
  }
  const values = deltas.map((delta) => delta === null ? null : Number(delta))
  if (values.some((value) => value !== null && !Number.isFinite(value))) {
    return { reason: `INVALID_${axisName}_VALUE`, approximateAvailable: false }
  }
  return {
    axis: {
      column,
      values,
      bigintOrigin: origin,
      approximate: !safe,
    },
    approximateAvailable: false,
  }
}

function projectFloatAxis(
  result: QueryResult,
  column: ResultColumn,
  allowNull: boolean,
  axisName: 'X' | 'Y',
): AxisResult {
  const values: Array<number | null> = []
  for (const row of result.rows) {
    const scalar = row.cells[column.key]
    if (allowNull && (!scalar || scalar.kind === 'null')) {
      values.push(null)
      continue
    }
    if (!scalar || scalar.kind !== 'float64') {
      return { reason: `INVALID_${axisName}_VALUE`, approximateAvailable: false }
    }
    const value = Number(scalar.decimalText)
    if (!Number.isFinite(value)) {
      return { reason: `INVALID_${axisName}_VALUE`, approximateAvailable: false }
    }
    values.push(value)
  }
  if (!values.some((value) => value !== null)) {
    return { reason: `INVALID_${axisName}_VALUE`, approximateAvailable: false }
  }
  return { axis: { column, values, approximate: false }, approximateAvailable: false }
}

function projectNumericTextAxis(
  result: QueryResult,
  column: ResultColumn,
  allowNull: boolean,
  allowApproximate: boolean,
  axisName: 'X' | 'Y',
): AxisResult {
  if (!allowApproximate) {
    return { reason: `UNSAFE_${axisName}_DELTA`, approximateAvailable: true }
  }
  const values: Array<number | null> = []
  for (const row of result.rows) {
    const scalar = row.cells[column.key]
    if (allowNull && (!scalar || scalar.kind === 'null')) {
      values.push(null)
      continue
    }
    if (!scalar || scalar.kind !== 'numeric_text') {
      return { reason: `INVALID_${axisName}_VALUE`, approximateAvailable: false }
    }
    const value = Number(scalar.decimalText)
    if (!Number.isFinite(value)) {
      return { reason: `INVALID_${axisName}_VALUE`, approximateAvailable: false }
    }
    values.push(value)
  }
  if (!values.some((value) => value !== null)) {
    return { reason: `INVALID_${axisName}_VALUE`, approximateAvailable: false }
  }
  return { axis: { column, values, approximate: true }, approximateAvailable: false }
}

function projectAxis(
  result: QueryResult,
  column: ResultColumn,
  allowNull: boolean,
  allowApproximate: boolean,
  axisName: 'X' | 'Y',
): AxisResult {
  if (EXACT_KINDS.has(column.kind)) {
    return projectExactAxis(result, column, allowNull, allowApproximate, axisName)
  }
  if (column.kind === 'float64') return projectFloatAxis(result, column, allowNull, axisName)
  return projectNumericTextAxis(result, column, allowNull, allowApproximate, axisName)
}

export function buildChartProjection(result: QueryResult, allowApproximate = false): ChartProjection {
  if (result.rows.length === 0) {
    return { status: 'blocked', reason: 'NO_NUMERIC_AXES', approximateAvailable: false, points: [] }
  }
  const xColumn = result.columns.find((column) => column.kind === 'timestamp_ns')
    ?? result.columns.find((column) => CHART_KINDS.has(column.kind))
  const yColumn = result.columns.find((column) => column.key !== xColumn?.key && CHART_KINDS.has(column.kind) && column.kind !== 'timestamp_ns')
  if (!xColumn || !yColumn) {
    return { status: 'blocked', reason: 'NO_NUMERIC_AXES', approximateAvailable: false, points: [] }
  }
  const x = projectAxis(result, xColumn, false, allowApproximate, 'X')
  if (!x.axis) {
    return { status: 'blocked', reason: x.reason, approximateAvailable: x.approximateAvailable, points: [] }
  }
  const y = projectAxis(result, yColumn, true, allowApproximate, 'Y')
  if (!y.axis) {
    return { status: 'blocked', reason: y.reason, approximateAvailable: y.approximateAvailable, points: [] }
  }
  const points = result.rows.map((_, rowIndex) => ({
    rowIndex,
    x: x.axis!.values[rowIndex]!,
    y: y.axis!.values[rowIndex],
  }))
  return {
    status: x.axis.approximate || y.axis.approximate ? 'approximate' : 'exact',
    approximateAvailable: false,
    xAxis: x.axis,
    yAxis: y.axis,
    points,
  }
}

function escapeHTML(value: string): string {
  return value
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;')
}

export function formatChartTooltip(result: QueryResult, projection: ChartProjection, dataIndex: number): string {
  const point = projection.points[dataIndex]
  const xColumn = projection.xAxis?.column
  const yColumn = projection.yAxis?.column
  if (!point || !xColumn || !yColumn) return ''
  const row = result.rows[point.rowIndex]
  if (!row) return ''
  return [
    '<div class="chart-tip">',
    `<strong>${escapeHTML(result.seriesName)}</strong><br/>`,
    `${escapeHTML(xColumn.label)}&nbsp;&nbsp;${escapeHTML(scalarDisplayText(row.cells[xColumn.key]))}<br/>`,
    `${escapeHTML(yColumn.label)}&nbsp;&nbsp;${escapeHTML(scalarDisplayText(row.cells[yColumn.key]))}`,
    '</div>',
  ].join('')
}

export function formatProjectedAxisValue(axis: ProjectedAxis, value: number): string {
  if (axis.approximate) {
    if (!Number.isFinite(value)) return ''
    if (value === 0) return '0'
    const sign = value > 0 ? '+' : ''
    return `${sign}${value.toExponential(2)}`
  }
  if (axis.bigintOrigin === undefined || !Number.isSafeInteger(value)) return String(value)
  const exact = axis.bigintOrigin + BigInt(value)
  if (axis.column.kind !== 'timestamp_ns') return exact.toString()
  const milliseconds = exact / 1_000_000n
  if (milliseconds < -MAX_SAFE_BIGINT_DELTA || milliseconds > MAX_SAFE_BIGINT_DELTA) return exact.toString()
  const numericMilliseconds = Number(milliseconds)
  try {
    return new Date(numericMilliseconds).toISOString().slice(11, 19)
  } catch {
    return exact.toString()
  }
}
