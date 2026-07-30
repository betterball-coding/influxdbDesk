import { describe, expect, it } from 'vitest'
import {
  buildChartProjection,
  formatChartTooltip,
  formatProjectedAxisValue,
  MAX_SAFE_BIGINT_DELTA,
} from './chartPrecision'
import type { QueryResult, ScalarKind, TypedScalar } from './types'

function decimal(kind: 'timestamp_ns' | 'int64' | 'uint64' | 'float64' | 'numeric_text', decimalText: string): TypedScalar {
  return { kind, decimalText }
}

function resultFor(
  xKind: ScalarKind,
  yKind: ScalarKind,
  values: Array<[TypedScalar, TypedScalar]>,
  seriesName = 'cpu',
): QueryResult {
  return {
    sessionId: 'session-precision',
    statementId: 0,
    seriesName,
    columns: [
      { key: 'time', label: 'time', kind: xKind },
      { key: 'value', label: 'value', kind: yKind },
    ],
    rows: values.map(([time, value], index) => ({
      id: `row-${index}`,
      cells: { time, value },
    })),
    complete: true,
    elapsedMs: 1,
    scannedBytes: 0,
  }
}

describe('chart precision projection', () => {
  it('keeps adjacent 19-digit timestamps and integers distinct through safe BigInt deltas', () => {
    const result = resultFor('timestamp_ns', 'int64', [
      [decimal('timestamp_ns', '1700000000000000001'), decimal('int64', '9007199254740992')],
      [decimal('timestamp_ns', '1700000000000000002'), decimal('int64', '9007199254740993')],
      [decimal('timestamp_ns', '1700000000000000003'), decimal('int64', '9007199254740994')],
    ])
    const sourceSnapshot = JSON.stringify(result)

    const projection = buildChartProjection(result)

    expect(projection.status).toBe('exact')
    expect(projection.points.map((point) => point.x)).toEqual([-1, 0, 1])
    expect(projection.points.map((point) => point.y)).toEqual([-1, 0, 1])
    expect(new Set(projection.points.map((point) => point.x)).size).toBe(3)
    expect(new Set(projection.points.map((point) => point.y)).size).toBe(3)
    expect(JSON.stringify(result)).toBe(sourceSnapshot)
  })

  it('projects uint64 maximum values without converting their raw decimal text to Number', () => {
    const result = resultFor('timestamp_ns', 'uint64', [
      [decimal('timestamp_ns', '1700000000000000001'), decimal('uint64', '18446744073709551614')],
      [decimal('timestamp_ns', '1700000000000000002'), decimal('uint64', '18446744073709551615')],
    ])

    const projection = buildChartProjection(result)

    expect(projection.status).toBe('exact')
    expect(projection.points.map((point) => point.y)).toEqual([0, 1])
    expect(projection.yAxis?.bigintOrigin).toBe(18_446_744_073_709_551_614n)
  })

  it('accepts the largest safe common-origin range and blocks a range one unit beyond it', () => {
    const safeRange = MAX_SAFE_BIGINT_DELTA * 2n
    const safe = resultFor('uint64', 'int64', [
      [decimal('uint64', '0'), decimal('int64', '1')],
      [decimal('uint64', safeRange.toString()), decimal('int64', '2')],
    ])
    const unsafe = resultFor('uint64', 'int64', [
      [decimal('uint64', '0'), decimal('int64', '1')],
      [decimal('uint64', (safeRange + 1n).toString()), decimal('int64', '2')],
    ])

    const safeProjection = buildChartProjection(safe)
    const blockedProjection = buildChartProjection(unsafe)

    expect(safeProjection.status).toBe('exact')
    expect(safeProjection.points.map((point) => point.x)).toEqual([
      -Number(MAX_SAFE_BIGINT_DELTA),
      Number(MAX_SAFE_BIGINT_DELTA),
    ])
    expect(blockedProjection).toMatchObject({
      status: 'blocked',
      reason: 'UNSAFE_X_DELTA',
      approximateAvailable: true,
    })
  })

  it('requires explicit approximate mode for an unsafe delta and marks the projection continuously', () => {
    const result = resultFor('timestamp_ns', 'int64', [
      [decimal('timestamp_ns', '-9223372036854775808'), decimal('int64', '1')],
      [decimal('timestamp_ns', '9223372036854775807'), decimal('int64', '2')],
    ])

    expect(buildChartProjection(result)).toMatchObject({
      status: 'blocked',
      reason: 'UNSAFE_X_DELTA',
      approximateAvailable: true,
    })
    const approximate = buildChartProjection(result, true)
    expect(approximate.status).toBe('approximate')
    expect(approximate.xAxis?.approximate).toBe(true)
    expect(approximate.points.every((point) => Number.isFinite(point.x))).toBe(true)
    const labels = approximate.points.map((point) => formatProjectedAxisValue(approximate.xAxis!, point.x))
    expect(labels).toEqual(['-9.22e+18', '+9.22e+18'])
    expect(labels.every((label) => label.length <= 10)).toBe(true)
  })

  it('always formats tooltip values from the original decimalText, including float -0', () => {
    const result = resultFor('timestamp_ns', 'float64', [
      [decimal('timestamp_ns', '1700000000000000001'), decimal('float64', '-0')],
      [decimal('timestamp_ns', '1700000000000000002'), decimal('float64', '1.7976931348623157e+308')],
    ], '<cpu>')
    const projection = buildChartProjection(result)

    const first = formatChartTooltip(result, projection, 0)
    const second = formatChartTooltip(result, projection, 1)

    expect(first).toContain('1700000000000000001')
    expect(first).toContain('value&nbsp;&nbsp;-0')
    expect(second).toContain('1.7976931348623157e+308')
    expect(first).toContain('&lt;cpu&gt;')
    expect(first).not.toContain('<strong><cpu>')
  })

  it('rejects non-finite float input even when approximate mode is requested', () => {
    const result = resultFor('timestamp_ns', 'float64', [
      [decimal('timestamp_ns', '1700000000000000001'), decimal('float64', '1e309')],
    ])

    expect(buildChartProjection(result)).toMatchObject({
      status: 'blocked',
      reason: 'INVALID_Y_VALUE',
      approximateAvailable: false,
    })
    expect(buildChartProjection(result, true).status).toBe('blocked')
  })
})
