import { describe, expect, it } from 'vitest'
import { buildQueryResultCSV, queryResultFilename } from './queryResultCsv'
import type { QueryResult } from './types'

const result: QueryResult = {
  sessionId: 'session-export',
  statementId: 0,
  seriesName: 'cpu/load',
  columns: [
    { key: 'time', label: 'time', kind: 'timestamp_ns' },
    { key: 'host', label: 'host', kind: 'string' },
    { key: 'value', label: 'value', kind: 'int64' },
  ],
  rows: [{
    id: 'row-0',
    cells: {
      time: { kind: 'timestamp_ns', decimalText: '1700000000123456788' },
      host: { kind: 'string', value: 'edge,"01"' },
      value: { kind: 'int64', decimalText: '9007199254740993' },
    },
  }],
  complete: true,
  elapsedMs: 1,
  scannedBytes: 0,
}

describe('query result CSV', () => {
  it('keeps exact values, readable time, and RFC 4180 escaping', () => {
    const csv = buildQueryResultCSV(result, result.rows)

    expect(csv).toContain('time,host,value\r\n')
    expect(csv).toContain('9007199254740993')
    expect(csv).toContain('"edge,""01"""')
    expect(csv).not.toContain('1700000000123456788')
  })

  it('creates a filesystem-safe CSV filename', () => {
    expect(queryResultFilename(result.seriesName)).toBe('cpu_load-query-result.csv')
  })
})
