import { describe, expect, it } from 'vitest'
import { defaultMeasurementQuery, quoteInfluxIdentifier, quoteInfluxString } from './influxql'

describe('InfluxQL quoting', () => {
  it('quotes identifiers without allowing quotes or backslashes to escape the identifier', () => {
    expect(quoteInfluxIdentifier('cpu "edge"\\raw')).toBe('"cpu \\"edge\\"\\\\raw"')
    expect(defaultMeasurementQuery('cpu "edge"\\raw')).toBe(
      'SELECT * FROM "cpu \\"edge\\"\\\\raw" WHERE time > now() - 5m',
    )
  })

  it('quotes tag values without allowing apostrophes or backslashes to escape the literal', () => {
    expect(quoteInfluxString("o'hare\\west")).toBe("'o\\'hare\\\\west'")
  })
})
