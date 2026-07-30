import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it } from 'vitest'
import { quoteInfluxIdentifier, quoteInfluxString } from '../influxql'
import { useWorkbenchStore } from '../store'
import { QueryAssistant } from './QueryAssistant'

const initialState = useWorkbenchStore.getState()

afterEach(() => {
  cleanup()
  useWorkbenchStore.setState(initialState, true)
})

describe('QueryAssistant measurement binding', () => {
  it('shows no mock measurement, field, or tag defaults before a schema selection', () => {
    useWorkbenchStore.setState({ selectedMeasurement: undefined, measurementSchemaError: undefined })
    render(<QueryAssistant />)

    expect(screen.getByText('未选择 Measurement')).toBeTruthy()
    expect(screen.queryByRole('option', { name: /cpu/ })).toBeNull()
    expect(screen.queryByRole('option', { name: /usage_user/ })).toBeNull()
    expect(screen.queryByText('sh-east')).toBeNull()
  })

  it('builds only from the selected real fields and tags with safe quoting', () => {
    const measurement = 'sensor "north"\\rack'
    const field = 'temperature "raw"'
    const tag = "site'code"
    useWorkbenchStore.setState({
      selectedMeasurement: { database: 'metrics-live', measurement },
      measurementSchemaLoading: false,
      measurementSchemaError: undefined,
      schema: [{
        name: 'metrics-live',
        retentionPolicies: ['autogen'],
        measurements: [{
          name: measurement,
          fields: [{ name: field, type: 'float' }, { name: 'state', type: 'string' }],
          tags: [tag, 'host-live'],
        }],
      }],
      tabs: [{ id: 'query-1', title: 'Query', query: '', database: 'metrics-live', dirty: false }],
      activeTabId: 'query-1',
    })
    render(<QueryAssistant />)

    expect(screen.getByRole('option', { name: `${field} · float` })).toBeTruthy()
    expect(screen.getByRole('option', { name: tag })).toBeTruthy()
    expect(screen.queryByRole('option', { name: /usage_user/ })).toBeNull()
    expect(screen.queryByText('region')).toBeNull()

    fireEvent.change(screen.getByLabelText('Tag'), { target: { value: tag } })
    fireEvent.change(screen.getByLabelText('Tag value'), { target: { value: "o'hare\\west" } })
    fireEvent.click(screen.getByRole('button', { name: '应用到编辑器' }))

    const quotedField = quoteInfluxIdentifier(field)
    expect(useWorkbenchStore.getState().tabs[0].query).toBe(
      `SELECT mean(${quotedField}) AS ${quotedField}\n` +
      `FROM ${quoteInfluxIdentifier(measurement)}\n` +
      `WHERE time > now() - 6h\n` +
      `  AND ${quoteInfluxIdentifier(tag)} = ${quoteInfluxString("o'hare\\west")}\n` +
      `GROUP BY time(5m), ${quoteInfluxIdentifier(tag)} fill(null)`,
    )
  })

  it('restricts a selected string field to count instead of numeric mock aggregations', () => {
    useWorkbenchStore.setState({
      selectedMeasurement: { database: 'metrics', measurement: 'events' },
      schema: [{
        name: 'metrics', retentionPolicies: ['autogen'],
        measurements: [{ name: 'events', fields: [{ name: 'message', type: 'string' }], tags: [] }],
      }],
    })
    render(<QueryAssistant />)

    expect((screen.getByLabelText('Aggregation') as HTMLSelectElement).value).toBe('count')
    expect(screen.queryByRole('option', { name: 'mean' })).toBeNull()
  })
})
