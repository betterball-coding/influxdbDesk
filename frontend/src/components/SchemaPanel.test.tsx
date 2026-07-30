import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useWorkbenchStore } from '../store'
import { SchemaPanel } from './SchemaPanel'

const initialState = useWorkbenchStore.getState()

afterEach(() => {
  cleanup()
  delete window.go
  useWorkbenchStore.setState(initialState, true)
})

describe('SchemaPanel measurements', () => {
  it('renders every loaded measurement without false zero field counts', () => {
    useWorkbenchStore.setState({
      activeConnectionId: 'profile-1',
      connections: [{
        id: 'profile-1', name: 'data_engine', url: 'http://127.0.0.1:8086',
        environment: 'production', state: 'connected',
      }],
      schemaLoading: false,
      schema: [{
        name: 'data_engine',
        retentionPolicies: ['autogen'],
        measurements: [
          { name: 'dm_001', fields: [], tags: [] },
          { name: 'dm_002', fields: [], tags: [] },
          { name: 'dm_003', fields: [], tags: [] },
        ],
      }],
    })

    const { container } = render(<SchemaPanel />)

    expect(screen.getByText('dm_001')).toBeTruthy()
    expect(screen.getByText('dm_002')).toBeTruthy()
    expect(screen.getByText('dm_003')).toBeTruthy()
    expect(container.querySelectorAll('.tree-node--measurement')).toHaveLength(3)
    expect(container.querySelectorAll('.tree-node--measurement .tree-count')).toHaveLength(0)
  })

  it('selects a measurement, safely updates the active query, and merges native field and tag details', async () => {
    const measurement = 'cpu "edge"\\raw'
    const getMeasurementSchema = vi.fn(async () => ({
      name: measurement,
      fields: [{ name: 'temperature', type: 'float' }],
      tags: ['host'],
    }))
    window.go = { main: { App: { GetMeasurementSchema: getMeasurementSchema } } }
    useWorkbenchStore.setState({
      activeConnectionId: 'profile-1',
      schemaLoading: false,
      selectedMeasurement: undefined,
      measurementSchemaLoading: false,
      schema: [{
        name: 'metrics',
        retentionPolicies: ['autogen'],
        measurements: [{ name: measurement, fields: [], tags: [] }],
      }],
      tabs: [{ id: 'query-1', title: 'Query', query: 'SELECT 1', database: 'old', dirty: false }],
      activeTabId: 'query-1',
    })

    render(<SchemaPanel />)
    fireEvent.click(screen.getByRole('button', { name: measurement }))

    expect(getMeasurementSchema).toHaveBeenCalledWith('profile-1', 'metrics', measurement)
    await waitFor(() => expect(screen.getByText('temperature')).toBeTruthy())
    expect(screen.getByText('host')).toBeTruthy()
    expect(useWorkbenchStore.getState().selectedMeasurement).toEqual({
      database: 'metrics', measurement,
    })
    expect(useWorkbenchStore.getState().tabs[0]).toMatchObject({
      database: 'metrics',
      query: 'SELECT * FROM "cpu \\"edge\\"\\\\raw" WHERE time > now() - 5m',
      dirty: true,
    })
    expect(useWorkbenchStore.getState().schema[0].measurements[0]).toEqual({
      name: measurement,
      fields: [{ name: 'temperature', type: 'float' }],
      tags: ['host'],
      detailLoaded: true,
    })

    fireEvent.click(screen.getByText(measurement).closest('button')!)
    fireEvent.click(screen.getByText(measurement).closest('button')!)
    await waitFor(() => expect(screen.getByText('temperature')).toBeTruthy())
    expect(getMeasurementSchema).toHaveBeenCalledTimes(1)
  })

  it('does not let a slower previous measurement response overwrite the latest selection', async () => {
    let resolveFirst: ((value: unknown) => void) | undefined
    let resolveSecond: ((value: unknown) => void) | undefined
    const getMeasurementSchema = vi.fn((_profileId: unknown, _database: unknown, measurement: unknown) =>
      new Promise((resolve) => {
        if (measurement === 'first') resolveFirst = resolve
        else resolveSecond = resolve
      }),
    )
    window.go = { main: { App: { GetMeasurementSchema: getMeasurementSchema } } }
    useWorkbenchStore.setState({
      activeConnectionId: 'profile-1',
      schema: [{
        name: 'metrics', retentionPolicies: ['autogen'],
        measurements: [
          { name: 'first', fields: [], tags: [] },
          { name: 'second', fields: [], tags: [] },
        ],
      }],
      tabs: [{ id: 'query-1', title: 'Query', query: '', database: 'metrics', dirty: false }],
      activeTabId: 'query-1',
    })

    const first = useWorkbenchStore.getState().selectMeasurement('metrics', 'first')
    const second = useWorkbenchStore.getState().selectMeasurement('metrics', 'second')
    resolveSecond?.({ name: 'second', fields: [{ name: 'newest', type: 'integer' }], tags: ['current'] })
    await second
    resolveFirst?.({ name: 'first', fields: [{ name: 'stale', type: 'float' }], tags: ['old'] })
    await first

    const state = useWorkbenchStore.getState()
    expect(state.selectedMeasurement).toEqual({ database: 'metrics', measurement: 'second' })
    expect(state.schema[0].measurements).toEqual([
      {
        name: 'first', fields: [{ name: 'stale', type: 'float' }], tags: ['old'],
        detailLoaded: true,
      },
      {
        name: 'second', fields: [{ name: 'newest', type: 'integer' }], tags: ['current'],
        detailLoaded: true,
      },
    ])
    expect(state.tabs[0].query).toBe('SELECT * FROM "second" WHERE time > now() - 5m')

    await useWorkbenchStore.getState().selectMeasurement('metrics', 'first')
    expect(getMeasurementSchema).toHaveBeenCalledTimes(2)
  })

  it('keeps a very large measurement catalog virtualized and searchable', async () => {
    const measurements = Array.from({ length: 64_149 }, (_, index) => ({
      name: `measurement-${index}`,
      fields: [],
      tags: [],
    }))
    useWorkbenchStore.setState({
      activeConnectionId: 'profile-large',
      connections: [{
        id: 'profile-large', name: 'Large catalog', url: 'http://127.0.0.1:8086',
        defaultDatabase: 'data_engine', environment: 'production', state: 'connected',
      }],
      schemaLoading: false,
      schema: [{ name: 'data_engine', retentionPolicies: ['autogen'], measurements }],
    })

    const { container } = render(<SchemaPanel />)
    await waitFor(() => expect(screen.getByText('measurement-0')).toBeTruthy())
    expect(container.querySelectorAll('.tree-node--measurement').length).toBeLessThan(100)

    fireEvent.change(screen.getByPlaceholderText('筛选数据库对象'), {
      target: { value: 'measurement-64148' },
    })
    await waitFor(() => expect(screen.getByText('measurement-64148')).toBeTruthy())
    expect(container.querySelectorAll('.tree-node--measurement')).toHaveLength(1)
  })
})
