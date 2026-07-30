import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import React from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useWorkbenchStore } from '../store'
import type { QueryResult } from '../types'
import { ResultChart, ResultGrid, ResultPane } from './ResultPane'

vi.mock('echarts/core', () => ({
  use: vi.fn(),
  init: vi.fn(() => ({
    setOption: vi.fn(),
    resize: vi.fn(),
    dispose: vi.fn(),
  })),
}))
vi.mock('echarts/charts', () => ({ LineChart: {} }))
vi.mock('echarts/components', () => ({ GridComponent: {}, TooltipComponent: {} }))
vi.mock('echarts/renderers', () => ({ CanvasRenderer: {} }))

class ResizeObserverStub {
  observe() {}
  disconnect() {}
}

const unsafeResult: QueryResult = {
  sessionId: 'session-unsafe-chart',
  statementId: 0,
  seriesName: 'cpu',
  columns: [
    { key: 'time', label: 'time', kind: 'timestamp_ns' },
    { key: 'value', label: 'value', kind: 'int64' },
  ],
  rows: [
    {
      id: 'row-1',
      cells: {
        time: { kind: 'timestamp_ns', decimalText: '-9223372036854775808' },
        value: { kind: 'int64', decimalText: '9007199254740992' },
      },
    },
    {
      id: 'row-2',
      cells: {
        time: { kind: 'timestamp_ns', decimalText: '9223372036854775807' },
        value: { kind: 'int64', decimalText: '9007199254740993' },
      },
    },
  ],
  complete: true,
  elapsedMs: 1,
  scannedBytes: 0,
}

const initialWorkbenchState = useWorkbenchStore.getState()

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
  useWorkbenchStore.setState(initialWorkbenchState, true)
})

describe('ResultChart precision guard', () => {
  it('blocks unsafe deltas by default and keeps a warning visible after explicit approximation', () => {
    vi.stubGlobal('ResizeObserver', ResizeObserverStub)
    render(<ResultChart result={unsafeResult} />)

    expect(screen.getByText('数值范围超出精确绘图边界')).toBeTruthy()
    expect(screen.queryByLabelText('查询结果折线图')).toBeNull()

    fireEvent.click(screen.getByRole('checkbox', { name: '近似投影' }))

    expect(screen.getByLabelText('查询结果折线图')).toBeTruthy()
    expect(screen.getByText(/近似投影已启用/)).toBeTruthy()
    expect((screen.getByRole('checkbox', { name: '近似投影' }) as HTMLInputElement).checked).toBe(true)
  })
})

describe('ResultGrid column resizing', () => {
  it('resizes a column with pointer movement and keyboard controls', () => {
    render(<ResultGrid result={unsafeResult} />)
    const header = document.querySelector('.result-grid-header') as HTMLDivElement
    const separator = screen.getByRole('separator', { name: '调整 time 列宽' })

    expect(header.style.gridTemplateColumns).toBe('36px 140px 140px')

    fireEvent.pointerDown(separator, { clientX: 140, pointerId: 1 })
    fireEvent.pointerMove(separator, { clientX: 200, pointerId: 1 })
    fireEvent.pointerUp(separator, { clientX: 200, pointerId: 1 })
    expect(header.style.gridTemplateColumns).toBe('36px 200px 140px')

    fireEvent.keyDown(separator, { key: 'ArrowLeft' })
    expect(header.style.gridTemplateColumns).toBe('36px 190px 140px')
  })

  it('returns stable row ids for individual and select-all changes', () => {
    const onSelectedRowsChange = vi.fn()
    const view = render(
      <ResultGrid result={unsafeResult} onSelectedRowsChange={onSelectedRowsChange} />,
    )

    fireEvent.click(screen.getByRole('checkbox', { name: '选择结果行 row-1' }))
    expect([...onSelectedRowsChange.mock.calls[0][0]]).toEqual(['row-1'])

    view.rerender(
      <ResultGrid
        result={unsafeResult}
        onSelectedRowsChange={onSelectedRowsChange}
        selectedRowIds={new Set(['row-1'])}
      />,
    )
    const selectAll = screen.getByRole('checkbox', { name: '选择当前已加载的全部行' }) as HTMLInputElement
    expect(selectAll.indeterminate).toBe(true)

    fireEvent.click(selectAll)
    expect([...onSelectedRowsChange.mock.calls[1][0]]).toEqual(['row-1', 'row-2'])
  })

  it('selects a continuous set of rows while the pointer is dragged over the checkbox rail', () => {
    function SelectionHarness() {
      const [selected, setSelected] = React.useState<Set<string>>(new Set())
      return <ResultGrid result={unsafeResult} selectedRowIds={selected} onSelectedRowsChange={setSelected} />
    }

    render(<SelectionHarness />)
    const first = screen.getByRole('checkbox', { name: '选择结果行 row-1' })
    const second = screen.getByRole('checkbox', { name: '选择结果行 row-2' })
    const firstCell = first.closest('label')!
    const secondCell = second.closest('label')!

    fireEvent.pointerDown(firstCell, { button: 0, buttons: 1, pointerId: 7, pointerType: 'mouse' })
    fireEvent.pointerEnter(secondCell, { buttons: 1, pointerId: 7, pointerType: 'mouse' })
    fireEvent.pointerUp(window, { button: 0, buttons: 0, pointerId: 7, pointerType: 'mouse' })

    expect((first as HTMLInputElement).checked).toBe(true)
    expect((second as HTMLInputElement).checked).toBe(true)
  })
})

describe('ResultPane inline export menu', () => {
  it('shows selected and total row counts without opening the task drawer', async () => {
    useWorkbenchStore.setState({
      queryState: 'succeeded',
      result: { ...unsafeResult, totalRows: '2' },
      taskDrawerOpen: false,
    })
    render(<ResultPane />)

    fireEvent.click(screen.getByRole('checkbox', { name: '选择结果行 row-1' }))
    fireEvent.pointerDown(screen.getByRole('button', { name: '导出结果' }), { button: 0 })

    const selectedItem = await screen.findByRole('menuitem', { name: /导出所选行/ })
    expect(selectedItem.textContent).toContain('1 行')
    expect(screen.getByRole('menuitem', { name: /导出全部查询结果/ }).textContent).toContain('2 行')
    expect(useWorkbenchStore.getState().taskDrawerOpen).toBe(false)
  })
})
