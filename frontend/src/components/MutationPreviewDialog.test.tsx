import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useWorkbenchStore } from '../store'
import { MutationPreviewDialog } from './MutationPreviewDialog'

const initialWorkbenchState = useWorkbenchStore.getState()

afterEach(() => {
  cleanup()
  useWorkbenchStore.setState(initialWorkbenchState, true)
})

describe('MutationPreviewDialog', () => {
  it('shows a locked preview with close as the only command and clears the preview', () => {
    useWorkbenchStore.setState({
      mutationDialogOpen: true,
      mutationPhase: 'preview',
      mutationPreview: {
        canonicalQuery: 'DROP DATABASE "telemetry"',
        operationKind: 'DROP_DATABASE',
        target: 'telemetry',
        confirmationRequired: true,
        executable: false,
      },
    })
    render(<MutationPreviewDialog />)

    expect(screen.getByText('连接仍处于保护锁定状态')).toBeTruthy()
    expect(screen.queryByRole('button', { name: '执行变更' })).toBeNull()
    expect(screen.queryByLabelText('输入目标名称以确认')).toBeNull()
    fireEvent.click(screen.getAllByRole('button', { name: '关闭' }).at(-1)!)
    expect(useWorkbenchStore.getState().mutationPreview).toBeUndefined()
  })

  it('requires an exact target before dispatching an executable preview', () => {
    const executeMutation = vi.fn(async () => undefined)
    useWorkbenchStore.setState({
      mutationDialogOpen: true,
      mutationPhase: 'preview',
      mutationPreview: {
        canonicalQuery: 'DROP DATABASE "telemetry"',
        operationKind: 'DROP_DATABASE',
        target: 'telemetry',
        confirmationRequired: true,
        executable: true,
        token: 'opaque-token',
        expiresAt: '2026-07-23T00:02:00Z',
      },
      executeMutation,
    })
    render(<MutationPreviewDialog />)

    const confirmation = screen.getByLabelText('输入目标名称以确认')
    const execute = screen.getByRole('button', { name: '执行变更' }) as HTMLButtonElement
    expect(execute.disabled).toBe(true)
    fireEvent.change(confirmation, { target: { value: 'Telemetry' } })
    expect(execute.disabled).toBe(true)
    fireEvent.change(confirmation, { target: { value: 'telemetry' } })
    expect(execute.disabled).toBe(false)
    fireEvent.click(execute)
    expect(executeMutation).toHaveBeenCalledWith('telemetry')
  })
})
