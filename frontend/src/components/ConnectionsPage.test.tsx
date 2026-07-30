import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useWorkbenchStore } from '../store'
import { ConnectionsPage } from './ConnectionsPage'

const initialState = useWorkbenchStore.getState()

afterEach(() => {
  cleanup()
  useWorkbenchStore.setState(initialState, true)
})

describe('ConnectionsPage management actions', () => {
  it('opens edit mode and requires confirmation before deletion', async () => {
    const deleteConnection = vi.fn(async () => undefined)
    useWorkbenchStore.setState({
      activeConnectionId: 'profile-1',
      connections: [{
        id: 'profile-1', revision: '3', name: 'Data engine',
        url: 'http://192.168.2.6:8086', defaultDatabase: 'data_engine',
        username: 'operator', authMode: 'BASIC', environment: 'production',
        protectionMode: 'ProtectedLocked', state: 'connected', version: '1.12.2',
      }],
      deleteConnection,
    })
    render(<ConnectionsPage />)

    expect(screen.getByText('data_engine')).toBeTruthy()
    expect(screen.getByText('operator')).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: '编辑 Data engine' }))
    expect(useWorkbenchStore.getState()).toMatchObject({
      connectionDialogOpen: true,
      connectionDialogProfileId: 'profile-1',
    })

    fireEvent.click(screen.getByRole('button', { name: '删除 Data engine' }))
    expect(screen.getByRole('dialog', { name: '删除连接' })).toBeTruthy()
    expect(deleteConnection).not.toHaveBeenCalled()
    fireEvent.click(screen.getByRole('button', { name: /^删除$/ }))
    await waitFor(() => expect(deleteConnection).toHaveBeenCalledWith('profile-1'))
  })
})
