import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useWorkbenchStore } from '../store'
import { buildServerUrl, ConnectionDialog, requiresInsecureAuthConsent } from './ConnectionDialog'

const initialState = useWorkbenchStore.getState()

afterEach(() => {
  cleanup()
  useWorkbenchStore.setState(initialState, true)
})

describe('ConnectionDialog direct connection flow', () => {
  it('requires explicit consent before a direct Basic connection uses remote HTTP', async () => {
    const saveConnection = vi.fn(async () => true)
    useWorkbenchStore.setState({
      connectionDialogOpen: true,
      connectionDialogProfileId: undefined,
      saveConnection,
    })
    render(<ConnectionDialog />)

    expect(screen.queryByText('认证方式')).toBeNull()
    expect(screen.queryByText('环境')).toBeNull()
    expect(screen.queryByText('SSH 隧道')).toBeNull()
    fireEvent.change(screen.getByLabelText('连接名称'), { target: { value: 'Data engine' } })
    fireEvent.change(screen.getByLabelText('主机'), { target: { value: '192.168.2.6' } })
    fireEvent.change(screen.getByLabelText('端口'), { target: { value: '8086' } })
    fireEvent.change(screen.getByLabelText('数据库'), { target: { value: 'data_engine' } })
    fireEvent.change(screen.getByLabelText('用户名'), { target: { value: 'operator' } })
    fireEvent.change(screen.getByLabelText('密码'), { target: { value: 'secret-value' } })
    fireEvent.click(screen.getByRole('button', { name: /^连接$/ }))
    expect((await screen.findByRole('alert')).textContent).toContain('必须确认明文传输风险')
    expect(saveConnection).not.toHaveBeenCalled()
    const consent = screen.getByRole('checkbox', { name: /允许通过 HTTP 发送认证信息/ })
    fireEvent.click(consent)
    fireEvent.click(screen.getByRole('button', { name: /^连接$/ }))

    await waitFor(() => expect(saveConnection).toHaveBeenCalledWith({
      id: undefined,
      expectedRevision: undefined,
      name: 'Data engine',
      baseUrl: 'http://192.168.2.6:8086',
      defaultDatabase: 'data_engine',
      username: 'operator',
      secret: 'secret-value',
      allowInsecureAuth: true,
      environment: 'development',
      authMode: undefined,
      protectionMode: 'ProtectedLocked',
    }))
  })

  it('prefills editable non-secret fields and leaves an empty password to preserve credentials', async () => {
    const saveConnection = vi.fn(async () => true)
    useWorkbenchStore.setState({
      connectionDialogOpen: true,
      connectionDialogProfileId: 'profile-1',
      connections: [{
        id: 'profile-1', revision: '7', name: 'Data engine',
        url: 'http://192.168.2.6:8086', defaultDatabase: 'data_engine',
        username: 'operator', authMode: 'BASIC', environment: 'production',
        allowInsecureAuth: true,
        protectionMode: 'ProtectedLocked', state: 'connected',
      }],
      saveConnection,
    })
    render(<ConnectionDialog />)

    await waitFor(() => expect((screen.getByLabelText('主机') as HTMLInputElement).value).toBe('192.168.2.6'))
    expect((screen.getByLabelText('端口') as HTMLInputElement).value).toBe('8086')
    expect((screen.getByLabelText('数据库') as HTMLInputElement).value).toBe('data_engine')
    expect((screen.getByLabelText('用户名') as HTMLInputElement).value).toBe('operator')
    expect((screen.getByLabelText('密码') as HTMLInputElement).value).toBe('')
    fireEvent.click(screen.getByRole('button', { name: '保存并重新连接' }))

    await waitFor(() => expect(saveConnection).toHaveBeenCalledWith(expect.objectContaining({
      id: 'profile-1', expectedRevision: '7', secret: undefined,
      allowInsecureAuth: true,
      baseUrl: 'http://192.168.2.6:8086', defaultDatabase: 'data_engine',
    })))
  })

  it('fails closed for a migrated authenticated HTTP profile until consent is checked', async () => {
    const saveConnection = vi.fn(async () => true)
    useWorkbenchStore.setState({
      connectionDialogOpen: true,
      connectionDialogProfileId: 'profile-legacy',
      connections: [{
        id: 'profile-legacy', revision: '3', name: 'Legacy LAN',
        url: 'http://192.168.2.8:8086', defaultDatabase: 'metrics',
        username: 'operator', authMode: 'BASIC', environment: 'development',
        allowInsecureAuth: false,
        protectionMode: 'ProtectedLocked', state: 'disconnected',
      }],
      saveConnection,
    })
    render(<ConnectionDialog />)

    const consent = await screen.findByRole('checkbox', { name: /允许通过 HTTP 发送认证信息/ })
    expect((consent as HTMLInputElement).checked).toBe(false)
    fireEvent.click(screen.getByRole('button', { name: '保存并重新连接' }))
    expect((await screen.findByRole('alert')).textContent).toContain('必须确认明文传输风险')
    expect(saveConnection).not.toHaveBeenCalled()
  })

  it('normalizes host and port without allowing URL credentials', () => {
    expect(buildServerUrl('192.168.2.6', '8086')).toBe('http://192.168.2.6:8086')
    expect(buildServerUrl('https://influx.example.test/proxy', '8443')).toBe('https://influx.example.test:8443/proxy')
    expect(() => buildServerUrl('http://user:secret@host', '8086')).toThrow('HOST_INVALID')
  })

  it('does not request insecure transport consent for HTTPS or loopback HTTP', () => {
    expect(requiresInsecureAuthConsent('https://influx.example.test:8086', true)).toBe(false)
    expect(requiresInsecureAuthConsent('http://127.0.0.1:8086', true)).toBe(false)
    expect(requiresInsecureAuthConsent('http://[::1]:8086', true)).toBe(false)
    expect(requiresInsecureAuthConsent('http://192.168.2.6:8086', false)).toBe(false)
    expect(requiresInsecureAuthConsent('http://192.168.2.6:8086', true)).toBe(true)
  })
})
