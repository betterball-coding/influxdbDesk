import { Eye, EyeOff, Server, X } from 'lucide-react'
import { useEffect, useState } from 'react'
import type { ConnectionDraft } from '../bridge'
import { useWorkbenchStore } from '../store'
import type { ConnectionProfile } from '../types'
import { IconButton } from './IconButton'

interface ConnectionForm {
  name: string
  host: string
  port: string
  database: string
  username: string
  password: string
}

const emptyForm: ConnectionForm = {
  name: '',
  host: '127.0.0.1',
  port: '8086',
  database: '',
  username: '',
  password: '',
}

export function buildServerUrl(host: string, port: string): string {
  const normalizedHost = host.trim()
  const normalizedPort = port.trim()
  if (!normalizedHost) throw new Error('HOST_REQUIRED')
  if (!/^\d{1,5}$/.test(normalizedPort)) throw new Error('PORT_INVALID')
  const portNumber = Number(normalizedPort)
  if (portNumber < 1 || portNumber > 65535) throw new Error('PORT_INVALID')

  const candidate = /^[a-z][a-z\d+.-]*:\/\//i.test(normalizedHost)
    ? normalizedHost
    : `http://${normalizedHost}`
  const url = new URL(candidate)
  if ((url.protocol !== 'http:' && url.protocol !== 'https:') || url.username || url.password) {
    throw new Error('HOST_INVALID')
  }
  url.port = normalizedPort
  url.search = ''
  url.hash = ''
  const value = url.toString()
  return url.pathname === '/' ? value.slice(0, -1) : value.replace(/\/$/, '')
}

function formForProfile(profile: ConnectionProfile | undefined): ConnectionForm {
  if (!profile) return { ...emptyForm }
  try {
    const url = new URL(profile.url)
    const path = url.pathname === '/' ? '' : url.pathname.replace(/\/$/, '')
    const host = `${url.protocol === 'http:' ? '' : `${url.protocol}//`}${url.hostname}${path}`
    const port = url.port || (url.protocol === 'https:' ? '443' : '80')
    return {
      name: profile.name,
      host,
      port,
      database: profile.defaultDatabase ?? '',
      username: profile.username ?? '',
      password: '',
    }
  } catch {
    return { ...emptyForm, name: profile.name, host: profile.url, database: profile.defaultDatabase ?? '' }
  }
}

export function ConnectionDialog() {
  const open = useWorkbenchStore((state) => state.connectionDialogOpen)
  const editingId = useWorkbenchStore((state) => state.connectionDialogProfileId)
  const connections = useWorkbenchStore((state) => state.connections)
  const setOpen = useWorkbenchStore((state) => state.setConnectionDialogOpen)
  const saveConnection = useWorkbenchStore((state) => state.saveConnection)
  const editingProfile = editingId ? connections.find((profile) => profile.id === editingId) : undefined
  const [showPassword, setShowPassword] = useState(false)
  const [busy, setBusy] = useState(false)
  const [message, setMessage] = useState('')
  const [form, setForm] = useState<ConnectionForm>(emptyForm)

  useEffect(() => {
    if (!open) return
    setForm(formForProfile(editingProfile))
    setShowPassword(false)
    setMessage('')
  }, [editingId, open])

  if (!open) return null

  const close = () => {
    setForm((current) => ({ ...current, password: '' }))
    setShowPassword(false)
    setMessage('')
    setOpen(false)
  }

  const update = <K extends keyof ConnectionForm>(key: K, value: ConnectionForm[K]) => {
    setForm((current) => ({ ...current, [key]: value }))
    setMessage('')
  }

  const connect = async () => {
    const host = form.host.trim()
    const database = form.database.trim()
    const username = form.username.trim()
    const password = form.password
    if (!host || !database) {
      setMessage('主机、端口和数据库不能为空。')
      return
    }
    if (password && !username) {
      setMessage('填写密码时必须同时填写用户名。')
      return
    }
    if (!editingProfile && Boolean(username) !== Boolean(password)) {
      setMessage('用户名和密码必须同时填写，或同时留空。')
      return
    }
    if (editingProfile?.authMode === 'NONE' && Boolean(username) !== Boolean(password)) {
      setMessage('启用账号认证时，用户名和密码必须同时填写。')
      return
    }

    let baseUrl: string
    try {
      baseUrl = buildServerUrl(host, form.port)
    } catch {
      setMessage('主机或端口格式无效。')
      return
    }

    const draft: ConnectionDraft = {
      id: editingProfile?.id,
      expectedRevision: editingProfile?.revision,
      name: form.name.trim() || `${host}:${form.port.trim()}`,
      baseUrl,
      defaultDatabase: database,
      username,
      secret: password || undefined,
      environment: editingProfile?.environment ?? 'development',
      authMode: editingProfile?.authMode,
      protectionMode: editingProfile?.protectionMode ?? 'ProtectedLocked',
    }

    setForm((current) => ({ ...current, password: '' }))
    setShowPassword(false)
    setBusy(true)
    try {
      await saveConnection(draft)
    } catch {
      setMessage(editingProfile ? '连接修改失败，原配置未被替换。' : '连接失败，请检查地址、数据库和账号。')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="modal-layer" role="presentation">
      <button className="modal-scrim" aria-label="关闭连接窗口" onClick={close} />
      <section className="connection-dialog connection-dialog--direct" role="dialog" aria-modal="true" aria-labelledby="connection-dialog-title">
        <header className="dialog-header">
          <div className="dialog-icon"><Server size={19} /></div>
          <div><span className="eyebrow">INFLUXDB 1.X</span><h2 id="connection-dialog-title">{editingProfile ? '编辑连接' : '新建连接'}</h2></div>
          <IconButton label="关闭" onClick={close}><X size={18} /></IconButton>
        </header>
        <div className="dialog-body connection-direct-form">
          <label className="dialog-field dialog-field--wide"><span>连接名称</span><input value={form.name} onChange={(event) => update('name', event.target.value)} placeholder={`${form.host || 'InfluxDB'}:${form.port || '8086'}`} /></label>
          <label className="dialog-field"><span>主机</span><input value={form.host} onChange={(event) => update('host', event.target.value)} placeholder="192.168.2.6" autoFocus /></label>
          <label className="dialog-field"><span>端口</span><input value={form.port} onChange={(event) => update('port', event.target.value)} inputMode="numeric" placeholder="8086" /></label>
          <label className="dialog-field dialog-field--wide"><span>数据库</span><input value={form.database} onChange={(event) => update('database', event.target.value)} placeholder="data_engine" /></label>
          <label className="dialog-field"><span>用户名</span><input value={form.username} onChange={(event) => update('username', event.target.value)} autoComplete="username" /></label>
          <label className="dialog-field"><span>密码</span><div className="password-shell"><input type={showPassword ? 'text' : 'password'} value={form.password} onChange={(event) => update('password', event.target.value)} placeholder={editingProfile ? '留空则保留原密码' : '无认证时留空'} autoComplete="new-password" /><button type="button" onClick={() => setShowPassword((value) => !value)} aria-label={showPassword ? '隐藏密码' : '显示密码'}>{showPassword ? <EyeOff size={14} /> : <Eye size={14} />}</button></div></label>
        </div>
        <footer className="dialog-footer connection-direct-footer">
          <div>{message && <span className="test-error" role="alert">{message}</span>}</div>
          <button className="secondary-button" onClick={close} disabled={busy}>取消</button>
          <button className="primary-button" onClick={() => void connect()} disabled={busy}>{editingProfile ? '保存并重新连接' : '连接'}</button>
        </footer>
      </section>
    </div>
  )
}
