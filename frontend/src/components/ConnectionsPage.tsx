import {
  CheckCircle2,
  Database,
  Download,
  Eye,
  LockKeyhole,
  Pencil,
  Plus,
  RefreshCw,
  Server,
  Trash2,
  Upload,
  UserRound,
  X,
} from 'lucide-react'
import { useState } from 'react'
import { bridge } from '../bridge'
import { useWorkbenchStore } from '../store'
import type { ConnectionProfile } from '../types'
import { IconButton } from './IconButton'

export function ConnectionsPage() {
  const connections = useWorkbenchStore((state) => state.connections)
  const activeConnectionId = useWorkbenchStore((state) => state.activeConnectionId)
  const setActiveConnection = useWorkbenchStore((state) => state.setActiveConnection)
  const setConnectionDialogOpen = useWorkbenchStore((state) => state.setConnectionDialogOpen)
  const deleteConnection = useWorkbenchStore((state) => state.deleteConnection)
  const [testing, setTesting] = useState<string>()
  const [testResult, setTestResult] = useState<Record<string, string>>({})
  const [deleteTarget, setDeleteTarget] = useState<ConnectionProfile>()
  const [deleting, setDeleting] = useState(false)
  const [deleteError, setDeleteError] = useState('')

  const test = async (id: string) => {
    setTesting(id)
    try {
      const result = await bridge.testConnection(id)
      setTestResult((current) => ({ ...current, [id]: `${result.latencyMs} ms · InfluxDB ${result.version}` }))
    } catch {
      setTestResult((current) => ({ ...current, [id]: '测试失败' }))
    } finally {
      setTesting(undefined)
    }
  }

  const confirmDelete = async () => {
    if (!deleteTarget) return
    setDeleting(true)
    setDeleteError('')
    try {
      await deleteConnection(deleteTarget.id)
      setDeleteTarget(undefined)
    } catch {
      setDeleteError('删除失败，连接配置和凭据均已保留。')
    } finally {
      setDeleting(false)
    }
  }

  const active = connections.find((connection) => connection.id === activeConnectionId) ?? connections[0]

  return (
    <main className="page-surface connections-page">
      <header className="page-header">
        <div><span className="eyebrow">CONNECTIONS</span><h1>连接管理</h1><p>InfluxDB 1.x 数据源</p></div>
        <button className="primary-button" onClick={() => setConnectionDialogOpen(true)}><Plus size={15} /> 新建连接</button>
      </header>
      <div className="connections-layout">
        <aside className="connection-inspector" aria-label="当前连接详情">
          {active ? (
            <>
              <header><span className={`connection-logo connection-logo--${active.environment}`}><Server size={18} /></span><div><span className="eyebrow">当前连接</span><h2>{active.name}</h2></div></header>
              <dl>
                <div><dt>服务地址</dt><dd>{active.url.replace(/^https?:\/\//, '')}</dd></div>
                <div><dt>默认数据库</dt><dd>{active.defaultDatabase || '自动选择'} / autogen</dd></div>
                <div><dt>认证方式</dt><dd>{active.username ? `Basic · ${active.username}` : '无认证'}</dd></div>
                <div><dt>环境</dt><dd className={active.environment === 'production' ? 'danger-text' : ''}>{active.environment === 'production' ? '生产' : active.environment === 'staging' ? '预发布' : '开发'}</dd></div>
              </dl>
              <section><h3>安全状态</h3><p><LockKeyhole size={14} /><span><strong>{active.protectionMode === 'PermanentReadOnly' ? '永久只读' : '写入保护已锁定'}</strong><small>查询与逻辑导出可用</small></span></p></section>
              <section><h3>能力快照</h3><div className="connection-capabilities"><span><Eye size={13} />读取</span><span><Download size={13} />导出</span><span className="is-muted"><Upload size={13} />写入</span></div></section>
              <footer><button className="secondary-button" onClick={() => void test(active.id)} disabled={testing === active.id}><RefreshCw size={14} className={testing === active.id ? 'spin' : ''} />重新测试</button><button className="primary-button" onClick={() => setConnectionDialogOpen(true, active.id)}><Pencil size={14} />编辑</button></footer>
            </>
          ) : <div className="connection-list__empty">新建连接后显示详情</div>}
        </aside>
        <section className="connection-table-shell" aria-label="连接列表">
          <div className="connection-table-heading">
            <span>名称</span><span>数据库</span><span>账号</span><span>状态</span><span />
          </div>
          <div className="connection-list">
            {connections.length === 0 && <div className="connection-list__empty">暂无连接</div>}
            {connections.map((connection) => (
              <article className={`connection-row ${connection.id === activeConnectionId ? 'is-selected' : ''}`} key={connection.id}>
                <button className="connection-row__identity" onClick={() => setActiveConnection(connection.id)}>
                  <span className={`connection-logo connection-logo--${connection.environment}`}><Server size={18} /></span>
                  <span><strong>{connection.name}</strong><small>{connection.url}</small></span>
                </button>
                <span className="connection-database" title={connection.defaultDatabase || '自动选择'}>
                  <Database size={13} /> {connection.defaultDatabase || '自动选择'}
                </span>
                <span className="connection-account" title={connection.username || '无认证'}>
                  <UserRound size={13} /> {connection.username || '无认证'}
                </span>
                <span className={`connection-health${testResult[connection.id] === '测试失败' ? ' connection-health--error' : ''}`}>
                  {testResult[connection.id]
                    ? <>{testResult[connection.id] !== '测试失败' && <CheckCircle2 size={14} />}{testResult[connection.id]}</>
                    : connection.state === 'connected'
                      ? <><CheckCircle2 size={14} />已连接{connection.version ? ` · v${connection.version}` : ''}</>
                      : '未连接'}
                </span>
                <span className="connection-row__actions">
                  <IconButton label={`编辑 ${connection.name}`} onClick={() => setConnectionDialogOpen(true, connection.id)}><Pencil size={14} /></IconButton>
                  <IconButton label={`删除 ${connection.name}`} className="icon-button--danger" onClick={() => { setDeleteError(''); setDeleteTarget(connection) }}><Trash2 size={14} /></IconButton>
                </span>
              </article>
            ))}
          </div>
        </section>
      </div>

      {deleteTarget && (
        <div className="modal-layer" role="presentation">
          <button className="modal-scrim" aria-label="取消删除连接" onClick={() => !deleting && setDeleteTarget(undefined)} />
          <section className="connection-delete-dialog" role="dialog" aria-modal="true" aria-labelledby="delete-connection-title">
            <header className="dialog-header">
              <div className="dialog-icon dialog-icon--danger"><Trash2 size={18} /></div>
              <div><span className="eyebrow">DELETE CONNECTION</span><h2 id="delete-connection-title">删除连接</h2></div>
              <IconButton label="关闭" onClick={() => setDeleteTarget(undefined)} disabled={deleting}><X size={18} /></IconButton>
            </header>
            <div className="connection-delete-body">
              <strong>{deleteTarget.name}</strong>
              <span>{deleteTarget.url}</span>
              {deleteError && <p role="alert">{deleteError}</p>}
            </div>
            <footer className="connection-delete-footer">
              <button className="secondary-button" onClick={() => setDeleteTarget(undefined)} disabled={deleting}>取消</button>
              <button className="danger-button" onClick={() => void confirmDelete()} disabled={deleting}><Trash2 size={14} /> 删除</button>
            </footer>
          </section>
        </div>
      )}
    </main>
  )
}
