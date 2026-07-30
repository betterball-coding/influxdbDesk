import {
  AlertTriangle,
  BadgeCheck,
  CircleX,
  Clock3,
  LoaderCircle,
  LockKeyhole,
  ShieldAlert,
  X,
} from 'lucide-react'
import { useEffect, useState } from 'react'
import { useWorkbenchStore } from '../store'
import type { MutationOperationResult } from '../types'
import { IconButton } from './IconButton'

interface OperationPresentation {
  tone: 'success' | 'warning' | 'danger'
  title: string
  detail: string
}

function operationPresentation(result: MutationOperationResult): OperationPresentation {
  switch (result.state) {
    case 'SUCCEEDED':
      return { tone: 'success', title: '执行成功', detail: '服务端已确认本次变更。' }
    case 'REJECTED':
    case 'REJECTED_NOT_SENT':
      return { tone: 'danger', title: '变更被拒绝', detail: '本次变更没有获得成功确认。' }
    case 'OUTCOME_UNKNOWN':
      return { tone: 'warning', title: '结果未知', detail: '请求可能已经送达，请先核对服务端状态，禁止直接重试。' }
    case 'CANCELED_NOT_SENT':
      return { tone: 'warning', title: '发送前已取消', detail: '本次变更未发往服务端。' }
    case 'FAILED_INTERNAL_NOT_SENT':
    case 'INTERRUPTED_NOT_SENT':
      return { tone: 'danger', title: '发送前失败', detail: '本次变更未发往服务端。' }
    default:
      return { tone: 'danger', title: '变更未完成', detail: '请根据安全错误码检查连接和保护状态。' }
  }
}

function formatExpiry(value: string | undefined): string {
  if (!value) return '不可执行'
  const timestamp = Date.parse(value)
  if (!Number.isFinite(timestamp)) return '授权时间无效'
  return new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(timestamp)
}

export function MutationPreviewDialog() {
  const open = useWorkbenchStore((state) => state.mutationDialogOpen)
  const phase = useWorkbenchStore((state) => state.mutationPhase)
  const preview = useWorkbenchStore((state) => state.mutationPreview)
  const result = useWorkbenchStore((state) => state.mutationResult)
  const safeMessage = useWorkbenchStore((state) => state.mutationSafeMessage)
  const executeMutation = useWorkbenchStore((state) => state.executeMutation)
  const close = useWorkbenchStore((state) => state.closeMutationDialog)
  const [confirmation, setConfirmation] = useState('')

  useEffect(() => {
    setConfirmation('')
  }, [preview?.token, open])

  if (!open) return null

  const confirmationMatches = !preview?.confirmationRequired
    || Boolean(preview.target && confirmation === preview.target)
  const canExecute = phase === 'preview' && Boolean(preview?.executable && preview.token && confirmationMatches)
  const presentation = result ? operationPresentation(result) : undefined

  const closeDialog = () => {
    setConfirmation('')
    close()
  }

  return (
    <div className="modal-layer" role="presentation">
      <button className="modal-scrim" aria-label="关闭变更预览" onClick={closeDialog} />
      <section className="mutation-dialog" role="dialog" aria-modal="true" aria-labelledby="mutation-dialog-title">
        <header className="dialog-header">
          <div className="dialog-icon mutation-dialog__icon"><ShieldAlert size={19} /></div>
          <div><span className="eyebrow">MUTATION</span><h2 id="mutation-dialog-title">变更预览</h2></div>
          <IconButton label="关闭" onClick={closeDialog}><X size={18} /></IconButton>
        </header>

        <div className="mutation-dialog__body">
          {phase === 'previewing' && (
            <div className="mutation-dialog__pending" role="status">
              <LoaderCircle className="spin" size={21} />
              <strong>正在生成后端预览</strong>
            </div>
          )}

          {preview && (phase === 'preview' || phase === 'executing') && (
            <>
              <dl className="mutation-summary">
                <div><dt>操作</dt><dd>{preview.operationKind}</dd></div>
                <div><dt>目标</dt><dd>{preview.target ?? '未指定'}</dd></div>
                <div><dt>授权到期</dt><dd><Clock3 size={12} />{formatExpiry(preview.expiresAt)}</dd></div>
              </dl>
              <div className="mutation-query">
                <span>最终 InfluxQL</span>
                <pre>{preview.canonicalQuery}</pre>
              </div>

              {!preview.executable && (
                <div className="mutation-notice mutation-notice--locked" role="status">
                  <LockKeyhole size={18} />
                  <div><strong>连接仍处于保护锁定状态</strong><span>当前预览不可执行。</span></div>
                </div>
              )}

              {phase === 'preview' && preview.executable && preview.confirmationRequired && (
                <label className="mutation-confirmation" htmlFor="mutation-confirmation-input">
                  <span>输入目标名称以确认</span>
                  <code>{preview.target ?? '目标不可用'}</code>
                  <input
                    id="mutation-confirmation-input"
                    aria-label="输入目标名称以确认"
                    value={confirmation}
                    onChange={(event) => setConfirmation(event.target.value)}
                    autoComplete="off"
                    spellCheck={false}
                  />
                </label>
              )}

              {phase === 'executing' && (
                <div className="mutation-notice" role="status">
                  <LoaderCircle className="spin" size={18} />
                  <div><strong>正在派发变更</strong><span>等待后端提交确定终态。</span></div>
                </div>
              )}
            </>
          )}

          {phase === 'complete' && result && presentation && (
            <div className={`mutation-outcome mutation-outcome--${presentation.tone}`} role="status">
              {presentation.tone === 'success' ? <BadgeCheck size={24} /> : presentation.tone === 'warning' ? <AlertTriangle size={24} /> : <CircleX size={24} />}
              <div>
                <strong>{presentation.title}</strong>
                <span>{presentation.detail}</span>
                {result.publicSafeMessage && <p>{result.publicSafeMessage}</p>}
                {result.publicErrorCode && <code>{result.publicErrorCode}</code>}
              </div>
            </div>
          )}

          {phase === 'error' && (
            <div className="mutation-outcome mutation-outcome--danger" role="alert">
              <CircleX size={24} />
              <div><strong>操作未继续</strong><span>{safeMessage ?? '变更服务暂时不可用。'}</span></div>
            </div>
          )}
        </div>

        <footer className="mutation-dialog__footer">
          <button className="secondary-button" onClick={closeDialog}>关闭</button>
          {phase === 'preview' && preview?.executable && (
            <button
              className="mutation-execute-button"
              disabled={!canExecute}
              onClick={() => void executeMutation(confirmation)}
            >
              <ShieldAlert size={14} /> 执行变更
            </button>
          )}
        </footer>
      </section>
    </div>
  )
}
