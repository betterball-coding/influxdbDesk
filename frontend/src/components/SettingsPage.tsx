import { Bell, Clock3, Database, Moon, Palette, Shield, Sun, Terminal, Trash2 } from 'lucide-react'
import { useWorkbenchStore } from '../store'

export function SettingsPage() {
  const theme = useWorkbenchStore((state) => state.theme)
  const setTheme = useWorkbenchStore((state) => state.setTheme)
  const assistantOpen = useWorkbenchStore((state) => state.assistantOpen)
  const toggleAssistant = useWorkbenchStore((state) => state.toggleAssistant)
  const queryResultTimeZone = useWorkbenchStore((state) => state.queryResultTimeZone)
  const setQueryResultTimeZone = useWorkbenchStore((state) => state.setQueryResultTimeZone)

  return (
    <main className="page-surface settings-page">
      <header className="page-header"><div><span className="eyebrow">PREFERENCES</span><h1>设置</h1><p>本机工作台选项</p></div></header>
      <div className="settings-layout">
        <nav className="settings-nav" aria-label="设置分类">
          <button className="is-active"><Palette size={15} /> 外观</button>
          <button><Terminal size={15} /> 查询</button>
          <button><Database size={15} /> 结果与缓存</button>
          <button><Shield size={15} /> 安全</button>
          <button><Bell size={15} /> 通知</button>
        </nav>
        <div className="settings-content">
          <section className="settings-section">
            <header><h2>外观</h2><p>界面主题和信息密度</p></header>
            <div className="setting-row">
              <div><strong>主题</strong><span>用于编辑器、结果与所有工具面板</span></div>
              <div className="theme-selector">
                <button className={theme === 'light' ? 'is-active' : ''} onClick={() => setTheme('light')}><Sun size={15} /> 浅色</button>
                <button className={theme === 'dark' ? 'is-active' : ''} onClick={() => setTheme('dark')}><Moon size={15} /> 深色</button>
              </div>
            </div>
            <div className="setting-row"><div><strong>信息密度</strong><span>调整列表、树和结果表格的行高</span></div><div className="theme-selector"><button className="is-active">舒适</button><button>紧凑</button></div></div>
            <div className="setting-row"><div><strong>查询辅助面板</strong><span>新查询默认显示可视化构建器</span></div><label className="switch"><input type="checkbox" checked={assistantOpen} onChange={toggleAssistant} /><i /></label></div>
          </section>
          <section className="settings-section">
            <header><h2>查询结果</h2><p>控制查询结果中时间戳的显示方式</p></header>
            <div className="setting-row">
              <div><strong>时间时区</strong><span>只调整查询结果显示，不改变 InfluxDB 中的原始时间戳</span></div>
              <div aria-label="查询结果时间时区" className="theme-selector" role="group">
                <button
                  aria-pressed={queryResultTimeZone === 'utc+8'}
                  className={queryResultTimeZone === 'utc+8' ? 'is-active' : ''}
                  onClick={() => setQueryResultTimeZone('utc+8')}
                  type="button"
                >
                  <Clock3 size={14} /> 东八区 (UTC+8)
                </button>
                <button
                  aria-pressed={queryResultTimeZone === 'utc'}
                  className={queryResultTimeZone === 'utc' ? 'is-active' : ''}
                  onClick={() => setQueryResultTimeZone('utc')}
                  type="button"
                >
                  零时区 (UTC)
                </button>
              </div>
            </div>
          </section>
          <section className="settings-section">
            <header><h2>安全</h2><p>所有写操作均通过预览与一次性授权执行</p></header>
            <div className="setting-row"><div><strong>默认保护模式</strong><span>新建连接时自动锁定写入</span></div><label className="switch"><input type="checkbox" defaultChecked /><i /></label></div>
          </section>
          <section className="settings-section">
            <header><h2>本地数据</h2><p>查询历史和临时结果由当前 Windows 用户保护</p></header>
            <div className="setting-row"><div><strong>保存查询历史</strong><span>90 天后自动清理</span></div><label className="switch"><input type="checkbox" defaultChecked /><i /></label></div>
            <div className="setting-row"><div><strong>结果缓存</strong><span>32 MiB 后切换到加密临时文件</span></div><button className="secondary-button"><Trash2 size={14} /> 清理缓存</button></div>
          </section>
        </div>
      </div>
    </main>
  )
}
