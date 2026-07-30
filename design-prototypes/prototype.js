(function () {
  'use strict'

  var root = document.getElementById('prototype-root')
  if (!root) return

  var concept = document.documentElement.dataset.concept || 'precision'

  var state = {
    view: 'query',
    assistant: true,
    protection: true,
    executed: true,
    selectedJob: 'incident',
    density: 'comfortable',
    selectedConnection: 'prod-east',
  }

  function icon(name, size) {
    return '<i data-lucide="' + name + '" style="width:' + (size || 17) + 'px;height:' + (size || 17) + 'px"></i>'
  }

  function refreshIcons() {
    if (window.lucide) window.lucide.createIcons({ attrs: { 'stroke-width': 1.8 } })
  }

  function navButton(view, name, label) {
    return [
      '<button class="nav-button ', state.view === view ? 'is-active' : '', '" data-view="', view,
      '" title="', label, '">', icon(name, 19), '<span>', label, '</span></button>',
    ].join('')
  }

  function shell() {
    root.innerHTML = [
      '<div class="prototype prototype--', concept, '" data-density="', state.density, '">',
        '<nav class="global-nav" aria-label="主导航">',
          '<button class="brand" data-view="query" title="InfluxDesk">',
            '<span class="brand-icon">', icon('database', 22), '</span>',
            '<span class="brand-copy"><strong>InfluxDesk</strong><small>InfluxDB 1.x</small></span>',
          '</button>',
          '<div class="nav-primary">',
            navButton('query', 'square-terminal', '查询'),
            navButton('connections', 'server-cog', '连接'),
            navButton('transfers', 'arrow-right-left', '传输'),
          '</div>',
          '<div class="nav-secondary">',
            navButton('settings', 'settings-2', '设置'),
            '<button class="nav-button" data-action="open-help" title="帮助">', icon('circle-help', 18), '<span>帮助</span></button>',
          '</div>',
        '</nav>',
        '<section class="main-shell">',
          '<header class="topbar">',
            '<button class="connection-switch" data-view="connections">',
              '<span class="live-dot"></span>',
              '<span><strong>Production East</strong><small>telemetry</small></span>',
              icon('chevron-down', 15),
            '</button>',
            '<div class="connection-address"><span>https://influx.example.net:8086</span><code>v1.12.4</code></div>',
            '<div class="topbar-actions">',
              '<button class="quiet-button protection-button ', state.protection ? '' : 'is-unlocked',
                '" data-action="toggle-protection">',
                icon(state.protection ? 'lock-keyhole' : 'lock-keyhole-open', 15),
                '<span>', state.protection ? '保护模式' : '已解锁 09:42', '</span>',
              '</button>',
              '<button class="icon-button" data-action="open-drawer" title="后台任务" aria-label="后台任务">',
                icon('panel-right-open', 17), '<span class="task-badge">1</span>',
              '</button>',
            '</div>',
          '</header>',
          '<main class="view-root" id="view-root"></main>',
          '<footer class="statusbar">',
            '<div><span class="ready-dot"></span><span>就绪</span><span>Mock bridge</span></div>',
            '<div>',
              '<button data-action="open-drawer">', icon('cloud', 13), '<span>2 个后台任务</span></button>',
              '<span>', icon('hard-drive', 13), ' 缓存 0.8 MiB</span>',
              '<span>', icon(state.protection ? 'lock' : 'unlock', 13), state.protection ? ' 写入受保护' : ' 临时解锁', '</span>',
            '</div>',
          '</footer>',
        '</section>',
        '<div id="overlay-root"></div>',
        '<div class="toast" id="toast" role="status"></div>',
      '</div>',
    ].join('')
    renderView()
  }

  function schemaPanel() {
    return [
      '<aside class="schema-panel">',
        '<header class="panel-heading">',
          '<div><span>SCHEMA</span><h2>资源浏览器</h2></div>',
          '<div><button class="icon-button" title="新建查询" aria-label="新建查询">', icon('plus', 15), '</button>',
          '<button class="icon-button" title="刷新 Schema" aria-label="刷新 Schema">', icon('refresh-cw', 15), '</button></div>',
        '</header>',
        '<label class="search-field">', icon('search', 14), '<input aria-label="筛选数据库对象" placeholder="筛选数据库对象" /></label>',
        '<div class="schema-tree">',
          '<div class="tree-connection"><span class="live-dot"></span><strong>Production East</strong><small>在线</small></div>',
          '<button class="tree-row tree-row--db is-open">', icon('chevron-down', 14), icon('database', 15), '<strong>telemetry</strong></button>',
          '<div class="tree-branch">',
            '<button class="tree-row">', icon('chevron-right', 14), icon('key-round', 14), '<span>Retention Policies</span><small>3</small></button>',
            '<button class="tree-row is-open">', icon('chevron-down', 14), icon('gauge', 14), '<span>Measurements</span><small>3</small></button>',
            '<div class="tree-leaves">',
              '<button class="tree-row is-selected">', icon('activity', 14), '<span>cpu</span><small>3</small></button>',
              '<button class="tree-row">', icon('activity', 14), '<span>http_requests</span><small>3</small></button>',
              '<button class="tree-row">', icon('activity', 14), '<span>disk</span><small>2</small></button>',
            '</div>',
          '</div>',
          '<button class="tree-row tree-row--db">', icon('chevron-right', 14), icon('database', 15), '<strong>operations</strong></button>',
        '</div>',
        '<footer class="panel-foot"><span>2 databases</span><span>4 measurements</span></footer>',
      '</aside>',
    ].join('')
  }

  function editorLines() {
    return [
      '<div class="editor-code" aria-label="InfluxQL 编辑器">',
        '<div class="line"><b>1</b><code><em>SELECT</em> mean(<q>usage_user</q>) <em>AS</em> <q>usage_user</q>,</code></div>',
        '<div class="line"><b>2</b><code>       sum(<q>requests</q>) <em>AS</em> <q>requests</q></code></div>',
        '<div class="line"><b>3</b><code><em>FROM</em> <q>telemetry</q>.<q>autogen</q>.<q>cpu</q></code></div>',
        '<div class="line"><b>4</b><code><em>WHERE</em> time &gt;= now() - 6h</code></div>',
        '<div class="line"><b>5</b><code>  <em>AND</em> <q>region</q> = <q>sh-east</q></code></div>',
        '<div class="line"><b>6</b><code><em>GROUP BY</em> time(5m), <q>host</q> fill(null)</code></div>',
      '</div>',
    ].join('')
  }

  function resultRows() {
    var hosts = ['api-01', 'api-02', 'api-03', 'api-04', 'api-05', 'api-06']
    var regions = ['sh-east', 'sh-west', 'bj-core']
    var rows = []
    for (var index = 0; index < 11; index += 1) {
      var seconds = String(20 + index * 5).padStart(2, '0')
      rows.push([
        '<tr><td><input type="checkbox" aria-label="选择结果行 ', index, '" /></td>',
        '<td><code>2026-07-29 14:38:', seconds, '.123456788</code></td>',
        '<td>', hosts[index % hosts.length], '</td>',
        '<td>', regions[index % regions.length], '</td>',
        '<td class="numeric">', (41.241 + index * 0.713).toFixed(4), '</td>',
        '<td class="numeric"><code>', String(9007199254740992n + BigInt(index)), '</code></td></tr>',
      ].join(''))
    }
    return rows.join('')
  }

  function resultPane() {
    if (!state.executed) {
      return '<div class="result-loading"><span class="spinner"></span><strong>正在执行查询</strong><small>等待第一个数据分片</small></div>'
    }
    return [
      '<section class="result-pane" aria-label="查询结果">',
        '<header class="result-tabs">',
          '<button class="is-active">', icon('table-2', 14), '数据 <small>180</small></button>',
          '<button>', icon('chart-no-axes-combined', 14), '图表</button>',
          '<button>', icon('info', 14), '消息</button>',
          '<div class="result-actions"><span>184 ms</span><span>419 KiB</span>',
            '<button class="quiet-button">', icon('download', 14), '导出</button></div>',
        '</header>',
        '<div class="table-scroll"><table class="data-table">',
          '<thead><tr><th><input type="checkbox" aria-label="选择全部结果" /></th><th>time <small>timestamp_ns</small></th><th>host <small>string</small></th><th>region <small>string</small></th><th>usage_user <small>float64</small></th><th>requests <small>int64</small></th></tr></thead>',
          '<tbody>', resultRows(), '</tbody>',
        '</table></div>',
      '</section>',
    ].join('')
  }

  function assistantPanel() {
    if (!state.assistant) return ''
    return [
      '<aside class="assistant-panel">',
        '<header class="panel-heading"><div><span>BUILDER</span><h2>查询辅助</h2></div>',
          '<button class="icon-button" data-action="toggle-assistant" title="关闭查询辅助" aria-label="关闭查询辅助">', icon('panel-right-close', 15), '</button></header>',
        '<div class="assistant-status">', icon('circle-check', 14), '<span><strong>已同步</strong><small>telemetry / cpu</small></span></div>',
        '<section class="assistant-section"><label>聚合函数<select><option>mean</option><option>sum</option><option>max</option></select></label>',
          '<label>Field<select><option>usage_user · float</option><option>usage_system · float</option><option>usage_idle · float</option></select></label></section>',
        '<section class="assistant-section"><h3>时间窗口</h3><div class="segmented"><button class="is-active">6 小时</button><button>24 小时</button><button>7 天</button></div>',
          '<label>分组粒度<select><option>5m</option><option>15m</option><option>1h</option></select></label></section>',
        '<section class="assistant-section"><h3>Tag 条件</h3><label>Tag<select><option>region</option><option>host</option><option>service</option></select></label>',
          '<label>值<input value="sh-east" /></label></section>',
        '<footer><button class="primary-button">', icon('wand-sparkles', 14), '应用到查询</button></footer>',
      '</aside>',
    ].join('')
  }

  function queryView() {
    return [
      '<div class="query-layout query-layout--', concept, ' ', state.assistant ? '' : 'assistant-hidden', '">',
        schemaPanel(),
        '<section class="query-workspace">',
          '<header class="query-tabs"><button class="is-active"><span>{ }</span> CPU 使用率 <small>×</small></button><button class="tab-add" aria-label="新建查询">', icon('plus', 16), '</button></header>',
          '<section class="editor-pane">',
            '<header class="editor-toolbar">',
              '<button class="run-button" data-action="run-query">', icon(state.executed ? 'play' : 'square', 14), '<span>', state.executed ? '执行' : '停止', '</span><kbd>Ctrl ↵</kbd></button>',
              '<button class="quiet-button">', icon('list-filter', 14), '格式化</button>',
              '<button class="quiet-button" data-action="open-mutation">', icon('shield-alert', 14), '预览变更</button>',
              '<button class="quiet-button">', icon('save', 14), '保存</button>',
              '<div class="editor-context"><label>数据库<select><option>telemetry</option><option>operations</option></select></label>',
                '<button class="icon-button" data-action="toggle-assistant" title="切换查询辅助" aria-label="切换查询辅助">', icon('panel-right', 16), '</button></div>',
            '</header>',
            editorLines(),
            '<footer class="editor-meta"><span>InfluxQL</span><span>Ln 1, Col 1</span></footer>',
          '</section>',
          resultPane(),
        '</section>',
        assistantPanel(),
      '</div>',
    ].join('')
  }

  function connectionRow(id, name, url, database, account, version, environment, connected) {
    return [
      '<div class="connection-row ', state.selectedConnection === id ? 'is-selected' : '', '" role="button" tabindex="0" data-connection="', id, '">',
        '<span class="connection-identity"><i class="server-mark ', environment === '生产' ? 'is-production' : '', '">', icon('server', 17), '</i>',
          '<span><strong>', name, '</strong><small>', url, '</small></span></span>',
        '<span><strong>', database, '</strong><small>默认数据库</small></span>',
        '<span><strong>', account, '</strong><small>', environment, '</small></span>',
        '<span class="health ', connected ? 'is-live' : '', '">', icon(connected ? 'circle-check' : 'circle-dashed', 14),
          '<span><strong>', connected ? '已连接' : '未连接', '</strong><small>', version || '未检测版本', '</small></span></span>',
        '<span class="row-actions"><button class="icon-button" data-action="edit-connection" title="编辑连接" aria-label="编辑连接">', icon('pencil', 15), '</button>',
          '<button class="icon-button danger" data-action="delete-connection" title="删除连接" aria-label="删除连接">', icon('trash-2', 15), '</button></span>',
      '</div>',
    ].join('')
  }

  function connectionsView() {
    return [
      '<section class="page-view connections-view connections-view--', concept, '">',
        '<header class="page-heading"><div><span>CONNECTIONS</span><h1>连接管理</h1><p>InfluxDB 1.x 数据源</p></div>',
          '<button class="primary-button" data-action="new-connection">', icon('plus', 15), '新建连接</button></header>',
        '<div class="connections-layout">',
          '<section class="connection-list-panel">',
            '<header class="list-heading"><span>名称</span><span>数据库</span><span>账号</span><span>状态</span><span></span></header>',
            '<div class="connection-list">',
              connectionRow('prod-east', 'Production East', 'https://influx.example.net:8086', 'telemetry', 'operator', 'v1.12.4 · 38 ms', '生产', true),
              connectionRow('lab-local', 'Local Lab', 'http://127.0.0.1:8086', 'operations', '无认证', 'v1.8.10', '开发', false),
            '</div>',
          '</section>',
          '<aside class="connection-inspector">',
            '<header><span class="server-mark is-production">', icon('server', 19), '</span><div><span>当前连接</span><h2>Production East</h2></div></header>',
            '<dl><div><dt>服务地址</dt><dd>influx.example.net:8086</dd></div><div><dt>默认数据库</dt><dd>telemetry / autogen</dd></div>',
              '<div><dt>认证方式</dt><dd>Basic · operator</dd></div><div><dt>环境</dt><dd class="danger-text">生产</dd></div></dl>',
            '<section><h3>安全状态</h3><p>', icon('lock-keyhole', 14), '<span><strong>写入保护已锁定</strong><small>查询与逻辑导出可用</small></span></p></section>',
            '<section><h3>能力快照</h3><div class="capability-list"><span>', icon('eye', 13), '读取</span><span>', icon('download', 13), '导出</span><span class="is-muted">', icon('upload', 13), '写入</span></div></section>',
            '<footer><button class="quiet-button">', icon('refresh-cw', 14), '重新测试</button><button class="primary-button" data-action="edit-connection">', icon('pencil', 14), '编辑</button></footer>',
          '</aside>',
        '</div>',
      '</section>',
    ].join('')
  }

  function jobButton(id, kind, title, stateLabel, revision, active) {
    return [
      '<button class="job-row ', active ? 'is-selected' : '', '" data-job="', id, '">',
        '<span class="job-icon">', icon(kind === 'import' ? 'file-up' : 'download', 16), '</span>',
        '<span><strong>', title, '</strong><small>', id, '</small></span>',
        '<span class="job-state ', id === 'incident' ? 'needs-action' : 'is-success', '">', stateLabel, '</span>',
        '<code>rev ', revision, '</code>',
      '</button>',
    ].join('')
  }

  function transfersView() {
    var incident = state.selectedJob === 'incident'
    return [
      '<section class="page-view transfer-view transfer-view--', concept, '">',
        '<header class="page-heading"><div><span>TRANSFERS</span><h1>数据传输</h1><p>逻辑导入、导出与人工决策</p></div>',
          '<div class="heading-actions"><button class="primary-button" data-action="new-import">', icon('file-up', 15), '新建导入</button>',
          '<button class="secondary-button" data-action="new-export">', icon('download', 15), '新建导出</button></div></header>',
        '<div class="protection-banner">', icon('shield-alert', 16), '<span><strong>写入保护已锁定</strong><small>导出可用；启动、继续和重放导入需要先解锁</small></span>',
          '<button class="quiet-button" data-action="toggle-protection">', icon('unlock', 14), '临时解锁</button></div>',
        '<div class="transfer-layout">',
          '<aside class="job-list"><header><span>当前连接</span><strong>2 个保留任务</strong></header>',
            jobButton('complete', 'export', '逻辑导出', 'SUCCEEDED', '12', !incident),
            jobButton('incident', 'import', '数据导入', 'NEEDS DECISION', '7', incident),
          '</aside>',
          '<section class="job-detail">',
            incident ? [
              '<header><div><span>IMPORT JOB</span><h2>import-demo-incident</h2></div><span class="large-state needs-action">NEEDS UNKNOWN DECISION</span></header>',
              '<dl class="meta-strip"><div><dt>State revision</dt><dd>7</dd></div><div><dt>Snapshot revision</dt><dd>7</dd></div><div><dt>更新时间</dt><dd>2026/7/29 11:20:15</dd></div></dl>',
              '<section class="detail-section"><h3>Checkpoint</h3><dl class="definition-grid"><div><dt>Sequence</dt><dd>4</dd></div><div><dt>Logical offset</dt><dd>67108864</dd></div><div><dt>Max points</dt><dd>5000</dd></div><div><dt>Max bytes</dt><dd>5242880</dd></div></dl></section>',
              '<section class="detail-section incident-section"><header>', icon('triangle-alert', 16), '<span><strong>待人工决策 · UNKNOWN</strong><small>Batch batch-demo · offset 67108864 → 68157440</small></span></header>',
                '<div class="decision-grid"><button class="is-active">REPLAY_EXACT</button><button>ASSUME_COMMITTED</button><button disabled>ACCEPT_PARTIAL</button><button>ABORT</button></div>',
                '<div class="segmented decision-after"><button class="is-active">处理后暂停</button><button>处理后继续</button></div>',
                '<footer><button class="secondary-button" data-action="preview-transfer">预览处置</button><button class="primary-button" disabled>提交处置</button></footer>',
              '</section>',
            ].join('') : [
              '<header><div><span>EXPORT JOB</span><h2>export-demo-complete</h2></div><span class="large-state is-success">SUCCEEDED</span></header>',
              '<dl class="meta-strip"><div><dt>Fragments</dt><dd>12 / 12</dd></div><div><dt>Output</dt><dd>2.8 GB</dd></div><div><dt>更新时间</dt><dd>2026/7/29 11:08:04</dd></div></dl>',
              '<section class="detail-section"><h3>导出产物</h3><dl class="definition-grid"><div><dt>格式</dt><dd>LP.GZ</dd></div><div><dt>数据库</dt><dd>telemetry</dd></div><div><dt>Measurement</dt><dd>cpu</dd></div><div><dt>时间范围</dt><dd>24h</dd></div></dl></section>',
              '<section class="detail-section success-section"><header>', icon('circle-check', 17), '<span><strong>导出完成</strong><small>所有分片已校验并提交到目标目录</small></span></header><footer><button class="primary-button">', icon('folder-open', 14), '打开所在目录</button></footer></section>',
            ].join(''),
          '</section>',
        '</div>',
      '</section>',
    ].join('')
  }

  function settingRow(title, description, control) {
    return '<div class="setting-row"><span><strong>' + title + '</strong><small>' + description + '</small></span>' + control + '</div>'
  }

  function settingsView() {
    return [
      '<section class="page-view settings-view settings-view--', concept, '">',
        '<header class="page-heading"><div><span>PREFERENCES</span><h1>设置</h1><p>本机工作台选项</p></div></header>',
        '<div class="settings-layout">',
          '<nav class="settings-nav" aria-label="设置分类"><button class="is-active">', icon('palette', 15), '外观</button><button>', icon('square-terminal', 15), '查询</button>',
            '<button>', icon('database', 15), '结果与缓存</button><button>', icon('shield', 15), '安全</button><button>', icon('bell', 15), '通知</button></nav>',
          '<div class="settings-content">',
            '<section class="settings-section"><header><h2>外观</h2><p>界面主题和信息密度</p></header>',
              settingRow('主题', '用于编辑器、结果与所有工具面板', '<div class="segmented"><button class="is-active">' + icon('sun', 14) + '浅色</button><button>' + icon('moon', 14) + '深色</button></div>'),
              settingRow('信息密度', '调整列表、树和结果表格的行高', '<div class="segmented"><button data-density="comfortable" class="' + (state.density === 'comfortable' ? 'is-active' : '') + '">舒适</button><button data-density="compact" class="' + (state.density === 'compact' ? 'is-active' : '') + '">紧凑</button></div>'),
              settingRow('查询辅助面板', '新查询默认显示可视化构建器', '<label class="switch"><input type="checkbox" checked /><i></i></label>'),
            '</section>',
            '<section class="settings-section"><header><h2>本地数据</h2><p>查询历史和临时结果由当前 Windows 用户保护</p></header>',
              settingRow('保存查询历史', '90 天后自动清理', '<label class="switch"><input type="checkbox" checked /><i></i></label>'),
              settingRow('结果缓存', '32 MiB 后切换到加密临时文件', '<button class="secondary-button">' + icon('trash-2', 14) + '清理缓存</button>'),
            '</section>',
            '<section class="settings-section"><header><h2>安全</h2><p>所有写操作均通过预览与一次性授权执行</p></header>',
              settingRow('默认保护模式', '新建连接时自动锁定写入', '<label class="switch"><input type="checkbox" checked /><i></i></label>'),
            '</section>',
          '</div>',
        '</div>',
      '</section>',
    ].join('')
  }

  function renderView() {
    var viewRoot = document.getElementById('view-root')
    if (!viewRoot) return
    var templates = {
      query: queryView,
      connections: connectionsView,
      transfers: transfersView,
      settings: settingsView,
    }
    viewRoot.innerHTML = templates[state.view]()
    document.querySelector('.prototype').dataset.density = state.density
    refreshIcons()
  }

  function modalFrame(label, body, wide) {
    return [
      '<div class="overlay-layer"><button class="scrim" data-action="close-overlay" aria-label="关闭"></button>',
      '<section class="modal ', wide ? 'modal--wide' : '', '" role="dialog" aria-modal="true" aria-label="', label, '">', body, '</section></div>',
    ].join('')
  }

  function openConnectionDialog(editing) {
    var body = [
      '<header class="modal-header"><span class="modal-icon">', icon('server', 18), '</span><div><span>INFLUXDB 1.X</span><h2>', editing ? '编辑连接' : '新建连接', '</h2></div>',
        '<button class="icon-button" data-action="close-overlay" aria-label="关闭">', icon('x', 17), '</button></header>',
      '<div class="modal-body form-grid">',
        '<label class="wide"><span>连接名称</span><input value="', editing ? 'Production East' : '', '" placeholder="生产集群" /></label>',
        '<label><span>主机</span><input value="', editing ? 'influx.example.net' : '', '" placeholder="192.168.2.6" /></label>',
        '<label><span>端口</span><input value="8086" /></label>',
        '<label class="wide"><span>数据库</span><input value="', editing ? 'telemetry' : '', '" placeholder="data_engine" /></label>',
        '<label><span>用户名</span><input value="', editing ? 'operator' : '', '" /></label>',
        '<label><span>密码</span><div class="password-field"><input type="password" placeholder="', editing ? '留空则保留原密码' : '无认证时留空', '" /><button aria-label="显示密码">', icon('eye', 14), '</button></div></label>',
      '</div>',
      '<footer class="modal-footer"><span></span><button class="secondary-button" data-action="close-overlay">取消</button>',
        '<button class="primary-button" data-action="save-connection">', editing ? '保存并重新连接' : '连接', '</button></footer>',
    ].join('')
    document.getElementById('overlay-root').innerHTML = modalFrame(editing ? '编辑连接' : '新建连接', body)
    refreshIcons()
  }

  function openMutationDialog() {
    var body = [
      '<header class="modal-header"><span class="modal-icon warning">', icon('shield-alert', 18), '</span><div><span>WRITE PREVIEW</span><h2>变更预览</h2></div>',
        '<button class="icon-button" data-action="close-overlay" aria-label="关闭">', icon('x', 17), '</button></header>',
      '<div class="modal-body mutation-body">',
        '<dl class="mutation-summary"><div><dt>连接</dt><dd>Production East</dd></div><div><dt>目标</dt><dd>telemetry</dd></div><div><dt>操作</dt><dd>DROP DATABASE</dd></div></dl>',
        '<section><h3>将要执行的语句</h3><pre>DROP DATABASE \"telemetry\"</pre></section>',
        '<div class="mutation-notice ', state.protection ? 'is-locked' : '', '">', icon(state.protection ? 'lock-keyhole' : 'triangle-alert', 18),
          '<span><strong>', state.protection ? '连接仍处于保护锁定状态' : '写入保护已临时解锁', '</strong><small>',
          state.protection ? '当前只能审阅，不会签发执行授权。' : '输入目标名称后签发一次性授权。', '</small></span></div>',
        state.protection ? '' : '<label class="confirmation-field"><span>输入目标名称以确认</span><input placeholder="telemetry" /></label>',
      '</div>',
      '<footer class="modal-footer"><span></span><button class="secondary-button" data-action="close-overlay">关闭</button>',
        state.protection ? '' : '<button class="danger-button" data-action="execute-mutation">执行变更</button>', '</footer>',
    ].join('')
    document.getElementById('overlay-root').innerHTML = modalFrame('变更预览', body, true)
    refreshIcons()
  }

  function openTaskDrawer() {
    var body = [
      '<header class="drawer-header"><div><span>BACKGROUND TASKS</span><h2>后台任务</h2></div><button class="icon-button" data-action="close-overlay" aria-label="关闭">', icon('x', 17), '</button></header>',
      '<div class="drawer-body"><button class="drawer-task"><span class="spinner"></span><span><strong>metrics-july.lp.gz</strong><small>telemetry / autogen</small></span><b>64%</b><i style="--progress:64%"></i></button>',
        '<button class="drawer-task"><span class="success-dot">', icon('check', 13), '</span><span><strong>cpu · 过去 24 小时</strong><small>逻辑导出 · LP.GZ</small></span><b>完成</b><i style="--progress:100%"></i></button></div>',
      '<footer class="drawer-footer"><button class="primary-button" data-view="transfers">打开传输工作台</button></footer>',
    ].join('')
    document.getElementById('overlay-root').innerHTML = '<div class="overlay-layer drawer-layer"><button class="scrim" data-action="close-overlay" aria-label="关闭"></button><aside class="task-drawer" aria-label="后台任务">' + body + '</aside></div>'
    refreshIcons()
  }

  function openDeleteDialog() {
    var body = [
      '<header class="modal-header"><span class="modal-icon danger">', icon('trash-2', 18), '</span><div><span>CONNECTION</span><h2>删除连接</h2></div>',
        '<button class="icon-button" data-action="close-overlay" aria-label="关闭">', icon('x', 17), '</button></header>',
      '<div class="modal-body delete-body"><strong>Production East</strong><code>https://influx.example.net:8086</code><p>只删除本机配置，不会修改 InfluxDB 服务器。</p></div>',
      '<footer class="modal-footer"><span></span><button class="secondary-button" data-action="close-overlay">取消</button><button class="danger-button" data-action="confirm-delete">删除</button></footer>',
    ].join('')
    document.getElementById('overlay-root').innerHTML = modalFrame('删除连接', body)
    refreshIcons()
  }

  function closeOverlay() {
    var overlay = document.getElementById('overlay-root')
    if (overlay) overlay.innerHTML = ''
  }

  function toast(message) {
    var element = document.getElementById('toast')
    if (!element) return
    element.textContent = message
    element.classList.add('is-visible')
    window.setTimeout(function () { element.classList.remove('is-visible') }, 1800)
  }

  root.addEventListener('click', function (event) {
    var target = event.target.closest('button, [data-view], [data-connection], [data-job], [data-density]')
    if (!target) return

    if (target.dataset.view) {
      state.view = target.dataset.view
      closeOverlay()
      shell()
      return
    }
    if (target.dataset.connection) {
      state.selectedConnection = target.dataset.connection
      renderView()
      return
    }
    if (target.dataset.job) {
      state.selectedJob = target.dataset.job
      renderView()
      return
    }
    if (target.dataset.density) {
      state.density = target.dataset.density
      shell()
      return
    }

    var action = target.dataset.action
    if (!action) return
    if (action === 'toggle-protection') {
      state.protection = !state.protection
      shell()
      toast(state.protection ? '写入保护已锁定' : '写入保护临时解锁')
    } else if (action === 'toggle-assistant') {
      state.assistant = !state.assistant
      renderView()
    } else if (action === 'run-query') {
      state.executed = false
      renderView()
      window.setTimeout(function () { state.executed = true; renderView(); toast('查询完成 · 180 行') }, 620)
    } else if (action === 'new-connection') {
      openConnectionDialog(false)
    } else if (action === 'edit-connection') {
      event.stopPropagation()
      openConnectionDialog(true)
    } else if (action === 'save-connection') {
      closeOverlay()
      toast('连接已保存并重新连接')
    } else if (action === 'delete-connection') {
      event.stopPropagation()
      openDeleteDialog()
    } else if (action === 'confirm-delete') {
      closeOverlay()
      toast('已删除本机连接配置')
    } else if (action === 'open-mutation') {
      openMutationDialog()
    } else if (action === 'execute-mutation') {
      closeOverlay()
      toast('原型：变更授权流程已完成')
    } else if (action === 'open-drawer') {
      openTaskDrawer()
    } else if (action === 'close-overlay') {
      closeOverlay()
    } else if (action === 'new-import') {
      toast('已打开新建导入流程')
    } else if (action === 'new-export') {
      toast('已打开新建导出流程')
    } else if (action === 'preview-transfer') {
      toast('处置预览已生成，等待确认')
    } else if (action === 'open-help') {
      toast('InfluxDesk 帮助中心')
    }
  })

  shell()
})()
