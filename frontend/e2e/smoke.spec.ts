import { readFile } from 'node:fs/promises'
import { expect, test, type Download, type Page } from '@playwright/test'
import { mockSchema } from '../src/mockData'

const exactInteger = '9007199254740993'
const rawTimestamp = '1700000000123456788'
const mutation = 'DROP DATABASE "telemetry"'

async function expectNoDocumentOverflow(page: Page) {
  const dimensions = await page.evaluate(() => ({
    innerWidth: window.innerWidth,
    innerHeight: window.innerHeight,
    documentWidth: document.documentElement.scrollWidth,
    documentHeight: document.documentElement.scrollHeight,
    bodyWidth: document.body.scrollWidth,
    bodyHeight: document.body.scrollHeight,
  }))
  expect(dimensions.documentWidth).toBeLessThanOrEqual(dimensions.innerWidth)
  expect(dimensions.bodyWidth).toBeLessThanOrEqual(dimensions.innerWidth)
  expect(dimensions.documentHeight).toBeLessThanOrEqual(dimensions.innerHeight)
  expect(dimensions.bodyHeight).toBeLessThanOrEqual(dimensions.innerHeight)
}

async function downloadText(download: Download): Promise<string> {
  const path = await download.path()
  expect(path).not.toBeNull()
  return readFile(path!, 'utf8')
}

test('query precision and protected mutation workflow remain safe', async ({ page }) => {
  const consoleIssues: string[] = []
  page.on('console', (message) => {
    if (message.type() === 'error' || message.type() === 'warning') {
      consoleIssues.push(`${message.type()}: ${message.text()}`)
    }
  })
  page.on('pageerror', (error) => consoleIssues.push(`pageerror: ${error.message}`))

  await page.goto('/', { waitUntil: 'networkidle' })
  await expect(page).toHaveTitle('InfluxDesk')
  await expect(page.locator('#root')).toContainText('CPU 使用率')
  await expect(page.getByRole('button', { name: '后台任务' }).locator('.icon-button__badge')).toHaveText('1')
  await page.getByRole('button', { name: '收起资源浏览器' }).click()
  await expect(page.getByRole('complementary', { name: '已收起的资源浏览器' })).toBeVisible()
  await page.getByRole('button', { name: '展开资源浏览器' }).click()
  await expect(page.getByRole('button', { name: '当前连接' })).toBeVisible()
  await expect(page.locator('vite-error-overlay, #webpack-dev-server-client-overlay')).toHaveCount(0)
  await expectNoDocumentOverflow(page)

  await page.locator('.command-button--primary').click()
  await expect(page.getByText(exactInteger, { exact: true }).first()).toBeVisible()
  const readableTimestamp = await page.evaluate(async (timestamp) => {
    const { formatTimestampNs } = await import('/src/timestampFormat.ts')
    return formatTimestampNs(timestamp)
  }, rawTimestamp)
  await expect(page.getByText(readableTimestamp, { exact: true }).first()).toBeVisible()
  await expect(page.getByText(rawTimestamp, { exact: true })).toHaveCount(0)

  const timeHeader = page.locator('.result-grid-header-cell').first()
  const timeResizer = page.getByRole('separator', { name: '调整 time 列宽' })
  const widthBefore = (await timeHeader.boundingBox())!.width
  const resizerBox = (await timeResizer.boundingBox())!
  await page.mouse.move(resizerBox.x + resizerBox.width / 2, resizerBox.y + resizerBox.height / 2)
  await page.mouse.down()
  await page.mouse.move(resizerBox.x + resizerBox.width / 2 + 60, resizerBox.y + resizerBox.height / 2)
  await page.mouse.up()
  const widthAfter = (await timeHeader.boundingBox())!.width
  expect(widthAfter - widthBefore).toBeGreaterThan(50)

  const editor = page.locator('.monaco-editor').first()
  await editor.click({ position: { x: 220, y: 60 } })
  const unformattedQuery = `select * from "cpu" where time > now() - 5m and "region" = 'sh-east'`
  const formattedQuery = `SELECT *\nFROM "cpu"\nWHERE time > now() - 5m\n  AND "region" = 'sh-east'`
  await page.keyboard.press('Control+A')
  await page.keyboard.insertText(unformattedQuery)
  await page.getByRole('button', { name: '格式化' }).click()
  await expect.poll(() => page.evaluate(async () => {
    const { useWorkbenchStore } = await import('/src/store.ts')
    const state = useWorkbenchStore.getState()
    return state.tabs.find((tab) => tab.id === state.activeTabId)?.query
  })).toBe(formattedQuery)

  await expect(page.getByRole('button', { name: '撤销编辑' })).toBeEnabled()
  await page.getByRole('button', { name: '撤销编辑' }).click()
  await expect.poll(() => page.evaluate(async () => {
    const { useWorkbenchStore } = await import('/src/store.ts')
    const state = useWorkbenchStore.getState()
    return state.tabs.find((tab) => tab.id === state.activeTabId)?.query
  })).toBe(unformattedQuery)

  await expect(page.getByRole('button', { name: '重做编辑' })).toBeEnabled()
  await page.getByRole('button', { name: '重做编辑' }).click()
  await expect.poll(() => page.evaluate(async () => {
    const { useWorkbenchStore } = await import('/src/store.ts')
    const state = useWorkbenchStore.getState()
    return state.tabs.find((tab) => tab.id === state.activeTabId)?.query
  })).toBe(formattedQuery)

  await editor.click({ position: { x: 220, y: 60 } })
  await page.keyboard.press('Control+A')
  await page.keyboard.insertText(mutation)
  await expect(page.locator('.view-lines')).toContainText('DROP DATABASE')

  await page.getByRole('button', { name: '预览变更' }).click()
  let dialog = page.getByRole('dialog', { name: '变更预览' })
  await expect(dialog.getByText('连接仍处于保护锁定状态')).toBeVisible()
  await expect(dialog.getByRole('button', { name: '执行变更' })).toHaveCount(0)
  await expectNoDocumentOverflow(page)
  await dialog.getByRole('button', { name: '关闭', exact: true }).last().click()
  await expect(dialog).toHaveCount(0)

  const clearedState = await page.evaluate(async () => {
    const { useWorkbenchStore } = await import('/src/store.ts')
    const state = useWorkbenchStore.getState()
    return { open: state.mutationDialogOpen, hasPreview: Boolean(state.mutationPreview) }
  })
  expect(clearedState).toEqual({ open: false, hasPreview: false })

  await page.getByRole('button', { name: '解锁保护模式' }).click()
  await page.getByRole('button', { name: '预览变更' }).click()
  dialog = page.getByRole('dialog', { name: '变更预览' })
  let confirmation = dialog.getByLabel('输入目标名称以确认')
  await expect(confirmation).toBeVisible()
  let execute = dialog.getByRole('button', { name: '执行变更' })
  await expect(execute).toBeDisabled()
  await confirmation.fill('Telemetry')
  await expect(execute).toBeDisabled()
  await confirmation.fill('telemetry')
  await expect(execute).toBeEnabled()

  await dialog.getByRole('button', { name: '关闭', exact: true }).last().click()
  await page.getByRole('button', { name: '预览变更' }).click()
  dialog = page.getByRole('dialog', { name: '变更预览' })
  confirmation = dialog.getByLabel('输入目标名称以确认')
  await expect(confirmation).toHaveValue('')
  execute = dialog.getByRole('button', { name: '执行变更' })
  await expect(execute).toBeDisabled()

  await confirmation.fill('telemetry')
  await execute.click()
  await expect(dialog.getByText('执行成功')).toBeVisible()
  await expectNoDocumentOverflow(page)
  expect(consoleIssues).toEqual([])
})

test('query result time zone switches between UTC+8 and UTC and persists', async ({ page }) => {
  await page.goto('/', { waitUntil: 'networkidle' })
  await page.locator('.command-button--primary').click()

  const timestampLabels = await page.evaluate(async (timestamp) => {
    const { formatTimestampNs } = await import('/src/timestampFormat.ts')
    return {
      east8: formatTimestampNs(timestamp, 'utc+8'),
      utc: formatTimestampNs(timestamp, 'utc'),
    }
  }, rawTimestamp)
  await expect(page.getByText(timestampLabels.east8, { exact: true }).first()).toBeVisible()

  await page.getByRole('button', { name: '设置', exact: true }).click()
  const utcButton = page.getByRole('button', { name: '零时区 (UTC)', exact: true })
  await utcButton.click()
  await expect(utcButton).toHaveAttribute('aria-pressed', 'true')

  await page.getByRole('button', { name: '查询工作台', exact: true }).click()
  await expect(page.getByText(timestampLabels.utc, { exact: true }).first()).toBeVisible()
  await expect(page.getByText(timestampLabels.east8, { exact: true })).toHaveCount(0)

  await page.reload({ waitUntil: 'networkidle' })
  await page.getByRole('button', { name: '设置', exact: true }).click()
  await expect(page.getByRole('button', { name: '零时区 (UTC)', exact: true })).toHaveAttribute('aria-pressed', 'true')
})

test('schema measurement selection binds the exact query and assistant context', async ({ page }) => {
  const telemetry = mockSchema.find((database) => database.name === 'telemetry')
  const cpu = telemetry?.measurements.find((measurement) => measurement.name === 'cpu')
  expect(telemetry).toBeDefined()
  expect(cpu).toBeDefined()

  await page.goto('/', { waitUntil: 'networkidle' })

  const schema = page.locator('.schema-panel')
  const telemetryButton = schema.getByRole('button', { name: 'telemetry', exact: true })
  const measurementsButton = schema.getByRole('button', { name: /Measurements/ })

  await telemetryButton.click()
  await expect(measurementsButton).toBeHidden()
  await telemetryButton.click()
  await expect(measurementsButton).toBeVisible()

  const cpuButton = schema.getByRole('button', { name: /^cpu\b/ })
  await measurementsButton.click()
  await expect(cpuButton).toBeHidden()
  await measurementsButton.click()
  await expect(cpuButton).toBeVisible()
  await cpuButton.click()

  const editor = page.getByRole('region', { name: 'InfluxQL 编辑器' })
  await expect(editor.locator('.view-lines')).toHaveText('SELECT * FROM "cpu" WHERE time > now() - 5m')
  await expect(editor.getByLabel('数据库')).toHaveValue('telemetry')

  await page.getByRole('button', { name: '展开查询辅助器' }).click()
  const assistant = page.locator('.query-assistant')
  await expect(assistant.locator('.builder-state')).toHaveText(/已同步/)
  await expect(assistant.locator('.assistant-source-value')).toHaveAttribute('title', 'telemetry / cpu')
  await expect(assistant.locator('.assistant-source-value strong')).toHaveText('cpu')

  const fieldOptions = assistant.getByLabel('Field').locator('option')
  expect(await fieldOptions.evaluateAll((options) => options.map((option) => ({
    value: (option as HTMLOptionElement).value,
    label: option.textContent,
  })))).toEqual(cpu!.fields.map((field) => ({
    value: field.name,
    label: `${field.name} · ${field.type}`,
  })))
  await expect(assistant.getByLabel('Field')).toHaveValue('usage_user')

  const tagOptions = assistant.getByLabel('Tag').locator('option')
  expect(await tagOptions.evaluateAll((options) => options.map((option) => ({
    value: (option as HTMLOptionElement).value,
    label: option.textContent,
  })))).toEqual([
    { value: '', label: '不使用 Tag' },
    ...cpu!.tags.map((tag) => ({ value: tag, label: tag })),
  ])
  await expect(assistant.getByLabel('Tag', { exact: true })).toHaveValue('region')
  await expect(assistant.getByLabel('Tag value')).toHaveValue('sh-east')
})

test('query result export stays inline and downloads selected or all rows', async ({ page, context }) => {
  const consoleIssues: string[] = []
  page.on('console', (message) => {
    if (message.type() === 'error' || message.type() === 'warning') consoleIssues.push(message.text())
  })
  page.on('pageerror', (error) => consoleIssues.push(error.message))

  await page.goto('/', { waitUntil: 'networkidle' })
  await page.locator('.command-button--primary').click()
  const resultPane = page.getByRole('region', { name: '查询结果' })
  await expect(page.getByRole('checkbox', { name: '选择结果行 0', exact: true })).toBeVisible()

  const firstSelectCell = page.locator('.result-grid-select-cell').nth(0)
  const secondSelectCell = page.locator('.result-grid-select-cell').nth(1)
  const firstSelectBox = (await firstSelectCell.boundingBox())!
  const secondSelectBox = (await secondSelectCell.boundingBox())!
  await page.mouse.move(firstSelectBox.x + firstSelectBox.width / 2, firstSelectBox.y + firstSelectBox.height / 2)
  await page.mouse.down()
  await page.mouse.move(secondSelectBox.x + secondSelectBox.width / 2, secondSelectBox.y + secondSelectBox.height / 2, { steps: 4 })
  await page.mouse.up()
  await expect(page.getByRole('checkbox', { name: '选择结果行 0', exact: true })).toBeChecked()
  await expect(page.getByRole('checkbox', { name: '选择结果行 1', exact: true })).toBeChecked()
  await page.getByRole('button', { name: '导出结果' }).click()

  const selectedItem = page.getByRole('menuitem', { name: /导出所选行/ })
  await expect(selectedItem).toContainText('2 行')
  await expect(page.getByRole('menuitem', { name: /导出全部查询结果/ })).toContainText('180 行')

  const selectedDownloadEvent = page.waitForEvent('download')
  await selectedItem.click()
  const selectedCSV = await downloadText(await selectedDownloadEvent)
  expect(selectedCSV.startsWith('\ufefftime,host,region,usage_user,requests\r\n')).toBe(true)
  expect(selectedCSV.trimEnd().split('\r\n')).toHaveLength(3)
  expect(selectedCSV).toContain('9007199254740992')
  expect(selectedCSV).toContain('9007199254740993')
  expect(selectedCSV).not.toContain(rawTimestamp)
  await expect(resultPane.getByRole('status')).toContainText('已导出 2 行查询结果')

  await page.getByRole('button', { name: '导出结果' }).click()
  const allDownloadEvent = page.waitForEvent('download')
  await page.getByRole('menuitem', { name: /导出全部查询结果/ }).click()
  const allCSV = await downloadText(await allDownloadEvent)
  expect(allCSV.trimEnd().split('\r\n')).toHaveLength(181)
  await expect(resultPane.getByRole('status')).toContainText('已导出 180 行查询结果')

  expect(context.pages()).toHaveLength(1)
  expect(page.url()).toMatch(/127\.0\.0\.1:41739\/$/)
  expect(await page.evaluate(async () => {
    const { useWorkbenchStore } = await import('/src/store.ts')
    return useWorkbenchStore.getState().taskDrawerOpen
  })).toBe(false)
  await expectNoDocumentOverflow(page)
  expect(consoleIssues).toEqual([])
})

test('connection creation is direct and saved connections can be edited or deleted', async ({ page }) => {
  await page.goto('/', { waitUntil: 'networkidle' })

  const connectionSelector = page.getByRole('button', { name: '当前连接' })
  await expect(connectionSelector).toContainText('Production East')
  await connectionSelector.click()
  await page.getByRole('menuitemradio', { name: '选择连接 Local Lab' }).click()
  await expect(connectionSelector).toContainText('Local Lab')
  await expect(page.getByRole('dialog', { name: '编辑连接' })).toHaveCount(0)
  await page.getByRole('button', { name: '编辑连接', exact: true }).click()
  const selectorEditDialog = page.getByRole('dialog', { name: '编辑连接' })
  await expect(selectorEditDialog.getByLabel('连接名称')).toHaveValue('Local Lab')
  await selectorEditDialog.getByRole('button', { name: '关闭' }).click()
  await connectionSelector.click()
  await page.getByRole('menuitemradio', { name: '选择连接 Production East' }).click()
  await expect(connectionSelector).toContainText('Production East')

  await page.getByRole('button', { name: '连接管理' }).click()
  await page.getByRole('button', { name: '新建连接' }).click()

  const createDialog = page.getByRole('dialog', { name: '新建连接' })
  await expect(createDialog.getByText('认证方式')).toHaveCount(0)
  await expect(createDialog.getByText('SSH 隧道')).toHaveCount(0)
  await createDialog.getByLabel('连接名称').fill('Direct test')
  await createDialog.getByLabel('主机').fill('192.168.2.6')
  await createDialog.getByLabel('端口').fill('8086')
  await createDialog.getByLabel('数据库').fill('data_engine')
  await createDialog.getByLabel('用户名').fill('operator')
  await createDialog.locator('input[type="password"]').fill('secret')
  const insecureAuthConsent = createDialog.getByRole('checkbox', { name: /允许通过 HTTP 发送认证信息/ })
  await expect(insecureAuthConsent).toBeVisible()
  await expect(insecureAuthConsent).not.toBeChecked()
  await createDialog.getByRole('button', { name: '连接', exact: true }).click()
  await expect(createDialog.getByRole('alert')).toContainText('必须确认明文传输风险')
  await expect(createDialog).toBeVisible()
  await insecureAuthConsent.check()
  await createDialog.getByRole('button', { name: '连接', exact: true }).click()
  await expect(page.getByRole('region', { name: 'InfluxQL 编辑器' })).toBeVisible()

  await page.getByRole('button', { name: '连接管理' }).click()
  await page.getByRole('button', { name: '编辑 Direct test' }).click()
  const editDialog = page.getByRole('dialog', { name: '编辑连接' })
  await expect(editDialog.getByLabel('数据库')).toHaveValue('data_engine')
  await expect(editDialog.locator('input[type="password"]')).toHaveValue('')
  await expect(editDialog.getByRole('checkbox', { name: /允许通过 HTTP 发送认证信息/ })).toBeChecked()
  await editDialog.getByLabel('连接名称').fill('Direct edited')
  await editDialog.getByRole('button', { name: '保存并重新连接' }).click()

  await page.getByRole('button', { name: '连接管理' }).click()
  await expect(page.locator('.connection-list').getByText('Direct edited')).toBeVisible()
  await page.getByRole('button', { name: '删除 Local Lab' }).click()
  const deleteDialog = page.getByRole('dialog', { name: '删除连接' })
  await expect(deleteDialog.getByText('Local Lab')).toBeVisible()
  await deleteDialog.getByRole('button', { name: '删除', exact: true }).click()
  await expect(page.getByText('Local Lab')).toHaveCount(0)
  await expectNoDocumentOverflow(page)
})

test('transfer panel exposes safe import decisions and bounded forms', async ({ page }) => {
  const consoleIssues: string[] = []
  page.on('console', (message) => {
    if (message.type() === 'error' || message.type() === 'warning') consoleIssues.push(message.text())
  })
  page.on('pageerror', (error) => consoleIssues.push(error.message))

  await page.goto('/', { waitUntil: 'networkidle' })
  await page.getByRole('button', { name: '传输任务' }).click()
  const panel = page.getByRole('complementary', { name: '导入导出任务' })
  await expect(panel.getByText('数据传输')).toBeVisible()
  await expect(panel.getByRole('button', { name: 'REPLAY_EXACT' })).toBeVisible()
  await expect(panel.getByRole('button', { name: 'ASSUME_COMMITTED' })).toBeVisible()
  await expect(panel.getByRole('button', { name: 'ACCEPT_PARTIAL' })).toBeDisabled()
  await expect(panel.getByRole('button', { name: 'ABORT' })).toBeVisible()

  await panel.getByRole('button', { name: '新建导入' }).click()
  await panel.getByLabel('格式').selectOption('CSV')
  await expect(panel.getByText('CSV 显式映射')).toBeVisible()
  await expect(panel.getByLabel('CSV 来源列')).toBeVisible()

  const bounds = await panel.evaluate((element) => ({
    clientWidth: element.clientWidth,
    scrollWidth: element.scrollWidth,
    clientHeight: element.clientHeight,
    scrollHeight: element.scrollHeight,
  }))
  expect(bounds.scrollWidth).toBeLessThanOrEqual(bounds.clientWidth)
  expect(bounds.scrollHeight).toBeLessThanOrEqual(bounds.clientHeight)
  await expectNoDocumentOverflow(page)
  expect(consoleIssues).toEqual([])
})
