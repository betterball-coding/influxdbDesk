import { Check, ChevronDown, Database, LoaderCircle, PanelRightClose, WandSparkles } from 'lucide-react'
import { useState } from 'react'
import { quoteInfluxIdentifier, quoteInfluxString } from '../influxql'
import { useWorkbenchStore } from '../store'
import type { SchemaMeasurement } from '../types'

const numericFieldTypes = new Set(['float', 'integer', 'unsigned'])
const numericAggregations = ['mean', 'max', 'min', 'sum', 'count'] as const
const countAggregation = ['count'] as const

function MeasurementBuilder({
  database,
  measurement,
}: {
  database: string
  measurement: SchemaMeasurement
}) {
  const updateQuery = useWorkbenchStore((state) => state.updateQuery)
  const defaultField = measurement.fields.find((candidate) => candidate.name === 'usage_user')?.name ?? measurement.fields[0]?.name ?? ''
  const defaultTag = measurement.tags.includes('region') ? 'region' : ''
  const [requestedField, setRequestedField] = useState(defaultField)
  const [requestedAggregation, setRequestedAggregation] = useState('mean')
  const [range, setRange] = useState('6h')
  const [interval, setInterval] = useState('5m')
  const [requestedTag, setRequestedTag] = useState(defaultTag)
  const [tagValue, setTagValue] = useState(defaultTag ? 'sh-east' : '')

  const field = measurement.fields.some((candidate) => candidate.name === requestedField)
    ? requestedField
    : measurement.fields[0]?.name ?? ''
  const fieldType = measurement.fields.find((candidate) => candidate.name === field)?.type
  const aggregations = fieldType && numericFieldTypes.has(fieldType) ? numericAggregations : countAggregation
  const aggregation = aggregations.some((candidate) => candidate === requestedAggregation)
    ? requestedAggregation
    : aggregations[0]
  const tag = measurement.tags.includes(requestedTag) ? requestedTag : ''

  const apply = () => {
    if (!field) return
    const quotedField = quoteInfluxIdentifier(field)
    const filter = tag && tagValue
      ? `\n  AND ${quoteInfluxIdentifier(tag)} = ${quoteInfluxString(tagValue)}`
      : ''
    const groupTag = tag ? `, ${quoteInfluxIdentifier(tag)}` : ''
    updateQuery(
      `SELECT ${aggregation}(${quotedField}) AS ${quotedField}\n` +
      `FROM ${quoteInfluxIdentifier(measurement.name)}\n` +
      `WHERE time > now() - ${range}${filter}\n` +
      `GROUP BY time(${interval})${groupTag} fill(null)`,
    )
  }

  return (
    <>
      <div className="assistant-scroll">
        <fieldset className="assistant-section">
          <legend>聚合函数</legend>
          <label className="field-control">
            <span>函数</span>
            <div>
              <select aria-label="Aggregation" value={aggregation} onChange={(event) => setRequestedAggregation(event.target.value)}>
                {aggregations.map((candidate) => <option key={candidate}>{candidate}</option>)}
              </select>
              <ChevronDown size={13} />
            </div>
          </label>
          <label className="field-control">
            <span>Field</span>
            <div>
              <select aria-label="Field" value={field} disabled={measurement.fields.length === 0} onChange={(event) => setRequestedField(event.target.value)}>
                {measurement.fields.length === 0
                  ? <option value="">没有可用 Field</option>
                  : measurement.fields.map((candidate) => <option key={candidate.name} value={candidate.name}>{candidate.name} · {candidate.type}</option>)}
              </select>
              <ChevronDown size={13} />
            </div>
          </label>
        </fieldset>
        <fieldset className="assistant-section">
          <legend>时间窗口</legend>
          <div className="segmented-control">
            {[['6h', '6 小时'], ['24h', '24 小时'], ['7d', '7 天']].map(([value, label]) => <button key={value} className={range === value ? 'is-active' : ''} onClick={() => setRange(value)}>{label}</button>)}
          </div>
          <label className="field-control"><span>分组粒度</span><div><select value={interval} onChange={(event) => setInterval(event.target.value)}><option>1m</option><option>5m</option><option>15m</option><option>1h</option></select><ChevronDown size={13} /></div></label>
        </fieldset>
        <fieldset className="assistant-section">
          <legend>Tag 条件</legend>
          <label className="field-control">
            <span>Tag</span>
            <div>
              <select aria-label="Tag" value={tag} disabled={measurement.tags.length === 0} onChange={(event) => { setRequestedTag(event.target.value); setTagValue('') }}>
                <option value="">不使用 Tag</option>
                {measurement.tags.map((candidate) => <option key={candidate} value={candidate}>{candidate}</option>)}
              </select>
              <ChevronDown size={13} />
            </div>
          </label>
          <label className="field-control">
            <span>值</span>
            <input aria-label="Tag value" value={tagValue} disabled={!tag} onChange={(event) => setTagValue(event.target.value)} placeholder="输入精确值" />
          </label>
        </fieldset>
      </div>
      <div className="assistant-footer">
        <button className="primary-button" aria-label="应用到编辑器" disabled={!field} onClick={apply}><WandSparkles size={14} />应用到查询</button>
      </div>
    </>
  )
}

export function QueryAssistant() {
  const schema = useWorkbenchStore((state) => state.schema)
  const selected = useWorkbenchStore((state) => state.selectedMeasurement)
  const loading = useWorkbenchStore((state) => state.measurementSchemaLoading)
  const error = useWorkbenchStore((state) => state.measurementSchemaError)
  const toggleAssistant = useWorkbenchStore((state) => state.toggleAssistant)
  const measurement = selected
    ? schema.find((database) => database.name === selected.database)
      ?.measurements.find((candidate) => candidate.name === selected.measurement)
    : undefined

  return (
    <aside className="query-assistant">
      <div className="assistant-heading">
        <div><span className="eyebrow">BUILDER</span><strong>查询辅助</strong></div>
        <button className="icon-button" type="button" aria-label="收起查询辅助器" title="收起查询辅助器" onClick={toggleAssistant}><PanelRightClose size={16} /></button>
      </div>
      <div className={`assistant-source-value${error ? ' assistant-source-value--error' : ''}`} title={measurement && selected ? `${selected.database} / ${measurement.name}` : undefined}>
        <span className={`builder-state${error ? ' builder-state--error' : ''}`}>
          {loading ? <LoaderCircle size={12} className="spin" /> : <Check size={12} />}
          {loading ? '正在读取' : error ? '读取失败' : measurement ? '已同步' : '未选择'}
        </span>
        <span><Database size={12} /><span>{measurement && selected ? selected.database : '选择'}</span><strong>{measurement ? measurement.name : 'Measurement'}</strong></span>
      </div>
      {measurement && !loading && !error ? (
        <MeasurementBuilder key={`${selected?.database}\u0000${selected?.measurement}`} database={selected!.database} measurement={measurement} />
      ) : (
        <div className="assistant-empty" role="status">
          {loading ? <LoaderCircle size={18} className="spin" /> : <Database size={18} />}
          <strong>{loading ? '正在读取 Measurement Schema' : error ?? '未选择 Measurement'}</strong>
          <span>{loading ? 'Field 与 Tag 返回后再启用查询构建。' : '从 Schema 中选择数据源后显示 Field 与 Tag。'}</span>
        </div>
      )}
    </aside>
  )
}
