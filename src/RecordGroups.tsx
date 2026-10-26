import { useId, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import DateSection from './DateSection';
import RecordDelete from './RecordDelete';
import { groupSubmissionDays, submissionTime } from './submission-dates';
import { initialCapture } from './collection-actions';
import { canDownload, groupVideos, splitInspections, videoStatusOptions } from './record-groups';
import type { BatchVideo, CollectionJob, InspectionRecord } from './record-groups';
import './record-groups.css';

const duration = (seconds: number | null) => seconds === null ? '时长未记录'
  : `${Math.floor(seconds / 60)}分${seconds % 60 ? `${Math.round(seconds % 60)}秒` : ''}`;

export function VideoStatusFilters({value, counts, total, disabled, onChange}: {
  value: string; counts: Record<string,number>; total: number; disabled: boolean;
  onChange: (status: string) => void;
}) {
  return <div className="video-status-filters" role="group" aria-label="采集记录状态筛选">
    {videoStatusOptions.filter(option => !['review','deleting'].includes(option.value)
      || counts[option.value] || value === option.value).map(option => <button type="button" key={option.value}
      aria-pressed={value === option.value} aria-controls="video-record-results" disabled={disabled}
      onClick={() => onChange(option.value)}>{option.label}<span>{option.value === 'all' ? total : counts[option.value] || 0}</span></button>)}
  </div>;
}

export function InspectionResults({records, selected, disabled, onToggle, onSelectAll, onCollect, canCollect, canSave, directory}: {
  records: InspectionRecord[]; selected: Set<string>; disabled: boolean;
  onToggle: (record: InspectionRecord) => void; onSelectAll: (ids: string[]) => void; onCollect:(download:boolean)=>void; canCollect:boolean; canSave:boolean; directory:string;
}) {
  const [category, setCategory] = useState<'approved' | 'unapproved'>('approved');
  const id = useId();
  const groups = useMemo(() => splitInspections(records), [records]);
  const rows = groups[category];
  const selectedCount = groups.approved.filter(record => selected.has(record.id)).length;
  const allSelected = !!groups.approved.length && selectedCount === groups.approved.length;
  return <div className="inspection-results">
    <div className="inspection-tabs" role="group" aria-label="质检准入分类">
      <button id={`${id}-approved`} aria-pressed={category === 'approved'} aria-controls={`${id}-results`}
        onClick={() => setCategory('approved')}>已批准 <span>{groups.approved.length}</span></button>
      <button id={`${id}-unapproved`} aria-pressed={category === 'unapproved'} aria-controls={`${id}-results`}
        onClick={() => setCategory('unapproved')}>未批准 <span>{groups.unapproved.length}</span></button>
    </div>
    <section id={`${id}-results`} aria-labelledby={`${id}-${category}`}>
      <div className="inspection-toolbar"><p>{category === 'approved'
        ? `符合准入 ${rows.length} 条，已选 ${selectedCount} 条。选择后直接采集。`
        : `未批准 ${rows.length} 条，以下显示具体原因。`}</p>
        {category === 'approved' && <button className="secondary" disabled={disabled || !rows.length}
          onClick={() => onSelectAll(allSelected ? [] : groups.approved.map(record => record.id))}>
          {allSelected ? '取消全选已批准' : '全选已批准'}</button>}
      </div>
      {rows.length ? <><div className="table-scroll"><table><thead><tr>
        {category === 'approved' && <th>选择</th>}<th>车牌</th><th>质检开始</th><th>质检结束</th><th>照片</th><th>时长</th>
        {category === 'approved' ? <th>计划采集（60分钟）</th> : <th>未批准原因</th>}
      </tr></thead><tbody>{rows.map(record => <tr key={record.id}>
        {category === 'approved' && <td><input type="checkbox" aria-label={`选择 ${record.plate} ${record.checkStartTime}`}
          checked={selected.has(record.id)} disabled={disabled} onChange={() => onToggle(record)} /></td>}
        <td><strong>{record.plate}</strong></td><td>{record.checkStartTime}</td><td>{record.checkCompleteTime}</td>
        <td>{record.photoCount}</td><td>{duration(record.inspectionDurationSeconds)}</td>
        {category === 'approved' ? <td className="planned-capture">{initialCapture(record.checkStartTime)?.start || '时间无效'}<small>至 {initialCapture(record.checkStartTime)?.end || '时间无效'}</small></td> : <td className="inspection-reason">{record.issue}</td>}
      </tr>)}</tbody></table></div>
      <div className="mobile-records">{rows.map(record => <div key={record.id} className="mobile-record">
        <div className="mobile-record-head">{category === 'approved' && <input type="checkbox"
          aria-label={`选择 ${record.plate} ${record.checkStartTime}`} checked={selected.has(record.id)} disabled={disabled}
          onChange={() => onToggle(record)} />}<strong>{record.plate}</strong></div>
        <p>质检开始：{record.checkStartTime}</p><p>质检结束：{record.checkCompleteTime}</p>
        <small>照片 {record.photoCount} 张 · {duration(record.inspectionDurationSeconds)}</small>
        {category === 'approved' && <p className="planned-capture">计划采集：{initialCapture(record.checkStartTime)?.start} → {initialCapture(record.checkStartTime)?.end} · 60分钟</p>}
        {record.issue && <p className="inspection-reason">{record.issue}</p>}
      </div>)}</div></> : <p className="group-empty">{category === 'approved' ? '本次查询没有已批准的实绩。' : '本次查询没有未批准的实绩。'}</p>}
    </section>
    <div className="capture-submit"><div><strong>已选 {selectedCount} 条 · 初采 60 分钟</strong>
      <p>采集后保存到：{directory || '请先启用文件工作区'}</p><p>自动核对已有任务，重复记录不再采集。</p></div>
      <div className="actions"><button className="secondary" disabled={!selectedCount || disabled || !canCollect} onClick={()=>onCollect(false)}>仅采集所选</button>
        <button disabled={!selectedCount || disabled || !canSave} onClick={()=>onCollect(true)}>采集并下载所选</button></div></div>
  </div>;
}

export function VideoBatches<T extends BatchVideo>({videos, jobs, totalByJob, selectedIds, disabled, downloadEnabled,
  onSelectBatch, onDownloadBatch, onDeleteRecords, children}: {
  videos: T[]; jobs: CollectionJob[]; totalByJob: Record<string, number>; selectedIds: Set<string>;
  disabled: boolean; downloadEnabled: boolean; onSelectBatch: (records: T[], select: boolean) => void;
  onDownloadBatch: (records: T[]) => void; onDeleteRecords:(records:T[],name:string,requestId:string)=>Promise<void>;
  children: (record: T) => ReactNode;
}) {
  const groups = useMemo(() => groupVideos(videos, jobs), [videos, jobs]);
  const dates = groupSubmissionDays(groups, group=>group.records[0].submittedAt || group.job?.createdAt);
  const deleteVersion=(records:T[])=>records.map(row=>`${row.id}:${row.revision}`).sort().join('|');
  return <div className="date-list" aria-label="按提交日期归类的采集记录">{dates.map(date=>{
    const records=date.records.flatMap(group=>group.records);
    return <DateSection key={date.day} day={date.day} summary={`${records.length} 条视频 · ${date.records.length} 次提交`}>
      <div className="date-tools"><p>按提交日期归类 · 仅操作当前筛选到的 {records.length} 条</p>
        <RecordDelete name={`${date.label} · ${records.length} 条采集记录`} version={deleteVersion(records)} disabled={disabled}
          onDelete={requestId=>onDeleteRecords(records,date.label,requestId)}/></div>
      {date.records.map(group=>{
        const downloadable=group.records.filter(canDownload);
        const selected=group.records.filter(record=>selectedIds.has(record.id)).length;
        const total=totalByJob[group.id] || group.records.length;
        return <section className="dated-batch" key={group.id} aria-label={`采集批次 ${group.id.slice(0,8)}`}>
          <div className="dated-batch-heading"><div><h3>{submissionTime(group.records[0].submittedAt || group.job?.createdAt)} 提交</h3>
            <p>批次 {group.id.slice(0,8)} · {group.records.length===total?`${total} 条`:`筛选 ${group.records.length} / ${total} 条`}</p></div>
            <RecordDelete name={`批次 ${group.id.slice(0,8)}`} version={deleteVersion(group.records)} disabled={disabled}
              onDelete={requestId=>onDeleteRecords(group.records,`批次 ${group.id.slice(0,8)}`,requestId)}/></div>
          <div className="batch-toolbar"><span>{selected ? `已选 ${selected} 条` : `可下载 ${downloadable.length} 条`}</span>
            <div className="actions"><button className="secondary" disabled={disabled || !downloadable.length}
              onClick={()=>onSelectBatch(downloadable,true)}>全选本批可下载</button>
              <button className="link" disabled={disabled || !selected} onClick={()=>onSelectBatch(group.records,false)}>取消选择</button>
              <button disabled={disabled || !downloadEnabled || !downloadable.length} onClick={()=>onDownloadBatch(downloadable)}>下载本批当前版</button></div></div>
          <div className="batch-video-grid">{group.records.map(record=><div className="batch-record" key={record.id}>{children(record)}</div>)}</div>
        </section>;
      })}
    </DateSection>;
  })}</div>;
}
