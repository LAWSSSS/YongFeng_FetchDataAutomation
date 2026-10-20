import { useId, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { CaretRight } from '@phosphor-icons/react';
import { batchDate, canDownload, categoryLabels, groupVideos, splitInspections, videoStatusOptions } from './record-groups';
import type { BatchVideo, CollectionJob, InspectionRecord, VideoCategory } from './record-groups';
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

export function InspectionResults({records, selected, disabled, onToggle, onSelectAll, onPreview}: {
  records: InspectionRecord[]; selected: Set<string>; disabled: boolean;
  onToggle: (record: InspectionRecord) => void; onSelectAll: (ids: string[]) => void; onPreview: () => void;
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
        ? `符合准入 ${rows.length} 条，已选 ${selectedCount} 条。核对采集时段后可统一采集并下载。`
        : `未批准 ${rows.length} 条，以下显示具体原因。`}</p>
        {category === 'approved' && <button className="secondary" disabled={disabled || !rows.length}
          onClick={() => onSelectAll(allSelected ? [] : groups.approved.map(record => record.id))}>
          {allSelected ? '取消全选已批准' : '全选已批准'}</button>}
      </div>
      {rows.length ? <><div className="table-scroll"><table><thead><tr>
        {category === 'approved' && <th>选择</th>}<th>车牌</th><th>质检开始</th><th>质检结束</th><th>照片</th><th>时长</th>
        {category === 'unapproved' && <th>未批准原因</th>}
      </tr></thead><tbody>{rows.map(record => <tr key={record.id}>
        {category === 'approved' && <td><input type="checkbox" aria-label={`选择 ${record.plate} ${record.checkStartTime}`}
          checked={selected.has(record.id)} disabled={disabled} onChange={() => onToggle(record)} /></td>}
        <td><strong>{record.plate}</strong></td><td>{record.checkStartTime}</td><td>{record.checkCompleteTime}</td>
        <td>{record.photoCount}</td><td>{duration(record.inspectionDurationSeconds)}</td>
        {category === 'unapproved' && <td className="inspection-reason">{record.issue}</td>}
      </tr>)}</tbody></table></div>
      <div className="mobile-records">{rows.map(record => <div key={record.id} className="mobile-record">
        <div className="mobile-record-head">{category === 'approved' && <input type="checkbox"
          aria-label={`选择 ${record.plate} ${record.checkStartTime}`} checked={selected.has(record.id)} disabled={disabled}
          onChange={() => onToggle(record)} />}<strong>{record.plate}</strong></div>
        <p>质检开始：{record.checkStartTime}</p><p>质检结束：{record.checkCompleteTime}</p>
        <small>照片 {record.photoCount} 张 · {duration(record.inspectionDurationSeconds)}</small>
        {record.issue && <p className="inspection-reason">{record.issue}</p>}
      </div>)}</div></> : <p className="group-empty">{category === 'approved' ? '本次查询没有已批准的实绩。' : '本次查询没有未批准的实绩。'}</p>}
    </section>
    <div className="panel-footer"><span>已选 {selectedCount} 条已批准实绩</span>
      <button disabled={!selectedCount || disabled} onClick={onPreview}>核对所选采集时段</button></div>
  </div>;
}

export function VideoBatches<T extends BatchVideo>({videos, jobs, totalByJob, selectedIds, disabled, downloadEnabled,
  onSelectBatch, onDownloadBatch, children}: {
  videos: T[]; jobs: CollectionJob[]; totalByJob: Record<string, number>; selectedIds: Set<string>;
  disabled: boolean; downloadEnabled: boolean; onSelectBatch: (records: T[], select: boolean) => void;
  onDownloadBatch: (records: T[]) => void; children: (record: T) => ReactNode;
}) {
  const groups = useMemo(() => groupVideos(videos, jobs), [videos, jobs]);
  const [visited, setVisited] = useState<Set<string>>(new Set());
  return <div className="batch-list" aria-label="按批次归类的采集记录">{groups.map(group => {
    const downloadable = group.records.filter(canDownload);
    const selected = group.records.filter(record => selectedIds.has(record.id)).length;
    const total = totalByJob[group.id] || group.records.length;
    return <details className="batch-group" key={group.id} onToggle={event => {
      if (event.currentTarget.open) setVisited(before => before.has(group.id) ? before : new Set([...before,group.id]));
    }}>
      <summary><CaretRight className="batch-chevron" size={18} aria-hidden="true" />
        <span className="batch-title"><strong>{batchDate(group.job?.createdAt)}</strong><small>批次 {group.id.slice(0, 8)} · {group.records.length === total ? `${total} 条记录` : `筛选 ${group.records.length} / ${total} 条`}{selected ? ` · 已选 ${selected} 条` : ''}</small></span>
        <span className="batch-counts">{(Object.entries(group.counts) as [VideoCategory, number][]).map(([key, count]) =>
          <span key={key} className={`batch-count ${key}`}>{categoryLabels[key]} {count}</span>)}<span className="batch-expand">展开查看</span></span>
      </summary>
      {visited.has(group.id) && <div className="batch-body"><div className="batch-toolbar"><span>以下操作仅作用于本批次当前筛选到的记录</span>
        <div className="actions"><button className="secondary" disabled={disabled || !downloadable.length}
          onClick={() => onSelectBatch(downloadable, true)}>全选本批可下载（{downloadable.length}）</button>
          <button className="link" disabled={disabled || !selected} onClick={() => onSelectBatch(group.records, false)}>取消本批选择</button>
          <button disabled={disabled || !downloadEnabled || !downloadable.length}
            onClick={() => onDownloadBatch(downloadable)}>下载本批可下载当前版</button></div>
      </div><div className="batch-video-grid">{group.records.map(record => <div className="batch-record" key={record.id}>{children(record)}</div>)}</div></div>}
    </details>;
  })}</div>;
}
