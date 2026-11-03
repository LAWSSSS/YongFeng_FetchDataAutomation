import { useMemo } from 'react';
import type { ReactNode } from 'react';
import DateSection from './DateSection';
import { groupSubmissionDays, submissionTime } from './submission-dates';
import { canDownload, groupVideos, videoStatusOptions } from './record-groups';
import type { BatchVideo, CollectionJob } from './record-groups';
import './record-groups.css';

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

export function VideoBatches<T extends BatchVideo>({videos, jobs, totalByJob, selectedIds, disabled,
  onSelectBatch, onDownloadBatch, children, focusedVideoId, focusKey}: {
  videos: T[]; jobs: CollectionJob[]; totalByJob: Record<string, number>; selectedIds: Set<string>;
  disabled: boolean;  onSelectBatch: (records: T[], select: boolean) => void;
  onDownloadBatch: (records: T[]) => void;
  children: (record: T) => ReactNode;
  focusedVideoId?:string;focusKey?:string;
}) {
  const groups = useMemo(() => groupVideos(videos, jobs), [videos, jobs]);
  const dates = groupSubmissionDays(groups, group=>group.records[0].submittedAt || group.job?.createdAt);
  return <div className="date-list" aria-label="按提交日期归类的采集记录">{dates.map(date=>{
    const records=date.records.flatMap(group=>group.records);
    return <DateSection key={date.day} day={date.day} rememberKey={`video-records:${date.day}`} expandedKey={records.some(r=>r.id===focusedVideoId)?focusKey:undefined} summary={`${records.length} 条视频 · ${date.records.length} 次提交`}>
      <div className="date-tools">
        <button className="secondary" disabled={disabled||!records.length} onClick={()=>onSelectBatch(records,true)}>选择当天记录</button></div>
      {date.records.map(group=>{
        const downloadable=group.records.filter(canDownload);
        const selected=group.records.filter(record=>selectedIds.has(record.id)).length;
        const total=totalByJob[group.id] || group.records.length;
        return <section className="dated-batch" key={group.id} aria-label={`采集批次 ${group.id.slice(0,8)}`}>
          <div className="dated-batch-heading"><div><h3>{submissionTime(group.records[0].submittedAt || group.job?.createdAt)} 提交</h3>
            <p>批次 {group.id.slice(0,8)} · {group.records.length===total?`${total} 条`:`筛选 ${group.records.length} / ${total} 条`}</p></div>
            <button className="secondary" disabled={disabled||!group.records.length} onClick={()=>onSelectBatch(group.records,true)}>选择本批记录</button></div>
          {(downloadable.length>0||selected>0)&&<div className="batch-toolbar"><span>{selected ? `已选 ${selected} 条` : `待确认 ${downloadable.length} 条`}</span>
            <div className="actions"><button className="secondary" disabled={disabled || !downloadable.length}
              onClick={()=>onSelectBatch(downloadable,true)}>全选本批待确认</button>
              <button className="link" disabled={disabled || !selected} onClick={()=>onSelectBatch(group.records,false)}>取消选择</button>
              <button disabled={disabled || !downloadable.length} onClick={()=>onDownloadBatch(downloadable)}>确认本批最终区间并处理</button></div></div>}
          <div className="batch-video-grid">{group.records.map(record=><div className="batch-record" key={record.id}>{children(record)}</div>)}</div>
        </section>;
      })}
    </DateSection>;
  })}</div>;
}
