import { useMemo } from 'react';
import type { ReactNode } from 'react';
import DateSection from './DateSection';
import { submissionTime } from './submission-dates';
import { groupVideoDays, videoStatusOptions } from './record-groups';
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

export function VideoBatches<T extends BatchVideo>({videos, visibleIds, jobs, selecting, disabled,
  onSelectBatch, children, focusedVideoId, focusKey}: {
  videos: T[]; visibleIds: Set<string>; jobs: CollectionJob[]; selecting:boolean;
  disabled: boolean;  onSelectBatch: (records: T[], select: boolean) => void;
  children: (record: T) => ReactNode;
  focusedVideoId?:string;focusKey?:string;
}) {
  const dates = useMemo(() => groupVideoDays(videos, jobs), [videos, jobs]);
  const datesByJob = useMemo(() => new Map(jobs.map(job=>[job.id,job.createdAt])), [jobs]);
  return <div className="date-list" aria-label="按提交日期归类的采集记录">{dates.map(date=>{
    const records=date.records;
    const selectable=selecting?records.filter(record=>visibleIds.has(record.id)):[];
    return <DateSection key={date.day} day={date.day} rememberKey={`video-records:${date.day}`} expandedKey={records.some(r=>r.id===focusedVideoId)?focusKey:undefined} summary={`${records.length} 条视频 · ${date.submissionCount} 次提交`}>
      {selecting&&<div className="date-tools">
        <button className="secondary" disabled={disabled||!selectable.length} onClick={()=>onSelectBatch(selectable,true)}>选择当天记录</button>
      </div>}
      <div className="batch-video-grid date-video-grid">{records.map(record=><div className="batch-record" key={record.id}>
        <p className="card-submission-time"><time>{submissionTime(record.submittedAt||datesByJob.get(record.jobId))} 提交</time></p>
        {children(record)}
      </div>)}</div>
    </DateSection>;
  })}</div>;
}
