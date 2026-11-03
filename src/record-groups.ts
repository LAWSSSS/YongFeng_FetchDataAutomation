import {partialComplete,recoveryInProgress} from './video-recovery.ts';

export type InspectionRecord = {
  id: string; plate: string; checkStartTime: string; checkCompleteTime: string;
  photoCount: number; inspectionDurationSeconds: number | null; issue?: string;
};

export type CollectionJob = {
  id: string; status: string; createdAt: string; items: {status: string; videoName: string}[];
  downloadAfterCollection?: boolean;
};

export type BatchVideo = {
  id: string; jobId: string; submittedAt?:string; activeRevision: number; revision: number;
  capabilities?:{version:number;expectedRevision:number;canConfirmFinal:boolean;confirmFinalReason?:string};
  versions: {revision: number; state: string; downloadRequested?: boolean;recovery?:{state:string;resultVerified?:boolean}}[];
  deletion?: {state: string};
  operation?:{kind:string;state:string;version?:number;expectedRevision?:number};
};

export const categoryLabels = {
  ready: '可查看', waiting: '等待平台结果', download: '下载中', saved: '已保存', complete:'已完成', review: '待核对', deleting: '清理中',
  processing:'恢复处理中',
};
export type VideoCategory = keyof typeof categoryLabels;
export const videoStatusOptions: {value: 'all' | VideoCategory; label: string}[] = [
  {value:'all',label:'全部记录'}, {value:'saved',label:categoryLabels.saved},
  {value:'ready',label:categoryLabels.ready}, {value:'download',label:categoryLabels.download},
  {value:'waiting',label:categoryLabels.waiting}, {value:'review',label:categoryLabels.review},
  {value:'processing',label:categoryLabels.processing},
  {value:'deleting',label:categoryLabels.deleting},
];

export type VideoSelection = {id: string; version: number; expectedRevision: number};

export function syncVideoSelection<T extends {id: string; revision: number; activeRevision: number}>(
  selection: Map<string, VideoSelection>, videos: T[],
) {
  if (!selection.size) return selection;
  let changed = false;
  const next = new Map(selection);
  for (const [id, chosen] of selection) {
    const video = videos.find(row => row.id === id);
    if (!video || video.activeRevision !== chosen.version || video.revision !== chosen.expectedRevision) {
      next.delete(id);
      changed = true;
    }
  }
  return changed ? next : selection;
}

export function selectVideoScope<T extends BatchVideo>(videos:T[],scope:string) {
  return videos.filter(video=>!['pending','error'].includes(video.deletion?.state||'') &&
    (scope==='downloadable'?canDownload(video):scope==='partial'?completedPartialVideo(video):scope==='all'));
}

export function splitInspections(records: InspectionRecord[]) {
  return {approved: records.filter(record => !record.issue), unapproved: records.filter(record => !!record.issue)};
}

export function videoCategory(video: BatchVideo): VideoCategory {
  if (['pending', 'error'].includes(video.deletion?.state || '')) return 'deleting';
  const active = video.versions.find(version => version.revision === video.activeRevision);
  if(active&&partialComplete(active))return 'saved';
  if(recoveryInProgress(video))return 'processing';
  if (!active || ['attention', 'failed', 'uncertain'].includes(active.state)) return 'review';
  if (active.state === 'saved') return 'saved';
  if (active.downloadRequested || ['downloading', 'publishing'].includes(active.state)) return 'download';
  return active.state === 'ready' ? 'ready' : 'waiting';
}

export function completedPartialVideo(video: BatchVideo) {
  const active = video.versions.find(v=>v.revision===video.activeRevision);
  return !['pending','error'].includes(video.deletion?.state||'') && !!active && partialComplete(active);
}

export function canDownload(video: BatchVideo) {
  const capability=video.capabilities;
  return capability?.canConfirmFinal===true&&capability.version===video.activeRevision
    &&capability.expectedRevision===video.revision;
}

/** Reconcile again at the mutation boundary, not only after the last poll. */
export function visibleVideoSelections<T extends BatchVideo>(selection:Iterable<VideoSelection>,videos:T[],confirm=false) {
  const visible=new Map(videos.map(video=>[video.id,video]));
  return [...selection].filter(choice=>{
    const video=visible.get(choice.id);
    return !!video&&video.activeRevision===choice.version&&video.revision===choice.expectedRevision
      &&(!confirm||canDownload(video));
  });
}

export function groupVideos<T extends BatchVideo>(videos: T[], jobs: CollectionJob[]) {
  const groups = new Map<string, T[]>();
  for (const video of videos) {
    const group = groups.get(video.jobId) || [];
    group.push(video);
    groups.set(video.jobId, group);
  }
  const ordered = [...jobs.map(job => job.id), ...groups.keys()];
  return [...new Set(ordered)].filter(id => groups.has(id)).map(id => {
    const records = groups.get(id)!;
    const counts = {} as Partial<Record<VideoCategory, number>>;
    for (const record of records) {
      const category = videoCategory(record);
      counts[category] = (counts[category] || 0) + 1;
    }
    return {id, job: jobs.find(job => job.id === id), records, counts};
  });
}

export function batchDate(value?: string) {
  if (!value) return '创建时间未记录';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '创建时间未记录' : new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  }).format(date);
}
