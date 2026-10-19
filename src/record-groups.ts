export type InspectionRecord = {
  id: string; plate: string; checkStartTime: string; checkCompleteTime: string;
  photoCount: number; inspectionDurationSeconds: number | null; issue?: string;
};

export type CollectionJob = {
  id: string; status: string; createdAt: string; items: {status: string; videoName: string}[];
  downloadAfterCollection?: boolean;
};

export type BatchVideo = {
  id: string; jobId: string; activeRevision: number; revision: number;
  versions: {revision: number; state: string; downloadRequested?: boolean}[];
  deletion?: {state: string};
};

export const categoryLabels = {
  ready: '可查看', waiting: '等待平台结果', download: '下载中', saved: '已保存', review: '待核对', deleting: '清理中',
};
export type VideoCategory = keyof typeof categoryLabels;
export const videoStatusOptions: {value: 'all' | VideoCategory; label: string}[] = [
  {value:'all',label:'全部记录'}, {value:'saved',label:categoryLabels.saved},
  {value:'ready',label:categoryLabels.ready}, {value:'download',label:categoryLabels.download},
  {value:'waiting',label:categoryLabels.waiting}, {value:'review',label:categoryLabels.review},
  {value:'deleting',label:categoryLabels.deleting},
];

export function splitInspections(records: InspectionRecord[]) {
  return {approved: records.filter(record => !record.issue), unapproved: records.filter(record => !!record.issue)};
}

export function videoCategory(video: BatchVideo): VideoCategory {
  if (['pending', 'error'].includes(video.deletion?.state || '')) return 'deleting';
  const active = video.versions.find(version => version.revision === video.activeRevision);
  if (!active || ['attention', 'failed', 'uncertain'].includes(active.state)) return 'review';
  if (active.state === 'saved') return 'saved';
  if (active.downloadRequested || ['downloading', 'publishing'].includes(active.state)) return 'download';
  return active.state === 'ready' ? 'ready' : 'waiting';
}

export function canDownload(video: BatchVideo) {
  const active = video.versions.find(version => version.revision === video.activeRevision);
  return !['pending', 'error'].includes(video.deletion?.state || '') && !!active && !active.downloadRequested
    && ['ready', 'waiting', 'submitted'].includes(active.state);
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
