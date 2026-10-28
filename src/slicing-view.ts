type SliceItem = {name: string; sourceDatasetId: string; sourceVideoId: string; state: string;
  categories: Record<string, {state: string; packageId?: string; taskId?: string}>};

export function physicalCategories<T extends SliceItem>(records: T[], source = 'all') {
  return (['bed','magnet'] as const).flatMap(categoryName => {
    if (source !== 'all' && source !== (categoryName === 'bed' ? '27912' : '27944')) return [];
    const candidates = records.filter(item => item.categories[categoryName] && item.categories[categoryName].state !== 'not_applicable');
    const score = (item: T) => {
      const category = item.categories[categoryName];
      return (category.packageId || category.taskId ? 100 : 0) + (category.state !== 'pending' ? 10 : 0) +
        (item.sourceDatasetId === (categoryName === 'bed' ? '27912' : '27944') ? 1 : 0);
    };
    const item = candidates.sort((a,b) => score(b)-score(a))[0];
    return item ? [{item,categoryName,category:item.categories[categoryName]}] : [];
  });
}

export function sliceTitle(name: string) {
  const match = name.match(/^(.+)_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})_/);
  return {title:match?.[1] || name.replace(/\.[^.]+$/, ''),
    when:match ? `${match[2]}-${match[3]}-${match[4]} ${match[5]}:${match[6]}:${match[7]}` : '',
    restored:/_恢复\.[^.]+$/.test(name)};
}

export function slicingControls(state: {enabled: boolean; busy: boolean;resumeAvailable?:boolean; run: {
  state: string; finishedAt?: string; singleMode?: boolean; singleItemId?: string;
} | null; items: {id: string; state: string}[]}) {
  const {run} = state;
  const idle = !state.enabled && !state.busy;
  const scanReady = !!run && (['complete','pilot_complete'].includes(run.state) ||
    (['attention','paused','interrupted'].includes(run.state) && !!(run.finishedAt || run.singleMode)));
  const selected = run?.singleMode ? state.items.find(item=>item.id === run.singleItemId) : undefined;
  const canResume = idle && !!run && ['paused','interrupted','waiting_connection','attention'].includes(run.state) &&
    (!run.singleMode || (!!selected && !['attention','complete','no_target','superseded'].includes(selected.state))) &&
    (state.resumeAvailable??state.items.some(item=>['pending','analyzing','platform_processing','waiting_connection','waiting','submitting'].includes(item.state)));
  return {idle,scanReady,canStart:idle && scanReady,canResume};
}

export function slicePage<T extends SliceItem>(items: T[], options: {
  status: string; source: string; search: string; page: number; pageSize: number;
}) {
  const needle = options.search.trim().toLocaleLowerCase();
  const filtered = items.filter(item => {
    const selectedCategory = options.source === 'all' ? undefined : options.source === '27912' ? 'bed' : 'magnet';
    const values = selectedCategory ? [item.categories[selectedCategory]].filter(Boolean) : Object.values(item.categories);
    const statuses = values.map(category => category.state).filter(state => state !== 'not_applicable');
    const finished = item.state === 'complete' || item.state === 'no_target';
    const matching = options.status === 'all' || (selectedCategory ? statuses.includes(options.status) : item.state === options.status) ||
      (options.status === 'unsliced' && !finished) ||
      (options.status === 'analyzing' && item.state === 'analyzing' && statuses.includes('pending')) ||
      (options.status === 'platform_processing' && statuses.some(state => ['ready','exporting','waiting_slot','waiting','submitting','queued','uploading','uploaded','importing'].includes(state)));
    return matching && (options.source === 'all' || statuses.length > 0) &&
      (!needle || (item.name + ' ' + item.sourceVideoId).toLocaleLowerCase().includes(needle));
  });
  const pageSize = Math.max(1, Math.floor(options.pageSize));
  const pages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const page = Math.max(1, Math.min(pages, Math.floor(options.page) || 1));
  return {rows:filtered.slice((page-1)*pageSize,page*pageSize),total:filtered.length,pages,page};
}

export function physicalFiles<T extends SliceItem & {id: string; path?: string;mediaId?:string;mediaGeneration?:number;collectionRevision?:number}>(items: T[]) {
  const groups = new Map<string, T[]>();
  for (const item of items) {
    // Unbound, similarly named sources must remain distinct until bound.
    const key = item.mediaId ? `${item.mediaId}:g${item.mediaGeneration??0}:v${item.collectionRevision??''}` : item.path || `unbound:${item.id}`;
    groups.set(key, [...(groups.get(key) || []), item]);
  }
  return [...groups].map(([key, records]) => ({key,records,name:records[0].name}));
}

export function physicalState(records: {state:string}[]) {
  return ['attention','review','analyzing','waiting_connection','pending','platform_processing'].find(
    status=>records.some(record=>record.state === status)) ||
    (records.every(record=>record.state === 'no_target') ? 'no_target' : 'complete');
}
