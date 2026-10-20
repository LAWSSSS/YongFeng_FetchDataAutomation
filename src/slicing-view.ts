type SliceItem = {name: string; sourceDatasetId: string; sourceVideoId: string; state: string;
  categories: Record<string, {state: string}>};

export function sliceTitle(name: string) {
  const match = name.match(/^(.+)_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})_/);
  return {title:match?.[1] || name.replace(/\.[^.]+$/, ''),
    when:match ? `${match[2]}-${match[3]}-${match[4]} ${match[5]}:${match[6]}:${match[7]}` : '',
    restored:/_恢复\.[^.]+$/.test(name)};
}

export function slicingControls(state: {enabled: boolean; busy: boolean; run: {
  state: string; finishedAt?: string; singleMode?: boolean; singleItemId?: string;
} | null; items: {id: string; state: string}[]}) {
  const {run} = state;
  const idle = !state.enabled && !state.busy;
  const scanReady = !!run && (['complete','pilot_complete'].includes(run.state) ||
    (['attention','paused','interrupted'].includes(run.state) && !!(run.finishedAt || run.singleMode)));
  const selected = run?.singleMode ? state.items.find(item=>item.id === run.singleItemId) : undefined;
  const canResume = idle && !!run && ['paused','interrupted','waiting_connection','attention'].includes(run.state) &&
    (!run.singleMode || (!!selected && !['attention','complete'].includes(selected.state)));
  return {idle,scanReady,canStart:idle && scanReady,canResume};
}

export function slicePage<T extends SliceItem>(items: T[], options: {
  status: string; source: string; search: string; page: number; pageSize: number;
}) {
  const needle = options.search.trim().toLocaleLowerCase();
  const filtered = items.filter(item => {
    const statuses = Object.values(item.categories).map(category => category.state);
    const matching = options.status === 'all' || item.state === options.status ||
      (options.status === 'platform_processing' && statuses.some(state => ['ready','waiting_slot','waiting','submitting'].includes(state)));
    return matching && (options.source === 'all' || item.sourceDatasetId === options.source) &&
      (!needle || (item.name + ' ' + item.sourceVideoId).toLocaleLowerCase().includes(needle));
  });
  const pageSize = Math.max(1, Math.floor(options.pageSize));
  const pages = Math.max(1, Math.ceil(filtered.length / pageSize));
  const page = Math.max(1, Math.min(pages, Math.floor(options.page) || 1));
  return {rows:filtered.slice((page-1)*pageSize,page*pageSize),total:filtered.length,pages,page};
}

export function physicalFiles<T extends SliceItem & {id: string; path?: string}>(items: T[]) {
  const groups = new Map<string, T[]>();
  for (const item of items) {
    // Unbound, similarly named sources must remain distinct until bound.
    const key = item.path || `unbound:${item.id}`;
    groups.set(key, [...(groups.get(key) || []), item]);
  }
  return [...groups].map(([key, records]) => ({key,records,name:records[0].name}));
}

export function physicalState(records: {state:string}[]) {
  return ['attention','analyzing','waiting_connection','pending','platform_processing'].find(
    status=>records.some(record=>record.state === status)) ||
    (records.every(record=>record.state === 'no_target') ? 'no_target' : 'complete');
}
