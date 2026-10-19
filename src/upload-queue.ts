type QueueItem = {state:string};
type QueueTarget = {items:QueueItem[];activeImport:string};

export function queueComplete(targets:QueueTarget[]):boolean {
  return targets.some(target=>target.items.length>0) && targets.every(target=>
    !target.activeImport && target.items.every(item=>item.state==='complete'));
}

export function effectiveUploadFilter(filter:string, items:QueueItem[]):string {
  if (filter!=='auto') return filter;
  return items.length>0 && items.every(item=>item.state==='complete') ? 'all' : 'active';
}

export function platformDatasetUrl(dataset:string, tab:'content'|'base'='content'):string {
  if (!['27912','27944'].includes(dataset)) throw new Error('不支持此上传目标。');
  return `http://pre-athena-mv.cisdigital.cn/#/dataset/${dataset}?tab=${tab}&namespace=1`;
}
