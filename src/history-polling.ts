const active=new Set(['running','pending','submitting','submitted','waiting','processing']);

/** Completed receipts are immutable history, not a background work queue. */
export function historyRefreshMs(expanded:boolean,statuses:(string|undefined)[]):number|null {
  return expanded&&statuses.some(status=>active.has(status||''))?4000:null;
}
