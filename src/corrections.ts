export type CorrectionDraft = {date: string; clock: string; minutes: string};
export type PreparedCorrection = {
  startTime: string; endTime: string; durationSeconds: number; expectedRevision: number; requestId: string;
};

export function correctionInterval(draft: CorrectionDraft) {
  const clock = /^\d{2}:\d{2}$/.test(draft.clock) ? `${draft.clock}:00` : draft.clock;
  const startTime = `${draft.date} ${clock}`;
  const match = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/.exec(startTime);
  const minutes = Number(draft.minutes);
  if (!match || !draft.minutes.trim() || !Number.isFinite(minutes) || minutes < 1 || minutes > 120) {
    throw new Error('请填写有效的日期、时间和 1–120 分钟时长。');
  }
  const [year, month, day, hour, minute, second] = match.slice(1).map(Number);
  const start = new Date(Date.UTC(year,month-1,day,hour,minute,second));
  if (year < 2000 || year > 2100 || start.toISOString().slice(0,19).replace('T',' ') !== startTime) {
    throw new Error('采集日期或时间无效。');
  }
  const durationSeconds = Math.round(minutes * 60);
  const endTime = new Date(start.getTime() + durationSeconds * 1000).toISOString().slice(0,19).replace('T',' ');
  return {startTime,endTime,durationSeconds};
}

export function prepareCorrection(draft: CorrectionDraft, currentRevision: number, requestId: string): PreparedCorrection {
  if (!Number.isInteger(currentRevision) || currentRevision < 0 || !requestId) {
    throw new Error('记录版本无效，请刷新记录后重新核对。');
  }
  // Clicking review explicitly binds the preserved input to the currently
  // displayed record. Later record changes require another review.
  return {...correctionInterval(draft),expectedRevision:currentRevision,requestId};
}

export function correctionReviewCurrent(review: PreparedCorrection | null, revision: number) {
  return !!review && review.expectedRevision === revision;
}
