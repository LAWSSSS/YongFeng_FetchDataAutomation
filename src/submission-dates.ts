/** Submission dates are Beijing calendar days; media/capture dates never substitute. */
const beijingTime = new Intl.DateTimeFormat('zh-CN', {
  timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false,
});

export function submissionInstant(value?: string | null) {
  if (!value) return null;
  const text = value.trim();
  const explicit = /(?:Z|[+-]\d{2}:?\d{2})$/i.test(text);
  const iso = text.replace(' ', 'T');
  const date = new Date(explicit ? iso : `${iso}${text.length === 10 ? 'T00:00:00' : ''}+08:00`);
  return Number.isNaN(date.getTime()) ? null : date;
}

export function submissionDay(value?: string | null) {
  const date = submissionInstant(value);
  return date ? new Date(date.getTime() + 8 * 3600_000).toISOString().slice(0, 10) : '';
}

export function submissionTime(value?: string | null) {
  const date = submissionInstant(value);
  return date ? beijingTime.format(date) : '时间未记录';
}

export function groupSubmissionDays<T>(items: T[], dateOf: (item:T)=>string | null | undefined) {
  const dates = new Map<string,T[]>();
  for (const item of items) {
    const day = submissionDay(dateOf(item));
    const values = dates.get(day) || [];
    values.push(item); dates.set(day, values);
  }
  return [...dates].sort(([a],[b])=>b.localeCompare(a)).map(([day,records])=>({day,label:day || '日期未记录',records}));
}
