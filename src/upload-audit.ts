export type AuditBucket = 'uploaded' | 'missing_magnet' | 'missing_bed' | 'both_missing' | 'held';

export function auditBucket(file: {issue?: string; targets: Record<string,{status: string}>}): AuditBucket {
  const bed = file.targets['27912']?.status;
  const magnet = file.targets['27944']?.status;
  if (file.issue || !['uploaded','missing'].includes(bed) || !['uploaded','missing'].includes(magnet)) return 'held';
  if (bed === magnet) return bed === 'uploaded' ? 'uploaded' : 'both_missing';
  return magnet === 'missing' ? 'missing_magnet' : 'missing_bed';
}
