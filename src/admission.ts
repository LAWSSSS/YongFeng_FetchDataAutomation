export type AdmissionRule = {
  requireRealPlate: boolean; minInspectionMinutes: number; requireIntelligentPhotos: boolean;
  minPhotoCount: number; matchMode: 'any' | 'all';
};
export type AdmissionDraft = Omit<AdmissionRule,'minInspectionMinutes' | 'minPhotoCount'> & {
  minInspectionMinutes: string; minPhotoCount: string;
};
export const defaultAdmission: AdmissionRule = {
  requireRealPlate:true,minInspectionMinutes:10,requireIntelligentPhotos:true,minPhotoCount:1,matchMode:'any',
};
export function admissionDraft(rule: AdmissionRule): AdmissionDraft {
  return {...rule,minInspectionMinutes:String(rule.minInspectionMinutes),minPhotoCount:String(rule.minPhotoCount)};
}
export function parseAdmissionDraft(draft: AdmissionDraft): AdmissionRule {
  const minutes = Number(draft.minInspectionMinutes);
  let photos = Number(draft.minPhotoCount);
  if (!draft.minInspectionMinutes.trim() || !Number.isFinite(minutes) || minutes < 0 || minutes > 44640) {
    throw new Error('请填写 0–44640 分钟之间的质检时长，实际时长必须严格超过该值。');
  }
  if (!draft.minPhotoCount.trim() || !Number.isSafeInteger(photos) || photos < 1 || photos > 10000) {
    if (draft.requireIntelligentPhotos) throw new Error('智能判级照片数量须为 1–10000 张之间的整数。');
    photos = defaultAdmission.minPhotoCount;
  }
  return {...draft,minInspectionMinutes:minutes,minPhotoCount:photos};
}
export function admissionSummary(rule: AdmissionRule) {
  const identity: string[] = [];
  if (rule.requireRealPlate) identity.push('有真实车牌');
  if (rule.requireIntelligentPhotos) identity.push(`智能判级有效照片至少 ${rule.minPhotoCount} 张`);
  const conditions = identity.join(rule.matchMode === 'all' ? ' 且 ' : ' 或 ');
  const minutes = `质检时长严格超过 ${rule.minInspectionMinutes} 分钟`;
  return identity.length > 1 ? `（${conditions}），且${minutes}` : conditions ? `${conditions}，且${minutes}` : `不要求车牌和照片；${minutes}`;
}
