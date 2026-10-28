import { useEffect, useState } from 'react';
import type { FormEvent } from 'react';
import { admissionDraft, admissionSummary, defaultAdmission, parseAdmissionDraft } from './admission';
import type { AdmissionDraft, AdmissionRule } from './admission';
import './admission.css';
import {localJson} from './local-api';

type Saved = {rule: AdmissionRule; revision: number; summary: string};
async function settingsRequest<T>(body?: object): Promise<T> {
  return localJson<T>('/api/settings/admission',body);
}

export default function AdmissionSettings<T>({searchId, disabled, onBlockedChange, onApplied}: {
  searchId?: string; disabled: boolean; onBlockedChange: (blocked: boolean) => void;
  onApplied: (result: {search: T | null}) => void;
}) {
  const [saved,setSaved] = useState<Saved | null>(null);
  const [draft,setDraft] = useState<AdmissionDraft>(admissionDraft(defaultAdmission));
  const [loading,setLoading] = useState(true);
  const [saving,setSaving] = useState(false);
  const [error,setError] = useState('');
  const [notice,setNotice] = useState('');
  let proposed: AdmissionRule | null = null;
  try {proposed = parseAdmissionDraft(draft);} catch { /* Keep incomplete input editable. */ }
  const dirty = !saved || !proposed || JSON.stringify(saved.rule) !== JSON.stringify(proposed);
  const blocked = loading || saving || !saved || dirty;
  useEffect(() => {onBlockedChange(blocked);},[blocked,onBlockedChange]);
  useEffect(() => {
    let active = true;
    void settingsRequest<Saved>().then(result => {
      if (active) {setSaved(result);setDraft(admissionDraft(result.rule));setError('');}
    }).catch(failure => {if (active) setError((failure as Error).message);})
      .finally(() => {if (active) setLoading(false);});
    return () => {active = false;};
  },[]);
  function edit<K extends keyof AdmissionDraft>(key: K, value: AdmissionDraft[K]) {
    setDraft(current => ({...current,[key]:value}));setNotice('');
  }
  async function reload() {
    setLoading(true);setError('');setNotice('');
    try {const result = await settingsRequest<Saved>();setSaved(result);setDraft(admissionDraft(result.rule));}
    catch (failure) {setError((failure as Error).message);}
    finally {setLoading(false);}
  }
  async function save(event: FormEvent) {
    event.preventDefault();if (!saved || loading || saving) return;
    setSaving(true);setError('');setNotice('');
    try {
      const rule = parseAdmissionDraft(draft);
      const result = await settingsRequest<Saved & {search:T | null}>({rule,expectedRevision:saved.revision,searchId});
      setSaved(result);setDraft(admissionDraft(result.rule));onApplied(result);
      setNotice(result.search ? '条件已保存，当前结果已重新区分已批准和未批准，请重新选择采集记录。' : '条件已保存，后续查询和采集将使用此条件。');
    } catch (failure) {
      setError((failure as Error).message);
      // Reconcile an unknown response without discarding the user's draft.
      try {
        const latest=await settingsRequest<Saved>();setSaved(latest);
        const desired=parseAdmissionDraft(draft);
        if(JSON.stringify(latest.rule)===JSON.stringify(desired)){
          const search=searchId?await localJson<T>(`/api/search/${encodeURIComponent(searchId)}`):null;
          onApplied({search});setError('');setNotice('条件已保存，已核对后端结果。');
        }
      } catch { /* Keep the original error and editable draft until reachable. */ }
    }
    finally {setSaving(false);}
  }
  return <form className="admission-form" onSubmit={event => void save(event)} aria-label="采集准入条件设置">
    <fieldset disabled={disabled || loading || saving}>
      <legend>采集准入条件</legend>
      <p className="admission-help">“无”表示不要求这一项，有没有都可以。质检时长始终必须满足；条件会保存到本机。</p>
      <div className="admission-fields">
        <label>真实车牌<select aria-label="真实车牌要求" value={draft.requireRealPlate ? 'yes' : 'no'}
          onChange={e => edit('requireRealPlate',e.target.value === 'yes')}>
          <option value="yes">有（要求真实车牌）</option><option value="no">无（不要求）</option>
        </select></label>
        <label>质检时长严格超过（分钟）<input type="number" inputMode="decimal" min="0" max="44640" step="any" required
          value={draft.minInspectionMinutes} onChange={e => edit('minInspectionMinutes',e.target.value)} /></label>
        <div className="admission-photos"><label>智能判级照片<select aria-label="智能判级照片要求" value={draft.requireIntelligentPhotos ? 'yes' : 'no'}
          onChange={e => edit('requireIntelligentPhotos',e.target.value === 'yes')}>
          <option value="yes">有（要求有效照片）</option><option value="no">无（不要求）</option>
        </select></label>
          {draft.requireIntelligentPhotos && <label>至少（张）<input aria-label="智能判级照片最少张数" type="number" inputMode="numeric" min="1" max="10000" step="1" required
            value={draft.minPhotoCount} onChange={e => edit('minPhotoCount',e.target.value)} /></label>}
        </div>
      </div>
      <div className="admission-combination"><label>车牌与照片的组合<select aria-label="车牌和照片条件组合" value={draft.matchMode}
        disabled={!draft.requireRealPlate || !draft.requireIntelligentPhotos} onChange={e => edit('matchMode',e.target.value as AdmissionRule['matchMode'])}>
        <option value="any">任意一项满足即可</option><option value="all">两项必须同时满足</option>
      </select></label><p>仅一项选“有”时只要求这一项；两项都选“无”时只按质检时长批准。</p></div>
      <p className="admission-current" role="status">{loading ? '正在读取已保存条件…' : proposed
        ? `${dirty ? '待保存条件' : '当前条件'}：${admissionSummary(proposed)}` : '请填写有效的质检时长和照片数量。'}</p>
      <div className="actions"><button className="secondary" disabled={!saved}>{saving ? '正在保存…' : '保存并应用准入条件'}</button>
        <button className="link" type="button" onClick={() => {setDraft(admissionDraft(defaultAdmission));setNotice('默认条件已填入，保存后生效。');}}>恢复默认</button>
        <button className="link" type="button" onClick={() => void reload()}>重新读取已保存条件</button></div>
    </fieldset>
    {error && <p className="error" role="alert">{error}{!saved && <button className="link" type="button" disabled={loading || disabled} onClick={() => void reload()}>重新读取</button>}</p>}
    {notice && <p className="success" role="status">{notice}</p>}
    {saved && dirty && <p className="admission-help">请先保存条件，再查询或提交采集。已有批次、视频和手动更正按提交时的条件保留。</p>}
  </form>;
}
