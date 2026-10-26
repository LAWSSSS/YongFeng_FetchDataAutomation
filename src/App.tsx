import { lazy, Suspense, useCallback, useEffect, useRef, useState } from 'react';
import PageBoundary from './PageBoundary';
import Workspace from './Workspace';
import type { FormEvent } from 'react';
import { usePolling } from './usePolling';
import AdmissionSettings from './AdmissionSettings';
import { collectionAction } from './collection-actions';
import { correctionInterval, correctionReviewCurrent, prepareCorrection } from './corrections';
import type { PreparedCorrection } from './corrections';
import { InspectionResults, VideoBatches, VideoStatusFilters } from './RecordGroups';
import { canDownload, syncVideoSelection, videoStatusOptions } from './record-groups';
import SubmissionRecords from './SubmissionRecords';
import RecordDelete from './RecordDelete';
import { videoRecovery } from './video-recovery';
import type { CollectionJob, InspectionRecord } from './record-groups';

const Slicing = lazy(() => import('./Slicing'));
const UploadAudit = lazy(() => import('./UploadAudit'));

type SessionEntry = {connected: boolean; user: string; hasSavedCredentials?: boolean; savedAccount?: string; credentialWarning?: string};
type Session = { source: SessionEntry; target: SessionEntry; capabilities?: {batchDownload?: boolean} };
type RecordRow = InspectionRecord;
type Search = {id: string; status: string; message: string; records: RecordRow[]; scanned: number; total: number; admissionRevision: number};
type Job = CollectionJob;
type Version = {revision: number; startTime: string; endTime: string; durationSeconds: number; state: string;
  platformStatus?: string; taskId?: string; resultId?: string; errorCode?:string; message?: string; filePath?: string; localCleanupState?:string;
  receivedBytes?: number; totalBytes?: number; attempts?: number; downloadRequested?: boolean; writeAttempted?: boolean;
  review?: {code: string; message: string; actual?: {videoName?: string; startTime?: string; endTime?: string; taskStatus?: string} | null}};
type Video = {id: string; jobId: string; submittedAt?:string; plate: string; name: string; qualityStart: string; qualityEnd: string;
  revision: number; activeRevision: number; acceptedRevision: number | null; versions: Version[];
  cleanupPending?: boolean; error?: string;
  deletion?: {state: string; message: string; requestId: string; expectedRevision: number; deleteCloud: boolean; deleteLocalFiles: boolean}};
type VideoState = {videos: Video[]; directory: string; lastCheck: string; checkError: string;
  hiddenRecordCount?:number;
  totalCount: number; filteredCount: number; byJob: Record<string,number>; counts: Record<string,number>};
type VideoSelection = {id: string; version: number; expectedRevision: number};
type BulkResult = {outcomes: {id: string; ok: boolean; message?: string}[]};
const blankVideoFilters = {plate: '', dateFrom: '', dateTo: '', timeBasis: 'quality', status: 'all', correction: 'all'};

async function api<T>(path: string, data?: unknown): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`/api${path}`, data === undefined ? undefined : {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data),
    });
  } catch { throw new Error('本地程序暂时无法连接。'); }
  const result = await response.json();
  if (!response.ok) throw new Error(result.message || result.detail || `请求失败（${response.status}）`);
  return result as T;
}

const chinaToday = () => new Date(Date.now() + 8 * 3600_000).toISOString().slice(0, 10);
const emptySession: Session = {source: {connected: false, user: ''}, target: {connected: false, user: ''}};
const emptyVideos: VideoState = {videos: [], directory: '', lastCheck: '', checkError: '', totalCount: 0, filteredCount: 0, byJob: {}, counts: {}};
const uid = () => crypto.randomUUID();
const duration = (seconds: number) => `${Math.floor(seconds / 60)}分${seconds % 60 ? `${seconds % 60}秒` : ''}`;
const size = (bytes: number) => `${(bytes / 1024 ** 2).toFixed(1)} MB`;
function Login({service, connected, user, savedAccount = '', hasSavedCredentials = false, credentialWarning = '', onDone}: {
  service: 'source' | 'target'; connected: boolean; user: string; savedAccount?: string;
  hasSavedCredentials?: boolean; credentialWarning?: string; onDone: () => Promise<void>
}) {
  const [account, setAccount] = useState(savedAccount || user);
  const [password, setPassword] = useState('');
  const [remember, setRemember] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  async function submit(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError('');
    try { await api(`/session/${service}/login`, {account, password, remember}); setPassword(''); await onDone(); }
    catch (failure) { setError((failure as Error).message); }
    finally { setBusy(false); }
  }
  return <div className="login-card"><div className="login-title"><strong>{service === 'source' ? '质检网站' : '采集平台'}</strong>
    <span className={connected ? 'dot connected' : 'dot'} />{connected ? `已连接 · ${user}` : '尚未连接'}</div>
    {!connected && <form className="login-form" onSubmit={submit}><input aria-label={`${service}账号`} placeholder="账号" value={account} onChange={e => setAccount(e.target.value)} required />
      <input aria-label={`${service}密码`} type="password" placeholder={hasSavedCredentials ? '输入密码以更新本机配置' : '密码'} value={password} onChange={e => setPassword(e.target.value)} required />
      <label className="remember-password"><input type="checkbox" checked={remember} onChange={e => setRemember(e.target.checked)} />长期保存到本机配置</label>
      <button disabled={busy}>{busy ? '连接中…' : '连接'}</button></form>}
    {hasSavedCredentials && !connected && <p className="hint">本机配置中已保存账号 {savedAccount}；登录后会更新保存项。</p>}
    {credentialWarning && <p className="error">{credentialWarning}</p>}{error && <p className="error">{error}</p>}</div>;
}

function ClearRecord({video, busy, act}: {video: Video; busy: boolean; act: (path: string, body: object) => Promise<boolean>}) {
  const [scope, setScope] = useState('local');
  const [checked, setChecked] = useState(false);
  const [base, setBase] = useState(video.revision);
  const clearing = video.deletion && ['pending','error'].includes(video.deletion.state);
  if (clearing) return <div className="clear-panel" role="status"><strong>{video.deletion!.state === 'pending' ? '正在清理这条记录' : '清理尚未完成'}</strong><p>{video.deletion!.message}</p>
    <div className="actions">{video.deletion!.state === 'error' && <button disabled={busy} onClick={() => void act('remove-record', {
      expectedRevision:video.deletion!.expectedRevision,requestId:video.deletion!.requestId,
      deleteCloud:video.deletion!.deleteCloud,deleteLocalFiles:video.deletion!.deleteLocalFiles})}>重新核对并清理</button>}
    <button className="secondary" disabled={busy} onClick={() => void act('cancel-clear', {expectedRevision:video.revision,requestId:uid()})}>停止继续清理</button></div></div>;
  return <>
    <RecordDelete name={`${video.name} · ${video.qualityStart}`} version={video.revision} disabled={busy} onDelete={requestId=>act('hide-record',{
      expectedRevision:video.revision,requestId})}/>
    <details className="clear-record" onToggle={e => {if (e.currentTarget.open) {setBase(video.revision);setChecked(false);}}}>
    <summary>更多清理选项</summary>
    <div className="clear-panel"><label>清除范围<select value={scope} onChange={e => {setScope(e.target.value);setChecked(false);}}>
      <option value="local">仅清除网页记录和临时缓存</option><option value="all">同时清除平台任务、视频和已下载文件</option></select></label>
      <p>{scope === 'all' ? '这条记录的所有绑定版本将被清除，包括已下载到最终目录的文件。其他记录保留。' : '平台任务、视频和最终目录里的文件会保留；本条不再显示在网页中。'}</p>
      <label className="review-check"><input type="checkbox" checked={checked} onChange={e => setChecked(e.target.checked)} />我确认清除 {video.name} · 质检 {video.qualityStart}</label>
      {base !== video.revision && <p className="error">记录刚有新操作，请关闭此处后重新核对。</p>}
      <button className="danger" disabled={busy || !checked || base !== video.revision} onClick={() => void act('remove-record', {
        expectedRevision:base,requestId:uid(),deleteCloud:scope === 'all',deleteLocalFiles:scope === 'all'})}>确认清除所选范围</button>
    </div></details></>;
}

function VideoCard({video, directory, onRefresh, selected, onSelect, onRemoved}: {video: Video; directory: string; onRefresh: () => Promise<void>; selected: boolean; onSelect: () => void;
  onRemoved:(removalId:string)=>void}) {
  const active = video.versions.find(version => version.revision === video.activeRevision)!;
  const draftKey = `correction-draft:${video.id}`;
  const [initialDraft] = useState(() => {
    try {
      const draft = JSON.parse(sessionStorage.getItem(draftKey) || 'null');
      if (draft && typeof draft.date === 'string' && typeof draft.clock === 'string' && typeof draft.minutes === 'string' && Number.isInteger(draft.revision)) return draft as {date:string; clock:string; minutes:string; revision:number; dirty:boolean};
    } catch { /* Use the last submitted interval if browser storage is unavailable. */ }
    return {date:active.startTime.slice(0,10),clock:active.startTime.slice(11,19),minutes:String(active.durationSeconds/60),revision:video.revision,dirty:false};
  });
  const [playing, setPlaying] = useState<number | null>(null);
  const [reviewed, setReviewed] = useState(false);
  const [date, setDate] = useState(initialDraft.date);
  const [clock, setClock] = useState(initialDraft.clock);
  const [minutes, setMinutes] = useState(initialDraft.minutes);
  const [draftRevision, setDraftRevision] = useState(initialDraft.revision);
  const [dirty, setDirty] = useState(initialDraft.dirty);
  const [prepared, setPrepared] = useState<PreparedCorrection | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  useEffect(() => {
    if (!dirty) {
      setDate(active.startTime.slice(0, 10)); setClock(active.startTime.slice(11, 19));
      setMinutes(String(active.durationSeconds / 60)); setDraftRevision(video.revision);
    }
    setPrepared(null);
    setReviewed(false); setPlaying(null);
  }, [video.activeRevision, video.revision]);
  useEffect(() => {
    try {
      if (dirty) sessionStorage.setItem(draftKey, JSON.stringify({date,clock,minutes,revision:draftRevision,dirty:true}));
      else sessionStorage.removeItem(draftKey);
    } catch { /* Editing still works when browser storage is unavailable. */ }
  }, [date,clock,minutes,draftRevision,dirty,draftKey]);
  let draftEnd = '';
  try { draftEnd = correctionInterval({date,clock,minutes}).endTime; } catch { /* handled on review */ }
  const isCurrentSaved = video.acceptedRevision === active.revision && active.state === 'saved';
  const recovery = videoRecovery(active);
  const clearing = ['pending','error'].includes(video.deletion?.state || '');
  const canCorrect = ['ready','saved','waiting','submitted','attention','failed'].includes(active.state)
    && (!active.downloadRequested || active.state === 'failed') && (!active.writeAttempted || !!active.taskId) && !clearing;
  const staleDraft = draftRevision !== video.revision;
  const playable = video.versions.filter(v => v.taskId && v.resultId && ['ready','saved','failed','publishing','attention'].includes(v.state)
    && !['result_missing','result_identity_changed','multiple_results'].includes(v.review?.code || ''));
  const playingVersion = video.versions.find(v => v.revision === playing);
  async function act(path: string, body: object) {
    setBusy(true); setError('');
    try {
      const result = await api<Video&{recordRemoval?:{removalId:string}}>(`/videos/${video.id}/${path}`, body);
      if (path === 'corrections' || path === 'recapture') {setDirty(false);setPrepared(null);setDraftRevision(result.revision);}
      if (path === 'hide-record' && result.recordRemoval) onRemoved(result.recordRemoval.removalId);
      await onRefresh();
      return true;
    }
    catch (failure) { setError((failure as Error).message);return false; }
    finally { setBusy(false); }
  }
  function prepare(event: FormEvent) {
    event.preventDefault();
    try {
      const reviewed = prepareCorrection({date,clock,minutes},video.revision,uid());
      if (reviewed.startTime === active.startTime && reviewed.endTime === active.endTime && !recovery.canRecapture) {
        throw new Error('采集区间未变化，请先修改日期、时间或时长，再核对修改。');
      }
      setDraftRevision(video.revision);setDirty(true);setPrepared(reviewed);setError('');
    }
    catch (failure) { setError((failure as Error).message); }
  }
  function confirmCorrection() {
    if (!prepared || !correctionReviewCurrent(prepared,video.revision)) {
      setPrepared(null);setError('记录已有新操作，你填写的时间已保留，请再次点击“核对修改”。');return;
    }
    if (recovery.canRecapture && prepared.startTime === active.startTime && prepared.endTime === active.endTime) {
      void act('recapture', {version:active.revision,expectedRevision:prepared.expectedRevision,requestId:prepared.requestId});
    } else {
      void act('corrections', {startTime:prepared.startTime,durationSeconds:prepared.durationSeconds,
        expectedRevision:prepared.expectedRevision,requestId:prepared.requestId});
    }
  }
  const saved = video.versions.find(version => version.revision === video.acceptedRevision);
  const historical = video.versions.filter(version => version.revision !== active.revision && version.state !== 'withdrawn');
  const renderVersion = (version: Version) => <div key={version.revision} className={version.revision === active.revision ? 'version current' : 'version'}>
    <span>第 {version.revision + 1} 版{version.revision === 0 ? ' · 初采' : ' · 手动更正'}</span>
    <strong>{version.startTime} → {version.endTime}</strong><span>{duration(version.durationSeconds)}</span>
    <span>{version.state === 'cloud_deleted' ? '旧版已清理' : version.platformStatus || version.state}</span>
  </div>;
  return <article className="video-card">
    <div className="video-head"><div><div className="record-pick"><input type="checkbox" aria-label={`选择采集记录 ${video.name} ${video.qualityStart}`} checked={selected} disabled={clearing || busy} onChange={onSelect} /><h3>{video.name}</h3></div><span className="subtle">质检 {video.qualityStart} → {video.qualityEnd}</span></div>
      <span className={`badge ${active.state}`}>{clearing ? '清理待完成' : active.downloadRequested && active.state === 'ready' ? '等待下载' : active.state === 'waiting' && active.platformStatus === 'READYING' ? '平台排队中' : ({ready:'可查看',saved:'已保存',waiting:'等待平台结果',submitted:'已提交',uncertain:'待核对',downloading:'下载中',publishing:'正在保存',failed:'需处理',attention:'待核对',pending:'排队中',submitting:'提交中'} as Record<string,string>)[active.state] || active.state}</span></div>
    <div className="video-versions">{renderVersion(active)}
      {!!historical.length && <details className="version-history"><summary>历史版本（{historical.length}）</summary>{historical.map(renderVersion)}</details>}
    </div>
    {active.message && <p className="notice">{active.message}</p>}
    {recovery.failedMedia && <div className="notice"><strong>视频未通过完整性检查</strong><p>可查看当前视频，修改日期、时间或时长后重采，也可按当前区间重新采集。重试下载会复用原平台文件；若原文件损坏，建议重新采集。旧版在新版验证成功前保留。</p></div>}
    {active.state === 'attention' && <div className="notice"><strong>处理方法</strong><p>{({task_missing:'先重新核对这一条；任务仍不存在时，可按保留的更正区间重新采集。',task_identity_changed:'核对平台实际时段。下面的输入保留你的更正；需要修改时可继续更正，也可按保留的区间重采。',result_missing:'先重新核对这一条；也可查看可用旧版。若视频结果仍不存在，手动按当前更正区间重采。',platform_failed:'平台采集失败或终止。确认时段后可手动重采或继续更正。',result_identity_changed:'平台结果ID不同，需要在原平台核对确切视频，程序会保留原绑定。',multiple_results:'原平台返回多个视频，需要核对确切结果，程序不会自动选择。',submission_ambiguous:'提交后出现多个相同任务，需要核对确切任务ID，暂时不要再次提交。'} as Record<string,string>)[active.review?.code || ''] || '先重新核对这一条。异常解除后会恢复可查看状态，更正区间会保留。'}</p>
      {active.review?.actual && <p>平台实际：{active.review.actual.videoName} · {active.review.actual.startTime} → {active.review.actual.endTime} · {active.review.actual.taskStatus}</p>}</div>}
    {video.error && <p className="notice">{video.error}</p>}
    {active.state === 'downloading' && <p className="notice">已下载 {size(active.receivedBytes || 0)} / {size(active.totalBytes || 0)} · 第 {active.attempts || 1} 次尝试</p>}
    {saved?.filePath && <p className="saved-path">{saved.localCleanupState==='complete'?'本地文件已按核验规则清理，云端视频与历史记录保留。':`本地最终文件：${saved.filePath}`}</p>}
    {video.cleanupPending && <p className="notice">新版已保存，旧版仍在核对清理。</p>}
    {!clearing && <div className="actions">{playable.some(v => v.revision === active.revision) && <button className="secondary" onClick={() => {setPlaying(playing === active.revision ? null : active.revision); setReviewed(false);}}>{playing === active.revision ? '关闭视频' : '查看视频'}</button>}
      {playable.some(v => v.revision !== active.revision) && <select aria-label={`查看${video.name}的其他版本`} value={playing ?? ''} onChange={e => {setPlaying(e.target.value === '' ? null : Number(e.target.value));setReviewed(false);}}><option value="">查看其他可用版本</option>{playable.map(v => <option key={v.revision} value={v.revision}>第{v.revision+1}版 · {v.startTime}</option>)}</select>}
      {['attention','uncertain','waiting','failed'].includes(active.state) && <button className="secondary" disabled={busy} onClick={() => {void act('check', {});}}>重新核对这一条</button>}
      {recovery.canRecapture && <button disabled={busy} onClick={() => {if(window.confirm(`按已保存的区间重新采集：${active.startTime} → ${active.endTime}。现有更正和旧版将保留，是否提交？`)) void act('recapture', {version:active.revision,expectedRevision:video.revision,requestId:uid()});}}>按当前区间重新采集</button>}
      {active.revision > 0 && !isCurrentSaved && !['saved','downloading','publishing','withdrawn'].includes(active.state) && <button className="secondary" disabled={busy} onClick={() => {void act('withdraw', {version: active.revision, expectedRevision: video.revision, requestId: uid()});}}>撤回本次更正</button>}
      {active.state === 'failed' && active.downloadRequested && <button disabled={busy} onClick={() => {void act('retry-download', {});}}>重试下载</button>}
      {active.downloadRequested && <button className="secondary" disabled={busy} onClick={() => void act('cancel-download', {version:active.revision,expectedRevision:video.revision,requestId:uid()})}>停止本条下载，保留断点</button>}</div>}
    {playingVersion && !clearing && <div className="player"><video controls playsInline preload="metadata" src={`/api/videos/${video.id}/versions/${playingVersion.revision}/stream`} onPlay={() => setReviewed(true)} />
      <p className="subtle">正在查看第{playingVersion.revision+1}版：{playingVersion.startTime} → {playingVersion.endTime}</p>
      {playingVersion.state === 'attention' && <p className="notice">此视频仅供核对，异常解除后才能确认下载。</p>}
      {playing === active.revision && active.state === 'ready' && <><label className="review-check"><input type="checkbox" checked={reviewed} onChange={e => setReviewed(e.target.checked)} />我已查看并确认这一版视频</label><button disabled={!reviewed || !directory || busy} onClick={() => {void act('confirm', {version: active.revision, expectedRevision: video.revision, requestId: uid()});}}>确认这版并下载</button>{!directory && <p className="notice">请连接外置盘后再下载。</p>}</>}</div>}
    {canCorrect && <details className="correction"><summary>继续更正采集日期、时间与时长</summary>
      <form onSubmit={prepare}><p>当前采集：{active.startTime} → {active.endTime} · {duration(active.durationSeconds)}</p>
        <p className="subtle">可以连续更正多次，也可以更正已提交的排队任务；每次都保留上一版本，最终确认保存后才清理旧版。</p>
        <div className="correction-fields"><label>采集日期<input type="date" value={date} min="2000-01-01" max="2100-12-31" onChange={e => {setDate(e.target.value); setPrepared(null);setDirty(true);}} required /></label>
        <label>开始时间（北京时间）<input type="time" step="1" value={clock} onChange={e => {setClock(e.target.value); setPrepared(null);setDirty(true);}} required /></label>
        <label>时长（分钟）<input type="number" min="1" max="120" step="any" value={minutes} onChange={e => {setMinutes(e.target.value); setPrepared(null);setDirty(true);}} required /></label></div>
        {staleDraft && <p className="notice" role="status">本条已有新操作，你填写的时间已保留。请先查看上方最新采集区间，再点击“核对修改”，即可基于当前记录确认重新采集。</p>}
        <p className="subtle">草稿结束时间：{draftEnd || '请填写有效时间'}</p>
        {prepared && <p className="confirm-text" role="status">已按当前记录核对：将采集 {prepared.startTime} → {prepared.endTime}，时长 {duration(prepared.durationSeconds)}。</p>}
        <div className="actions"><button type="submit" disabled={busy}>核对修改</button>
          {prepared && <button type="button" disabled={busy || !correctionReviewCurrent(prepared,video.revision)} onClick={confirmCorrection}>确认重新采集</button>}</div>
      </form></details>}
    <ClearRecord video={video} busy={busy} act={act} />
    {error && <p className="error">{error}</p>}
  </article>;
}

export default function App() {
  const [removedVideo,setRemovedVideo]=useState<{id:string;name:string;removalId:string;restoreRequestId:string}|null>(null);
  const [tab, setTab] = useState<'collect' | 'videos' | 'audit' | 'slicing'>(() => {
    try {const saved=sessionStorage.getItem('simple-active-tab');
      if (saved === 'audit' || saved === 'slicing') return saved;
      if (saved === 'videos') return saved;
    } catch { /* The initial page also works when local preferences are unavailable. */ }
    return 'collect';
  });
  useEffect(() => {try {sessionStorage.setItem('simple-active-tab',tab);} catch { /* Optional preference. */ }}, [tab]);
  const [session, setSession] = useState<Session>(emptySession);
  const [state, setState] = useState<VideoState>(emptyVideos);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [filters, setFilters] = useState({dateFrom: chinaToday(), dateTo: chinaToday(), timeFrom: '00:00:00', timeTo: '23:59:59', plates: ''});
  const [search, setSearch] = useState<Search | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const collectAction = useRef(collectionAction(api));
  const [admissionBlocked,setAdmissionBlocked] = useState(true);
  const [busy, setBusy] = useState('');
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [videoFilters, setVideoFilters] = useState(blankVideoFilters);
  const [appliedVideoFilters, setAppliedVideoFilters] = useState(blankVideoFilters);
  const [videoSelection, setVideoSelection] = useState<Map<string,VideoSelection>>(new Map());
  const [clearScope, setClearScope] = useState('local');
  const [showBulkClear, setShowBulkClear] = useState(false);
  const [bulkErrors, setBulkErrors] = useState<string[]>([]);
  const refreshSequence = useRef(0);
  const admissionChanged = useCallback((blocked: boolean) => {
    setAdmissionBlocked(blocked);
    if (blocked) {setSelected(new Set());}
  },[]);
  const refresh = useCallback(async () => {
    const sequence = ++refreshSequence.current;
    const query = new URLSearchParams(appliedVideoFilters);
    if (tab !== 'videos') query.set('summary', 'true');
    else query.set('compact','true');
    const [s, v, j] = await Promise.all([api<Session>('/session'), api<VideoState>(`/videos?${query}`),
      tab === 'collect' || tab === 'videos' ? api<Job[]>('/jobs?summary=true') : Promise.resolve(null)]);
    if (sequence !== refreshSequence.current) return;
    setSession(s); setState(v); if (j) setJobs(j);
    setVideoSelection(before => syncVideoSelection(before, v.videos));
  }, [appliedVideoFilters, tab]);
  useEffect(() => {void refresh().catch(() => undefined);}, [refresh]);
  usePolling(refresh, tab === 'videos' ? 4000 : tab === 'collect' ? 10000 : 30000, {immediate:false});
  usePolling(async () => {if (search) setSearch(await api<Search>(`/search/${search.id}`));},
    search?.status === 'running' && tab === 'collect' ? 1500 : null, {immediate:false});
  async function run(action: string, fn: () => Promise<void>) {
    setBusy(action); setError(''); setMessage('');
    try { await fn(); } catch (failure) { setError((failure as Error).message); }
    finally { setBusy(''); }
  }
  function select(record: RecordRow) { setSelected(before => {const next = new Set(before); if (next.has(record.id)) next.delete(record.id); else next.add(record.id); return next;});  }
  function pickVideo(video: Video) {
    setShowBulkClear(false);
    setVideoSelection(before => {const next = new Map(before); if (next.has(video.id)) next.delete(video.id);
      else next.set(video.id, {id:video.id,version:video.activeRevision,expectedRevision:video.revision}); return next;});
  }
  function applyVideoFilters(value: typeof blankVideoFilters) {
    setAppliedVideoFilters({...value});setVideoSelection(new Map());setShowBulkClear(false);setBulkErrors([]);
  }
  function filterVideoStatus(status: string) {
    setVideoFilters(current => ({...current,status}));
    applyVideoFilters({...appliedVideoFilters,status});
  }
  function selectBatch(records: Video[], select: boolean) {
    setShowBulkClear(false);
    setVideoSelection(before => {const next = new Map(before); records.forEach(video => {
      if (select) next.set(video.id, {id:video.id,version:video.activeRevision,expectedRevision:video.revision});
      else next.delete(video.id);
    }); return next;});
  }
  async function submitSelected(downloadAfterCollection:boolean) {
    if(!search || search.status!=='complete' || !selected.size)return;
    await run('submit',async()=>{
      const job=await collectAction.current<Job>(search.id,[...selected],search.admissionRevision,downloadAfterCollection);
      await refresh();setSelected(new Set());setTab('videos');
      setMessage(job.downloadAfterCollection?'已加入采集队列，平台完成后自动下载。':'已加入采集队列，完成后可查看和更正。');
    });
  }
  async function deleteVideoRecords(records:Video[],name:string,requestId:string) {
    const result=await api<{recordRemoval:{removalId:string}}>('/videos/records/remove',{
      requestId,selections:records.map(row=>({id:row.id,expectedRevision:row.revision})),
    });
    setRemovedVideo({id:requestId,name,removalId:result.recordRemoval.removalId,restoreRequestId:uid()});
    setVideoSelection(before=>{const next=new Map(before);records.forEach(row=>next.delete(row.id));return next;});
    await refresh();
  }
  async function bulk(action: 'confirm' | 'remove', explicit?: VideoSelection[]) {
    const selections = explicit || [...videoSelection.values()];
    if (!selections.length) return;
    if (action === 'confirm' && !window.confirm(`将选中的 ${selections.length} 条当前版本下载到 ${state.directory}？尚在采集的版本会等完成后保存；待核对项不会被批准。`)) return;
    await run(`bulk-${action}`, async () => {
      setBulkErrors([]);
      const result = await api<BulkResult>(`/videos/bulk-${action}`, {selections,requestId:uid(),deleteCloud:clearScope === 'all',deleteLocalFiles:clearScope === 'all'});
      const good = result.outcomes.filter(o => o.ok);
      setMessage(`${good.length} 条已加入${action === 'confirm' ? '下载' : '清理'}队列${result.outcomes.length > good.length ? `，${result.outcomes.length - good.length} 条已保留并需要核对` : ''}。`);
      setBulkErrors(result.outcomes.filter(o => !o.ok).map(o => `${state.videos.find(v => v.id === o.id)?.name || o.id}：${o.message}`));
      setVideoSelection(before => {const next = new Map(before);good.forEach(o => next.delete(o.id));return next;});
      setShowBulkClear(false);await refresh();
    });
  }
  return <div className="app-shell"><header className="site-header"><div className="brand"><span className="brand-symbol">映</span><div><strong>简易版质检视频采集</strong><small>采集 · 更正 · 上传 · 切片</small></div></div>
    <nav><button className={tab === 'collect' ? 'selected' : ''} onClick={() => setTab('collect')}>查询与批量采集</button><button className={tab === 'videos' ? 'selected' : ''} onClick={() => setTab('videos')}>视频查看与更正 <span>{state.totalCount}</span></button><button className={tab === 'audit' ? 'selected' : ''} onClick={() => setTab('audit')}>上传核对与自动上传</button><button className={tab === 'slicing' ? 'selected' : ''} onClick={() => setTab('slicing')}>模型识别与切片</button></nav></header>
    <main><section className={`hero ${tab === 'slicing' || tab === 'audit' ? 'compact' : ''}`}><div><h1>{tab === 'slicing' ? '按模型识别视频片段' : tab === 'audit' ? '找出还没上传的视频' : tab === 'collect' ? '从质检实绩发起采集' : '亲自决定留下哪一版'}</h1><p>{tab === 'slicing' ? '识别车斗和磁盘吊片段，导出照片并上传到对应图片集。' : tab === 'audit' ? '读取本地目录中的视频文件名，分别核对车斗检测与磁盘吊的导入日志。' : tab === 'collect' ? '初次采集固定为质检开始后 5 分钟，时长 60 分钟。在查询结果中选择后即可采集。' : '平台完成后先在线播放；确认原版或手动更正，最终才下载到你指定的文件夹。'}</p></div><div className="rule-card"><span>{tab === 'slicing' ? '默认模型抽样' : tab === 'audit' ? '共用一份本地清单' : '默认采集规则'}</span><strong>{tab === 'slicing' ? '30 秒 · 150 张' : tab === 'audit' ? '2 个数据集' : '+5 分钟'}</strong><small>{tab === 'slicing' ? '按目标区间自动选择切片间隔' : tab === 'audit' ? '各自读取日志 · 分别显示缺失名单' : '从质检开始时间计算 · 采集 60 分钟'}</small></div></section>
    <Workspace onChanged={refresh} />
    {(tab === 'collect' || tab === 'videos') && <div className="connections"><Login service="source" connected={session.source.connected} user={session.source.user}
      savedAccount={session.source.savedAccount} hasSavedCredentials={session.source.hasSavedCredentials} credentialWarning={session.source.credentialWarning} onDone={refresh} />
      <Login service="target" connected={session.target.connected} user={session.target.user}
      savedAccount={session.target.savedAccount} hasSavedCredentials={session.target.hasSavedCredentials} credentialWarning={session.target.credentialWarning} onDone={refresh} /></div>}
    {error && <p className="error global" role="alert">{error}</p>}{message && <p className="success global" role="status">{message}</p>}
    {bulkErrors.length > 0 && <div className="notice"><strong>这些条目已保留</strong><ul>{bulkErrors.map((item,i) => <li key={i}>{item}</li>)}</ul></div>}
    <PageBoundary key={tab}><Suspense fallback={<section className="panel" role="status">正在打开页面…</section>}>
    {tab === 'slicing' ? <Slicing onConnect={() => setTab('audit')} /> : tab === 'audit' ? <UploadAudit defaultDirectory={state.directory} /> : tab === 'collect' ? <><section className="panel"><div className="section-title"><div><h2>筛选质检实绩</h2></div><span>按保存的准入条件区分已批准与未批准</span></div>
      <details className="collection-admission"><summary>准入条件 <span>车牌、质检时长与照片要求</span></summary><AdmissionSettings<Search> searchId={search?.status === 'complete' ? search.id : undefined}
        disabled={!!busy || search?.status === 'running'} onBlockedChange={admissionChanged}
        onApplied={result => {if (result.search) setSearch(result.search);setSelected(new Set());}} /></details>
      <form className="filters" onSubmit={event => {event.preventDefault(); void run('search', async () => {const result = await api<Search>('/search', {filters}); setSearch(result); setSelected(new Set()); });}}>
        <label>开始日期<input type="date" value={filters.dateFrom} onChange={e => setFilters({...filters, dateFrom: e.target.value})} required /></label>
        <label>结束日期<input type="date" value={filters.dateTo} onChange={e => setFilters({...filters, dateTo: e.target.value})} required /></label>
        <label>每日开始<input type="time" step="1" value={filters.timeFrom} onChange={e => setFilters({...filters, timeFrom: e.target.value})} required /></label>
        <label>每日结束<input type="time" step="1" value={filters.timeTo} onChange={e => setFilters({...filters, timeTo: e.target.value})} required /></label>
        <label className="plate-filter">车牌，可留空<input value={filters.plates} onChange={e => setFilters({...filters, plates: e.target.value})} placeholder="多个车牌用逗号分隔" /></label>
        <button disabled={!session.source.connected || !!busy || admissionBlocked || search?.status === 'running'}>{busy === 'search' ? '查询中…' : '查询质检实绩'}</button></form>
      {search && <p className="notice">{search.message}{search.status === 'running' ? ` · 已检查 ${search.scanned} / ${search.total}` : ''}</p>}
      {search?.status === 'complete' && <InspectionResults key={`${search.id}:${search.admissionRevision}`} records={search.records} selected={selected} disabled={!!busy || admissionBlocked}
        onToggle={select} onSelectAll={ids => {setSelected(new Set(ids));}}
        canCollect={session.target.connected} canSave={!!session.capabilities?.batchDownload && !!state.directory && session.target.connected}
        directory={state.directory} onCollect={download=>void submitSelected(download)} />}</section>
      <SubmissionRecords kind="collection" onChanged={() => {void refresh();}} /></>
      : <><section className="panel"><div className="section-title"><div><h2>保存你明确选择的版本</h2></div><button className="secondary" disabled={!session.target.connected || !!busy} onClick={() => {void run('check', async () => {await api('/videos/check', {}); await refresh();});}}>检查平台状态</button></div>
        <p className="subtle">保存到工作区“采集视频”：{state.directory || '请先启用上方的文件工作区'}。最多同时下载 5 条；断盘时保留断点。可逐条查看后确认，也可批量批准下载。</p>{state.checkError && <p className="notice">{state.checkError}</p>}</section>
        <section className="panel record-controls"><div className="section-title"><h2>筛选采集记录</h2><span>筛选到 {state.filteredCount} / {state.totalCount} 条</span></div>
          <VideoStatusFilters value={appliedVideoFilters.status} counts={state.counts} total={state.totalCount}
            disabled={!!busy} onChange={filterVideoStatus} />
          <p className="status-filter-hint">点击状态立即筛选，数量随任务进度更新。下载中包含排队和写入磁盘；车牌、日期及更正条件点击“查询记录”生效。</p>
          <form className="record-filters" onSubmit={e => {e.preventDefault();applyVideoFilters(videoFilters);}}>
            <label className="record-search">车牌或视频名称<input value={videoFilters.plate} maxLength={500} placeholder="例如 VIRTUAL_020，多个名称用逗号分隔" onChange={e => setVideoFilters({...videoFilters,plate:e.target.value})} /></label>
            <label>日期依据<select value={videoFilters.timeBasis} onChange={e => setVideoFilters({...videoFilters,timeBasis:e.target.value})}><option value="quality">原始质检日期</option><option value="capture">当前版采集日期</option></select></label>
            <label>开始日期<input type="date" value={videoFilters.dateFrom} onChange={e => setVideoFilters({...videoFilters,dateFrom:e.target.value})} /></label>
            <label>结束日期<input type="date" value={videoFilters.dateTo} min={videoFilters.dateFrom || undefined} onChange={e => setVideoFilters({...videoFilters,dateTo:e.target.value})} /></label>
            <label>处理状态<select value={videoFilters.status} onChange={e => filterVideoStatus(e.target.value)}>{videoStatusOptions.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}</select></label>
            <label>更正情况<select value={videoFilters.correction} onChange={e => setVideoFilters({...videoFilters,correction:e.target.value})}><option value="all">全部记录</option><option value="corrected">有更正历史</option><option value="withdrawn">最后操作为撤回</option><option value="uncorrected">从未更正</option></select></label>
            <div className="actions"><button type="submit">查询记录</button><button type="button" className="secondary" onClick={() => {setVideoFilters(blankVideoFilters);applyVideoFilters(blankVideoFilters);}}>重置筛选</button></div>
          </form>
          <p className="record-counts" role="status">当前显示：{videoStatusOptions.find(option => option.value === appliedVideoFilters.status)?.label} · {state.filteredCount} / {state.totalCount} 条记录</p>
          <div className="selection-bar"><span>已选 {videoSelection.size} 条</span><button className="link" onClick={() => {setShowBulkClear(false);setVideoSelection(new Map(state.videos.filter(v => !['pending','error'].includes(v.deletion?.state || '')).map(v => [v.id,{id:v.id,version:v.activeRevision,expectedRevision:v.revision}])));}}>选择筛选结果</button>
            <button className="link" disabled={!!busy || !state.videos.some(canDownload)} onClick={() => selectBatch(state.videos.filter(canDownload), true)}>全选可下载当前版</button>
            <button className="link" disabled={!videoSelection.size} onClick={() => {setVideoSelection(new Map());setShowBulkClear(false);}}>取消选择</button>
            <div className="actions"><button disabled={!videoSelection.size || !state.directory || !!busy} onClick={() => void bulk('confirm')}>下载所选当前版</button><button className="secondary" disabled={!videoSelection.size || !!busy} onClick={() => setShowBulkClear(!showBulkClear)}>清除所选记录</button></div></div>
          {showBulkClear && <div className="clear-panel"><strong>清除已选 {videoSelection.size} 条</strong><label>清除范围<select value={clearScope} onChange={e => setClearScope(e.target.value)}><option value="local">仅网页记录和临时缓存</option><option value="all">平台任务、视频、记录及已下载文件</option></select></label>
            <p>{clearScope === 'all' ? '只处理这些条目已绑定的版本，删除后不能恢复视频。出现新更正或无法确认身份的条目会被保留。' : '平台视频和最终文件保留。出现新更正的条目会被保留。'}</p>
            <p className="selected-names">{[...videoSelection.keys()].map(id => state.videos.find(v => v.id === id)?.name || id).join('、')}</p>
            <div className="actions"><button className="danger" disabled={!!busy || !videoSelection.size} onClick={() => void bulk('remove')}>确认清除这 {videoSelection.size} 条</button><button className="secondary" onClick={() => setShowBulkClear(false)}>取消</button></div></div>}
        </section>
    <div id="video-record-results">{removedVideo&&<section className="panel record-deleted-notice" role="status">
      <span>已删除 {removedVideo.name} 的网页记录，原视频和更正进度保留。</span>
      <button className="link" disabled={!!busy} onClick={async()=>{
        setBusy('restore-record');setError('');
        try{await api('/videos/records/restore',{requestId:removedVideo.restoreRequestId,removalId:removedVideo.removalId});
          setRemovedVideo(null);await refresh();}
        catch(failure){setError((failure as Error).message);}finally{setBusy('');}
      }}>撤销删除</button></section>}
    {state.videos.length ? <VideoBatches videos={state.videos} jobs={jobs} totalByJob={state.byJob} selectedIds={new Set(videoSelection.keys())}
      disabled={!!busy} downloadEnabled={!!state.directory} onSelectBatch={selectBatch} onDeleteRecords={deleteVideoRecords}
      onDownloadBatch={records => void bulk('confirm', records.map(video => ({id:video.id,version:video.activeRevision,expectedRevision:video.revision})))}>
      {video => <VideoCard video={video} directory={state.directory} onRefresh={refresh} selected={videoSelection.has(video.id)} onSelect={() => pickVideo(video)} onRemoved={removalId=>{
        setRemovedVideo({id:video.id,name:video.name,removalId,restoreRequestId:uid()});
        setVideoSelection(before=>{const next=new Map(before);next.delete(video.id);return next;});
      }}/>}
    </VideoBatches> : <section className="panel empty"><h2>{state.totalCount ? '没有符合条件的记录' : state.hiddenRecordCount ? '当前没有可显示的记录' : '还没有采集记录'}</h2><p>{state.totalCount ? '可以调整日期、车牌和状态，或重置筛选。' : state.hiddenRecordCount ? '已删除记录的文件、更正和后台进度保留。' : '在“查询与批量采集”中提交任务后，这里会显示每辆车的采集进度。'}</p></section>}</div></>}
    </Suspense></PageBoundary></main><footer className="site-footer">本地运行 · 时间按北京时间显示 · 操作进度保存在本机</footer></div>;
}
