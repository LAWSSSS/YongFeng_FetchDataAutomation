import { lazy, Suspense, useCallback, useEffect, useRef, useState } from 'react';
import PageBoundary from './PageBoundary';
import {localJson} from './local-api';
import {requestIntents} from './request-intent';
import Workspace from './Workspace';
import DownloadManager from './DownloadManager';
import type { DownloadWorkers } from './DownloadManager';
import type { FormEvent } from 'react';
import { usePolling } from './usePolling';
import AdmissionSettings from './AdmissionSettings';
import { collectionAction } from './collection-actions';
import { correctionInterval, correctionReviewCurrent, prepareCorrection } from './corrections';
import type { PreparedCorrection } from './corrections';
import { InspectionResults, VideoBatches, VideoStatusFilters } from './RecordGroups';
import { syncVideoSelection } from './record-groups';
import RecordSelection from './RecordSelection';
import './compact-controls.css';
import SubmissionRecords from './SubmissionRecords';
import RecordDelete from './RecordDelete';
import { partialComplete, videoRecovery, versionDownloadable } from './video-recovery';
import type { CollectionJob, InspectionRecord } from './record-groups';
import VideoProcessing from './VideoProcessing';
import type { ProcessingGroup } from './VideoProcessing';
import DateSection from './DateSection';
import { groupSubmissionDays } from './submission-dates';

const Slicing = lazy(() => import('./Slicing'));

type SessionEntry = {connected: boolean; user: string; hasSavedCredentials?: boolean; savedAccount?: string; credentialWarning?: string};
type Session = { source: SessionEntry; target: SessionEntry; capabilities?: {batchDownload?: boolean} };
type RecordRow = InspectionRecord;
type Search = {id: string; status: string; message: string; records: RecordRow[]; scanned: number; total: number; admissionRevision: number};
type Job = CollectionJob;
type Version = {revision: number; startTime: string; endTime: string; durationSeconds: number; state: string;
  platformStatus?: string; taskId?: string; resultId?: string; errorCode?:string; message?: string; filePath?: string; sha256?:string; localCleanupState?:string;
  receivedBytes?: number; totalBytes?: number; attempts?: number; downloadRequested?: boolean; writeAttempted?: boolean;
  etag?:string; recovery?:{state:string;resultVerified?:boolean;filePath?:string;slicingItemId?:string;successfulSamples?:number;skippedSamples?:number;
    message?:string;errorCode?:string;progress?:number;categories?:Record<string,{state?:string;message?:string;errorCode?:string;observations?:number;skippedSamples?:number;imageCount?:number;actualImages?:number;packageId?:string}>};
  review?: {code: string; message: string; actual?: {videoName?: string; startTime?: string; endTime?: string; taskStatus?: string} | null}};
type Video = {id: string; jobId: string; submittedAt?:string; plate: string; name: string; qualityStart: string; qualityEnd: string;
  revision: number; activeRevision: number; acceptedRevision: number | null; versions: Version[];
  operation?:{id:string;kind:string;state:string;stage?:string;message?:string;slicingItemId?:string}; recovery?:Version['recovery'];
  cleanupPending?: boolean; error?: string;
  deletion?: {state: string; message: string; requestId: string; expectedRevision: number; deleteCloud: boolean; deleteLocalFiles: boolean}};
type VideoState = {videos: Video[]; directory: string; lastCheck: string; checkError: string;
  hiddenRecordCount?:number; downloadWorkers?:DownloadWorkers;
  totalCount: number; filteredCount: number; byJob: Record<string,number>; counts: Record<string,number>};
type VideoSelection = {id: string; version: number; expectedRevision: number};
type BulkResult = {outcomes: {id: string; ok: boolean; message?: string}[]};
const blankVideoFilters = {plate: '', dateFrom: '', dateTo: '', timeBasis: 'quality', status: 'all', correction: 'all'};

async function api<T>(path: string, data?: unknown): Promise<T> {
  return localJson<T>(`/api${path}`,data);
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

function VideoCard({video, directory, onRefresh, selected, onSelect, onRemoved, processing}: {video: Video; directory: string; onRefresh: () => Promise<void>; selected: boolean; onSelect: () => void; processing:ProcessingGroup[];
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
  const restartRequest = useRef<{version:number;expectedRevision:number;requestId:string}|null>(null);
  const operationLocked = useRef(false);
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
  const clearing = ['pending','error'].includes(video.deletion?.state || '');
  const recoveryOptions = videoRecovery(active);
  const recoveryRunning = ['queued','moving','slicing'].includes(active.recovery?.state || '')
    || (video.operation?.kind === 'recover-decodable' && ['queued','running','waiting_connection','reconciling'].includes(video.operation.state));
  const recoveryPaused = active.recovery?.state === 'paused'
    || (video.operation?.kind === 'recover-decodable' && video.operation.state === 'paused');
  const canPartialRecover = recoveryOptions.failedMedia && ['invalid_video','media_incomplete'].includes(active.errorCode || '')
    && !recoveryRunning && !recoveryPaused && !clearing
    && (!active.recovery || active.recovery.state === 'attention');
  const canCorrect = ['ready','saved','waiting','submitted','attention','failed'].includes(active.state)
    && (!active.downloadRequested || active.state === 'failed') && (!active.writeAttempted || !!active.taskId) && !clearing;
  const canWithdraw=active.revision>0&&!isCurrentSaved&&!['saved','downloading','publishing','withdrawn'].includes(active.state);
  const hasMoreActions=['attention','uncertain','waiting','failed'].includes(active.state)||canWithdraw||recoveryOptions.canRestartDownload||active.downloadRequested;
  const staleDraft = draftRevision !== video.revision;
  const playable = video.versions.filter(v => v.taskId && v.resultId && ['ready','saved','failed','publishing','attention'].includes(v.state)
    && !['result_missing','result_identity_changed','multiple_results'].includes(v.review?.code || ''));
  const playingVersion = video.versions.find(v => v.revision === playing);
  async function act(path: string, body: object) {
    if(operationLocked.current)return false;
    operationLocked.current=true;
    setBusy(true); setError('');
    try {
      const result = await api<Partial<Video>&{recordRemoval?:{removalId:string};operationId?:string}>(`/videos/${video.id}/${path}`, body);
      if (path === 'corrections' || path === 'recapture') {setDirty(false);setPrepared(null);setDraftRevision(video.revision);}
      if (path === 'hide-record' && result.recordRemoval) onRemoved(result.recordRemoval.removalId);
      await onRefresh();
      return true;
    }
    catch (failure) { setError((failure as Error).message);return false; }
    finally { operationLocked.current=false;setBusy(false); }
  }
  function prepare(event: FormEvent) {
    event.preventDefault();
    try {
      const reviewed = prepareCorrection({date,clock,minutes},video.revision,uid());
      if (reviewed.startTime === active.startTime && reviewed.endTime === active.endTime && !recoveryOptions.canRecapture) {
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
    if (recoveryOptions.canRecapture && prepared.startTime === active.startTime && prepared.endTime === active.endTime) {
      void act('recapture', {version:active.revision,expectedRevision:prepared.expectedRevision,requestId:prepared.requestId});
    } else {
      void act('corrections', {startTime:prepared.startTime,durationSeconds:prepared.durationSeconds,
        expectedRevision:prepared.expectedRevision,requestId:prepared.requestId});
    }
  }
  const saved = video.versions.find(version => version.revision === video.acceptedRevision);
  const historical = video.versions.filter(version => version.revision !== active.revision && version.state !== 'withdrawn');
  const renderVersion = (version: Version) => <div key={version.revision} className="version-entry"><div className={version.revision === active.revision ? 'version current' : 'version'}>
    <span>第 {version.revision + 1} 版{version.revision === 0 ? ' · 初采' : ' · 手动更正'}</span>
    <strong>{version.startTime} → {version.endTime}</strong><span>{duration(version.durationSeconds)}</span>
    <span>{partialComplete(version)?'原片不完整 · 可用部分已保存':version.state === 'cloud_deleted' ? '旧版已清理' : version.platformStatus || version.state}</span>
    </div>{version.revision !== active.revision && <div className="version-actions">
      {versionDownloadable(version) && !clearing ? <a className="version-download" download
        href={`/api/videos/${video.id}/versions/${version.revision}/download`}>下载第 {version.revision + 1} 版</a>
        : <span className="subtle">{version.state === 'cloud_deleted' ? '此版已清理，无法下载' : '此版结果未确认，暂不能下载'}</span>}
    </div>}</div>;
  return <article className="video-card" id={`video-${video.id}`}>
    <div className="video-head"><div><div className="record-pick"><input type="checkbox" aria-label={`选择采集记录 ${video.name} ${video.qualityStart}`} checked={selected} disabled={clearing || busy} onChange={onSelect} /><h3>{video.name}</h3></div><span className="subtle">质检 {video.qualityStart} → {video.qualityEnd}</span></div>
      <span className={`badge ${partialComplete(active)?'saved':active.state}`}>{clearing ? '清理待完成' : partialComplete(active)?'已保存（可用部分）': active.downloadRequested && active.state === 'ready' ? '等待下载' : active.state === 'waiting' && active.platformStatus === 'READYING' ? '平台排队中' : ({ready:'可查看',saved:'已保存',waiting:'等待平台结果',submitted:'已提交',uncertain:'待核对',downloading:'下载中',publishing:'正在保存',failed:'需处理',attention:'待核对',pending:'排队中',submitting:'提交中'} as Record<string,string>)[active.state] || active.state}</span></div>
    <div className="video-versions">{renderVersion(active)}
      {!!historical.length && <details className="version-history"><summary>历史版本（{historical.length}）</summary>
        <p className="subtle">历史版本保存到浏览器的下载目录；可在浏览器中选择保存位置，不改变当前更正和下载进度。</p>{historical.map(renderVersion)}</details>}
    </div>
    {recoveryRunning && <div className="recovery-progress" role="status"><strong>{video.operation?.stage === 'waiting_connection' ? '等待平台连接' : '可用部分处理中'}</strong>
      <p>{video.operation?.message || active.recovery?.message || '正在识别可解码画面。'}</p>
      {active.recovery?.slicingItemId && <button className="link" onClick={() => window.dispatchEvent(new CustomEvent('simple-open-slicing'))}>查看切片进度</button>}
      <button className="secondary" disabled={busy} onClick={() => void act('recover-decodable/pause', {version:active.revision,expectedRevision:video.revision,requestId:uid()})}>暂停此条</button>
    </div>}
    {active.recovery && !recoveryRunning && <div className={`recovery-progress ${active.recovery.state === 'attention' ? 'needs-review' : ''}`}>
      <strong>{partialComplete(active) ? '可用部分已保存' : active.recovery.state === 'attention' ? '可用部分需处理' : '部分视频处理状态'}</strong>
      <p>{active.recovery.message || '坏位置已跳过；原视频仍标记为不完整。'}</p>
      <span>有效判断 {active.recovery.successfulSamples || 0} · 跳过位置 {active.recovery.skippedSamples || 0}</span>
      {recoveryPaused && <button disabled={busy} onClick={() => void act('recover-decodable/resume', {version:active.revision,expectedRevision:video.revision,requestId:uid()})}>继续此条</button>}
    </div>}
    {!recoveryRunning && !active.recovery && !recoveryOptions.failedMedia && active.message && active.state !== 'attention' && <p className="notice">{active.message}</p>}
    {recoveryOptions.failedMedia && !recoveryRunning && !active.recovery && <div className="notice"><strong>视频末段缺失或无法解码</strong><p>复用已下载文件，只处理可解码部分；坏位置跳过，不生成替代视频。旧版和下载断点保留。</p></div>}
    {recoveryOptions.checkpointProblem && <div className="notice"><strong>断点校验未通过</strong><p>请在“更多操作”中核对平台任务或重新获取完整文件；历史版本和进度保留。</p></div>}
    {active.state === 'attention' && !recoveryOptions.failedMedia && !active.recovery && <div className="notice"><strong>需要核对</strong><p>{({task_missing:'先重新核对这一条；任务仍不存在时，可按保留的更正区间重新采集。',task_identity_changed:'平台任务信息已变化，原绑定保留；请先核对平台实际时间。',result_missing:'先核对平台结果ID；也可查看可用旧版。',platform_failed:'平台采集失败或终止，可核对时间后重新采集。',result_identity_changed:'平台结果ID不同，请在原平台核对确切视频；原绑定已保留。',multiple_results:'平台返回多个视频，需要人工选择正确结果。',submission_ambiguous:'提交后出现多个相同任务，请核对确切任务ID，暂勿重新提交。'} as Record<string,string>)[active.review?.code || ''] || active.message || '请核对本条的任务和结果绑定。'}</p>
      {active.review?.actual && <p>平台实际：{active.review.actual.videoName} · {active.review.actual.startTime} → {active.review.actual.endTime} · {active.review.actual.taskStatus}</p>}</div>}
    {video.error && !active.message && <p className="notice">{video.error}</p>}
    {active.state === 'downloading' && <p className="notice">已下载 {size(active.receivedBytes || 0)} / {size(active.totalBytes || 0)} · 第 {active.attempts || 1} 次尝试</p>}
    {saved?.filePath && <p className="saved-path">{saved.localCleanupState==='complete'?'本地文件已按核验规则清理，云端视频与历史记录保留。':`${partialComplete(saved)?'保留原片':'本地最终文件'}：${saved.filePath}`}</p>}
    {video.cleanupPending && <p className="notice">新版已保存，旧版仍在核对清理。</p>}
    {!clearing && <div className="actions primary-video-actions">
      {playable.some(v => v.revision === active.revision) && <button className="secondary" onClick={() => {setPlaying(playing === active.revision ? null : active.revision); setReviewed(false);}}>{playing === active.revision ? '关闭视频' : '查看视频'}</button>}
      {canPartialRecover && <button disabled={busy} onClick={() => void act('recover-decodable', {expectedRevision:video.revision,requestId:uid()})}>处理可解码部分</button>}
      {playable.some(v => v.revision !== active.revision) && <select aria-label={`查看${video.name}的其他版本`} value={playing ?? ''} onChange={e => {setPlaying(e.target.value === '' ? null : Number(e.target.value));setReviewed(false);}}><option value="">查看其他可用版本</option>{playable.map(v => <option key={v.revision} value={v.revision}>第{v.revision+1}版 · {v.startTime}</option>)}</select>}
    </div>}
    {!clearing && hasMoreActions && <details className="more-video-actions"><summary>重试与版本操作</summary><div className="actions">
      {['attention','uncertain','waiting','failed'].includes(active.state) && <button className="secondary" disabled={busy || recoveryRunning} onClick={() => {void act('check', {expectedRevision:video.revision,requestId:uid()});}}>重新核对这一条</button>}
      {recoveryOptions.canRecapture && <button disabled={busy} onClick={() => {if(window.confirm(`按已保存的区间重新采集：${active.startTime} → ${active.endTime}。现有更正和旧版将保留，是否提交？`)) void act('recapture', {version:active.revision,expectedRevision:video.revision,requestId:uid()});}}>按当前区间重新采集</button>}
      {active.revision > 0 && !isCurrentSaved && !['saved','downloading','publishing','withdrawn'].includes(active.state) && <button className="secondary" disabled={busy} onClick={() => {void act('withdraw', {version: active.revision, expectedRevision: video.revision, requestId: uid()});}}>撤回本次更正</button>}
      {active.state === 'failed' && active.downloadRequested && !partialComplete(active) && <button disabled={busy} onClick={() => {void act('retry-download', {});}}>重试下载</button>}
      {recoveryOptions.canRestartDownload && <button className="secondary" disabled={busy || !directory || recoveryRunning} onClick={() => {
        if (!window.confirm(`重新下载 ${video.name} 第 ${active.revision + 1} 版？只清除这一版的下载断点，重新获取完整视频；其他版本和更正均保留。`)) return;
        if (!restartRequest.current || restartRequest.current.version !== active.revision || restartRequest.current.expectedRevision !== video.revision) {
          restartRequest.current = {version:active.revision,expectedRevision:video.revision,requestId:uid()};
        }
        void act('restart-download', restartRequest.current);
      }}>重新下载完整文件</button>}
      {active.downloadRequested && <button className="secondary" disabled={busy || recoveryRunning} onClick={() => void act('cancel-download', {version:active.revision,expectedRevision:video.revision,requestId:uid()})}>停止本条下载，保留断点</button>}
    </div></details>}
    {playingVersion && !clearing && <div className="player"><video controls playsInline preload="metadata" src={`/api/videos/${video.id}/versions/${playingVersion.revision}/stream`} onPlay={() => setReviewed(true)} />
      <p className="subtle">正在查看第{playingVersion.revision+1}版：{playingVersion.startTime} → {playingVersion.endTime}</p>
      {playingVersion.state === 'attention' && <p className="notice">此视频仅供核对，异常解除后才能确认下载。</p>}
      {playing !== active.revision && versionDownloadable(playingVersion) && <p><a className="version-download" download href={`/api/videos/${video.id}/versions/${playingVersion.revision}/download`}>下载正在查看的第 {playingVersion.revision + 1} 版</a></p>}
      {playing === active.revision && active.state === 'ready' && <><label className="review-check"><input type="checkbox" checked={reviewed} onChange={e => setReviewed(e.target.checked)} />我已查看并确认这一版视频</label><button disabled={!reviewed || !directory || busy} onClick={() => {void act('confirm', {version: active.revision, expectedRevision: video.revision, requestId: uid()});}}>确认这版并下载</button>{!directory && <p className="notice">请连接外置盘后再下载。</p>}</>}</div>}
    <VideoProcessing groups={processing} onRefresh={onRefresh}/>
    {canCorrect && <details className="correction"><summary>更正采集时间</summary>
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
  const [tab, setTab] = useState<'collect' | 'videos' | 'slicing'>(() => {
    if(new URLSearchParams(location.search).get('tab')==='videos')return 'videos';
    try {const saved=sessionStorage.getItem('simple-active-tab');
      if (saved === 'audit') return 'videos';
      if (saved === 'slicing') return saved;
      if (saved === 'videos') return saved;
    } catch { /* The initial page also works when local preferences are unavailable. */ }
    return 'collect';
  });
  useEffect(() => {try {sessionStorage.setItem('simple-active-tab',tab);} catch { /* Optional preference. */ }}, [tab]);
  const [session, setSession] = useState<Session>(emptySession);
  const [state, setState] = useState<VideoState>(emptyVideos);
  const [loaded,setLoaded]=useState(false),[sessionLoaded,setSessionLoaded]=useState(false);
  const [processing,setProcessing]=useState<ProcessingGroup[]>([]);
  const [focus,setFocus]=useState<{videoId?:string;mediaId?:string;key:string}|null>(()=>{
    const query=new URLSearchParams(location.search);
    return query.get('tab')==='videos'?{videoId:query.get('videoId')||undefined,mediaId:query.get('mediaId')||undefined,key:uid()}:null;
  });
  const focusDone=useRef('');
  const [jobs, setJobs] = useState<Job[]>([]);
  const [filters, setFilters] = useState({dateFrom: chinaToday(), dateTo: chinaToday(), timeFrom: '00:00:00', timeTo: '23:59:59', plates: ''});
  const [search, setSearch] = useState<Search | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const collectAction = useRef(collectionAction(api));
  const [admissionBlocked,setAdmissionBlocked] = useState(true);
  const [busy, setBusy] = useState('');
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [readError,setReadError]=useState('');
  const [videoFilters, setVideoFilters] = useState(blankVideoFilters);
  const [appliedVideoFilters, setAppliedVideoFilters] = useState(blankVideoFilters);
  const [videoSelection, setVideoSelection] = useState<Map<string,VideoSelection>>(new Map());
  const [clearScope, setClearScope] = useState('local');
  const [showBulkClear, setShowBulkClear] = useState(false);
  const [bulkErrors, setBulkErrors] = useState<string[]>([]);
  const bulkIntents = useRef(requestIntents());
  const actionLocked = useRef(false);
  const refreshSequence = useRef(0);
  const navigate=useCallback((next:'collect'|'videos'|'slicing')=>{
    setError('');setReadError('');setMessage('');setBulkErrors([]);setTab(next);
  },[]);
  const admissionChanged = useCallback((blocked: boolean) => {
    setAdmissionBlocked(blocked);
    if (blocked) {setSelected(new Set());}
  },[]);
  const refresh = useCallback(async () => {
    const sequence = ++refreshSequence.current;
    const query = new URLSearchParams(appliedVideoFilters);
    if (tab !== 'videos') query.set('summary', 'true');
    else query.set('compact','true');
    const [s, v, j, p] = await Promise.allSettled([api<Session>('/session'), api<VideoState>(`/videos?${query}`),
      tab === 'collect' || tab === 'videos' ? api<Job[]>('/jobs?summary=true') : Promise.resolve(null),
      tab==='videos'?api<{groups:ProcessingGroup[]}>('/video-processing'):Promise.resolve(null)]);
    if (sequence !== refreshSequence.current) return;
    const failure=[s,v,j,p].find(result=>result.status==='rejected');
    setReadError(failure?.status==='rejected'?`部分状态读取失败：${(failure.reason as Error).message}`:'');
    if(s.status==='fulfilled'){setSession(s.value);setSessionLoaded(true);}
    if(j.status==='fulfilled'&&j.value)setJobs(j.value);
    if(p.status==='fulfilled'&&p.value)setProcessing(p.value.groups);
    if(v.status==='fulfilled'){
      setState(v.value);
      setLoaded(true);
      setVideoSelection(before => syncVideoSelection(before, v.value.videos));
    }
  }, [appliedVideoFilters, tab]);
  useEffect(() => {void refresh().catch(() => undefined);}, [refresh]);
  useEffect(() => {
    const openSlicing = () => navigate('slicing');
    const openVideo=(event:Event)=>{
      const detail=(event as CustomEvent<{videoId?:string;mediaId?:string}>).detail;
      setFocus({...detail,key:uid()});setVideoFilters(blankVideoFilters);setAppliedVideoFilters(blankVideoFilters);navigate('videos');
    };
    window.addEventListener('simple-open-slicing', openSlicing);
    window.addEventListener('simple-open-video',openVideo);
    return () => {window.removeEventListener('simple-open-slicing', openSlicing);window.removeEventListener('simple-open-video',openVideo);};
  }, [navigate]);
  useEffect(()=>{
    if(tab!=='videos'||!focus||focusDone.current===focus.key)return;
    const timer=setTimeout(()=>{
      const target=document.getElementById(focus.mediaId?`processing-${focus.mediaId}`:`video-${focus.videoId}`);
      if(target instanceof HTMLDetailsElement)target.open=true;
      if(target)focusDone.current=focus.key;
      target?.scrollIntoView({behavior:'smooth',block:'center'});
    },150);
    return()=>clearTimeout(timer);
  },[focus,tab,state.videos,processing]);
  usePolling(refresh, tab === 'videos' ? 4000 : tab === 'collect' ? 10000 : 30000, {immediate:false});
  usePolling(async () => {if (search) setSearch(await api<Search>(`/search/${search.id}`));},
    search?.status === 'running' && tab === 'collect' ? 1500 : null, {immediate:false});
  async function run(action: string, fn: () => Promise<void>) {
    if(actionLocked.current)return;
    actionLocked.current=true;
    ++refreshSequence.current;
    setBusy(action); setError(''); setMessage('');
    try { await fn(); } catch (failure) { setError((failure as Error).message); }
    finally { ++refreshSequence.current;actionLocked.current=false;setBusy(''); }
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
    const next={...videoFilters,status};
    setVideoFilters(next);applyVideoFilters(next);
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
      await refresh();setSelected(new Set());navigate('videos');
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
  async function bulk(action: 'confirm' | 'remove' | 'classify-saved', explicit?: VideoSelection[]) {
    const selections = explicit || [...videoSelection.values()];
    if (!selections.length) return;
    if (action === 'confirm' && !window.confirm(`将选中的 ${selections.length} 条当前版本下载到 ${state.directory}？尚在采集的版本会等完成后保存；待核对项不会被批准。`)) return;
    await run(`bulk-${action}`, async () => {
      setBulkErrors([]);
      const result = await api<BulkResult>(`/videos/bulk-${action}`, bulkIntents.current.payload(action,{selections,deleteCloud:action==='remove'&&clearScope==='all',deleteLocalFiles:action==='remove'&&clearScope==='all'}));
      bulkIntents.current.complete(action);
      const good = result.outcomes.filter(o => o.ok);
      setMessage(`${good.length} 条${action==='classify-saved'?'已归入“已保存”':`已加入${action === 'confirm' ? '下载' : '清理'}队列`}${result.outcomes.length > good.length ? `，${result.outcomes.length - good.length} 条保持原状，原因见下方` : ''}。`);
      setBulkErrors(result.outcomes.filter(o => !o.ok).map(o => `${state.videos.find(v => v.id === o.id)?.name || o.id}：${o.message}`));
      setVideoSelection(before => {const next = new Map(before);good.forEach(o => next.delete(o.id));return next;});
      setShowBulkClear(false);await refresh();
    });
  }
  return <div className="app-shell"><header className="site-header"><div className="brand"><span className="brand-symbol">映</span><div><strong>简易版质检视频采集</strong><small>采集 · 更正 · 切片</small></div></div>
    <nav><button className={tab === 'collect' ? 'selected' : ''} onClick={() => navigate('collect')}>查询与批量采集</button><button className={tab === 'videos' ? 'selected' : ''} onClick={() => navigate('videos')}>视频查看与更正 <span>{loaded?state.totalCount:'…'}</span></button><button className={tab === 'slicing' ? 'selected' : ''} onClick={() => navigate('slicing')}>模型识别与切片</button></nav></header>
    <main><section className="hero compact"><div><h1>{tab==='slicing'?'模型识别与切片':tab==='collect'?'查询与批量采集':'视频查看与更正'}</h1><p>{tab==='slicing'?'识别片段、生成照片并上传。':tab==='collect'?'质检开始后 5 分钟采集，时长 60 分钟。':'查看、更正和下载；已完成的可用部分自动归入已保存。'}</p></div></section>
    <Workspace onChanged={refresh} />
    {(tab === 'collect' || tab === 'videos') && (sessionLoaded?<details className="connections-summary" open={!(session.source.connected&&session.target.connected)}><summary>{session.source.connected&&session.target.connected?'质检与采集平台已连接':'连接平台'}</summary><div className="connections"><Login service="source" connected={session.source.connected} user={session.source.user}
      savedAccount={session.source.savedAccount} hasSavedCredentials={session.source.hasSavedCredentials} credentialWarning={session.source.credentialWarning} onDone={refresh} />
      <Login service="target" connected={session.target.connected} user={session.target.user}
      savedAccount={session.target.savedAccount} hasSavedCredentials={session.target.hasSavedCredentials} credentialWarning={session.target.credentialWarning} onDone={refresh} /></div></details>:<p className="subtle" role="status">正在读取平台连接…</p>)}
    {(error||readError) && <p className="error global" role="alert">{error||readError}</p>}{message && <p className="success global" role="status">{message}</p>}
    {bulkErrors.length > 0 && <div className="notice"><strong>这些条目已保留</strong><ul>{bulkErrors.map((item,i) => <li key={i}>{item}</li>)}</ul></div>}
    <PageBoundary key={tab}><Suspense fallback={<section className="panel" role="status">正在打开页面…</section>}>
    {tab === 'slicing' ? <Slicing /> : tab === 'collect' ? <><section className="panel"><div className="section-title"><div><h2>筛选质检实绩</h2></div><span>按保存的准入条件区分已批准与未批准</span></div>
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
      : <><DownloadManager workers={state.downloadWorkers} directory={state.directory} checkError={state.checkError} onRefresh={refresh} onSave={async payload=>{
          const workers=await api<DownloadWorkers>('/videos/download-limit',payload);
          ++refreshSequence.current;
          setState(before=>({...before,downloadWorkers:workers}));
          return workers;
        }} />
        <section className="panel record-controls"><div className="section-title"><h2>采集记录</h2><span role="status">{loaded?`${state.filteredCount} / ${state.totalCount} 条`:'正在读取…'}</span></div>
          <VideoStatusFilters value={appliedVideoFilters.status} counts={state.counts} total={state.totalCount}
            disabled={!!busy||!loaded} onChange={filterVideoStatus} />
          <form className="record-filters condensed" onSubmit={e => {e.preventDefault();applyVideoFilters(videoFilters);}}>
            <label className="record-search">车牌或视频名称<input value={videoFilters.plate} maxLength={500} placeholder="例如 VIRTUAL_020，多个名称用逗号分隔" onChange={e => setVideoFilters({...videoFilters,plate:e.target.value})} /></label>
            <div className="actions"><button type="submit">查询记录</button><button type="button" className="secondary" onClick={() => {setVideoFilters(blankVideoFilters);applyVideoFilters(blankVideoFilters);}}>重置筛选</button></div>
            <details className="record-advanced"><summary>日期与更正筛选{(appliedVideoFilters.dateFrom||appliedVideoFilters.dateTo||appliedVideoFilters.correction!=='all'||appliedVideoFilters.timeBasis!=='quality')?' · 已启用':''}</summary><div className="record-filter-details">
              <label>日期依据<select value={videoFilters.timeBasis} onChange={e => setVideoFilters({...videoFilters,timeBasis:e.target.value})}><option value="quality">原始质检日期</option><option value="capture">当前版采集日期</option></select></label>
              <label>开始日期<input type="date" value={videoFilters.dateFrom} onChange={e => setVideoFilters({...videoFilters,dateFrom:e.target.value})} /></label>
              <label>结束日期<input type="date" value={videoFilters.dateTo} min={videoFilters.dateFrom || undefined} onChange={e => setVideoFilters({...videoFilters,dateTo:e.target.value})} /></label>
              <label>更正情况<select value={videoFilters.correction} onChange={e => setVideoFilters({...videoFilters,correction:e.target.value})}><option value="all">全部记录</option><option value="corrected">有更正历史</option><option value="withdrawn">最后操作为撤回</option><option value="uncorrected">从未更正</option></select></label>
            </div></details>
          </form>
          <RecordSelection videos={state.videos} selectedIds={new Set(videoSelection.keys())} busy={!!busy||!loaded} downloadEnabled={!!state.directory}
            onSelect={records=>{setShowBulkClear(false);setVideoSelection(new Map(records.map(v=>[v.id,{id:v.id,version:v.activeRevision,expectedRevision:v.revision}])));}}
            onClear={()=>{setVideoSelection(new Map());setShowBulkClear(false);}} onDownload={()=>void bulk('confirm')}
            onSave={()=>void bulk('classify-saved')} onRemove={()=>setShowBulkClear(!showBulkClear)} />
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
    {groupSubmissionDays(processing.filter(g=>!g.videoId||(g.id===focus?.mediaId&&!state.videos.some(v=>v.id===g.videoId))),g=>g.createdAt).map(day=><DateSection key={day.day} day={day.day} expandedKey={day.records.some(g=>g.id===focus?.mediaId)?focus?.key:undefined} summary={`${day.records.length} 个本地视频待处理`}>
      <div className="batch-video-grid">{day.records.map(group=><article className="video-card" key={group.id}><h3 className="local-processing-name">{group.name}</h3><p className="subtle">{group.videoId?'关联采集记录已隐藏':'本地视频 · 无关联质检记录'}</p><VideoProcessing groups={[group]} onRefresh={refresh} candidates={state.videos}/></article>)}</div>
    </DateSection>)}
    {!loaded?<section className="panel empty" role="status" aria-busy="true"><p>正在读取采集记录，已有进度保留。</p></section>:state.videos.length ? <VideoBatches videos={state.videos} jobs={jobs} totalByJob={state.byJob} selectedIds={new Set(videoSelection.keys())} focusedVideoId={focus?.videoId} focusKey={focus?.key}
      disabled={!!busy} downloadEnabled={!!state.directory} onSelectBatch={selectBatch} onDeleteRecords={deleteVideoRecords}
      onDownloadBatch={records => void bulk('confirm', records.map(video => ({id:video.id,version:video.activeRevision,expectedRevision:video.revision})))}>
      {video => <VideoCard video={video} processing={processing.filter(g=>g.videoId===video.id)} directory={state.directory} onRefresh={refresh} selected={videoSelection.has(video.id)} onSelect={() => pickVideo(video)} onRemoved={removalId=>{
        setRemovedVideo({id:video.id,name:video.name,removalId,restoreRequestId:uid()});
        setVideoSelection(before=>{const next=new Map(before);next.delete(video.id);return next;});
      }}/>}
    </VideoBatches> : <section className="panel empty"><h2>{state.totalCount ? '没有符合条件的记录' : state.hiddenRecordCount ? '当前没有可显示的记录' : '还没有采集记录'}</h2><p>{state.totalCount ? '可以调整日期、车牌和状态，或重置筛选。' : state.hiddenRecordCount ? '已删除记录的文件、更正和后台进度保留。' : '在“查询与批量采集”中提交任务后，这里会显示每辆车的采集进度。'}</p></section>}</div></>}
    </Suspense></PageBoundary></main><footer className="site-footer">本地运行 · 时间按北京时间显示 · 操作进度保存在本机</footer></div>;
}
