import RecordOperations,{useRecordChanges} from './RecordOperations';
import { lazy, Suspense, useCallback, useEffect, useRef, useState } from 'react';
import PageBoundary from './PageBoundary';
import {cancelledRead,createReadCycle,localJson} from './local-api';
import {refreshSections,stateSection} from './refresh-sections';
import {requestIntents} from './request-intent';
import {useMediaChanges} from './useMediaChanges';
import Workspace from './Workspace';
import DownloadManager from './DownloadManager';
import type { DownloadWorkers } from './DownloadManager';
import type { FormEvent, ReactNode } from 'react';
import { usePolling } from './usePolling';
import DateCollection,{CollectionSettings} from './DateCollection';
import { correctionInterval, correctionReviewCurrent, prepareCorrection } from './corrections';
import type { PreparedCorrection } from './corrections';
import { VideoBatches, VideoStatusFilters } from './RecordGroups';
import { canDownload as canConfirmFinal, captureStatusLabel, syncVideoSelection, visibleVideoSelections } from './record-groups';
import RecordSelection from './RecordSelection';
import './compact-controls.css';
import SubmissionRecords from './SubmissionRecords';
import RecordDelete from './RecordDelete';
import RecordBatch, {reconcileRecordChoices} from './RecordBatch';
import type {RecordChoice} from './RecordBatch';
import { partialComplete, recoveryInProgress, videoRecovery, versionDownloadable } from './video-recovery';
import type { CollectionJob } from './record-groups';
import VideoProcessing from './VideoProcessing';
import type { ProcessingGroup } from './VideoProcessing';
import DateSection from './DateSection';
import { groupSubmissionDays } from './submission-dates';
import {initialPage,initialVideoTarget,navigationSearch,readInterval,transferProgress} from './view-preferences';
import {platformStatus} from './platform-availability';
import type {PlatformAvailability} from './platform-availability';
import type {Page} from './view-preferences';

const loadSlicing = () => import('./Slicing');
const warmSlicing = () => {void loadSlicing().catch(()=>{/* Navigation retains its normal retry boundary. */});};
const Slicing = lazy(loadSlicing);
const VisualConnection = lazy(() => loadSlicing().then(module=>({default:module.VisualConnection})));

type SessionEntry = {availability?:PlatformAvailability;connected: boolean; user: string; hasSavedCredentials?: boolean; savedAccount?: string; credentialWarning?: string};
type Session = { source: SessionEntry; target: SessionEntry; capabilities?: {batchDownload?: boolean} };
type Job = CollectionJob;
type Version = {
  historyDownload?:{state:string;message?:string;receivedBytes?:number;totalBytes?:number};revision: number; startTime: string; endTime: string; durationSeconds: number; state: string;
  platformStatus?: string; taskId?: string; resultId?: string; errorCode?:string; message?: string; filePath?: string; sha256?:string; localCleanupState?:string;
  receivedBytes?: number; totalBytes?: number; attempts?: number; downloadRequested?: boolean; writeAttempted?: boolean;submissionOutcome?:string;
  etag?:string; recovery?:{state:string;resultVerified?:boolean;filePath?:string;slicingItemId?:string;successfulSamples?:number;skippedSamples?:number;
    message?:string;stage?:string;errorCode?:string;progress?:number;categories?:Record<string,{state?:string;message?:string;errorCode?:string;observations?:number;skippedSamples?:number;imageCount?:number;actualImages?:number;packageId?:string}>};
  review?: {code: string; message: string; actual?: {videoName?: string; startTime?: string; endTime?: string; taskStatus?: string} | null}};
type Video = {id: string; jobId: string; submittedAt?:string; plate: string; name: string; qualityStart: string; qualityEnd: string;
  revision: number; activeRevision: number; acceptedRevision: number | null; versions: Version[];
  operation?:{id:string;kind:string;state:string;version?:number;expectedRevision?:number;stage?:string;message?:string;slicingItemId?:string}; recovery?:Version['recovery'];
  finalProcessing?:{operationId:string;state:string;stage:string;message:string;manualPaused?:boolean;successfulSamples?:number;skippedSamples?:number;endReason?:string;
    stageLabel?:string;terminal?:boolean;progress?:{done:number;total:number;unit:string}|null;
    issue?:{code:string;stage?:string;message:string}|null;environmentBlocker?:{code:string;message:string}|null};
  capabilities?:{version:number;expectedRevision:number;canConfirmFinal:boolean;confirmFinalReason?:string;canRestartDownload?:boolean;restartDownloadReason?:string;canContinueProcessing?:boolean;canWithdraw?:boolean;withdrawReason?:string;canForceRecapture?:boolean;captureInFlight?:boolean;canTerminateCapture?:boolean};
  cleanupPending?: boolean; error?: string;
  deletion?: {state: string; message: string; requestId: string; expectedRevision: number; deleteCloud: boolean; deleteLocalFiles: boolean}};
type VideoState = {videos: Video[]; directory: string; lastCheck: string; checkError: string;
  hiddenRecordCount?:number; downloadWorkers?:DownloadWorkers;
  totalCount: number; filteredCount: number; byJob: Record<string,number>; counts: Record<string,number>};
type VideoSelection = {id: string; version: number; expectedRevision: number};
type BulkResult = {outcomes: {id: string; ok: boolean; message?: string}[]};
const blankVideoFilters = {plate: '', dateFrom: '', dateTo: '', timeBasis: 'quality', status: 'all', correction: 'all'};

async function api<T>(path: string, data?: unknown, signal?:AbortSignal): Promise<T> {
  return localJson<T>(`/api${path}`,data,{signal});
}

const emptySession: Session = {source: {connected: false, user: ''}, target: {connected: false, user: ''}};
const emptyVideos: VideoState = {videos: [], directory: '', lastCheck: '', checkError: '', totalCount: 0, filteredCount: 0, byJob: {}, counts: {}};
const uid = () => crypto.randomUUID();
const duration = (seconds: number) => `${Math.floor(seconds / 60)}分${seconds % 60 ? `${seconds % 60}秒` : ''}`;
const size = (bytes: number) => `${(bytes / 1024 ** 2).toFixed(1)} MB`;
function Login({service, connected, user, savedAccount = '', hasSavedCredentials = false, credentialWarning = '', availability, onDone}: {
  service: 'source' | 'target'; connected: boolean; user: string; savedAccount?: string;
  hasSavedCredentials?: boolean; credentialWarning?: string; availability?:PlatformAvailability; onDone: () => Promise<void>
}) {
  const connection=platformStatus(availability,connected);
  const [account, setAccount] = useState(savedAccount || user);
  const [password, setPassword] = useState('');
  const [remember, setRemember] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  async function submit(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError('');
    try { await api(`/session/${service}/login`, {account, password, remember}); setPassword(''); void onDone().catch(()=>setError('登录已完成，等待状态更新。')); }
    catch (failure) { setError((failure as Error).message); }
    finally { setBusy(false); }
  }
  return <div className="login-card"><div className="login-title"><strong>{service === 'source' ? '质检网站' : '采集平台'}</strong>
    <span className={connection.connected ? 'dot connected' : 'dot'} />{connection.label}{connection.authenticated&&user?` · ${user}`:''}</div>
    {!connection.authenticated && <form className="login-form" onSubmit={submit}><input aria-label={`${service}账号`} placeholder="账号" value={account} onChange={e => setAccount(e.target.value)} required />
      <input aria-label={`${service}密码`} type="password" placeholder={hasSavedCredentials ? '输入密码以更新本机配置' : '密码'} value={password} onChange={e => setPassword(e.target.value)} required />
      <label className="remember-password"><input type="checkbox" checked={remember} onChange={e => setRemember(e.target.checked)} />保存登录</label>
      <button disabled={busy}>{busy ? '连接中…' : '连接'}</button></form>}
    {hasSavedCredentials && !connection.authenticated && <p className="hint">已安全保存账号 {savedAccount}；登录后会更新保存项。</p>}
    {credentialWarning && <p className="error">{credentialWarning}</p>}{error && <p className="error">{error}</p>}</div>;
}


function VideoCard({video,onSelect,selected,onOpen,children}:{
  video:Video;onSelect?:()=>void;selected?:boolean;onOpen:()=>void;children?:ReactNode;
}){
  const trigger=useRef<HTMLButtonElement>(null);
  const expanded=!!children;
  const active=video.versions.find(v=>v.revision===video.activeRevision)!;
  const clearing=['pending','error'].includes(video.deletion?.state||'');
  const label=video.finalProcessing?.stageLabel||(partialComplete(active)?'可用部分已保存':video.finalProcessing?.state==='complete'?'处理完成':captureStatusLabel(active));
  const progress=video.finalProcessing?.terminal?null:video.finalProcessing?.progress;
  return <article className={`video-card${expanded?' is-expanded':''}`} id={`video-${video.id}`}>
    <div className="video-head"><label className="record-pick">{onSelect&&<input type="checkbox" aria-label={`选择采集记录 ${video.name} ${video.qualityStart}`} checked={selected} disabled={clearing} onChange={onSelect}/>}<h3>{video.name}</h3></label><span className={`badge ${video.finalProcessing?.state||active.state}`}>{label}</span></div>
    <p className="record-time">{active.startTime}<br/>至 {active.endTime}</p><p className="subtle">{duration(active.durationSeconds)} · 第{active.revision+1}版</p>
    {progress&&<div className="pipeline-progress"><progress aria-label={`${video.name} ${label}`} value={progress.done} max={progress.total}/><small>{progress.unit==='bytes'?transferProgress(progress.done,progress.total):`${progress.done}/${progress.total} ${progress.unit}`}</small></div>}
    <button ref={trigger} className="secondary video-open-detail" aria-expanded={expanded}
      aria-controls={`video-details-${video.id}`} onClick={()=>{onOpen();if(expanded)trigger.current?.focus({preventScroll:true});}}>{expanded?'收起详情':'查看与更正'}</button>
    {children&&<div id={`video-details-${video.id}`} className="inline-video-details">{children}</div>}
  </article>;
}

function VideoPreview({ src, onPlay }: { src: string; onPlay: () => void }) {
  const ref = useRef<HTMLVideoElement>(null);
  useEffect(() => {
    const player = ref.current;
    if (!player) return;
    if (player.getAttribute('src') !== src) player.setAttribute('src', src);
    return () => {
      player.pause();
      player.removeAttribute('src');
      player.load();
    };
  }, [src]);
  return <video ref={ref} controls playsInline preload="metadata" src={src} onPlay={onPlay} />;
}

function VideoDetails({video,onRefresh,onRemoved,processing}:{
  video:Video;onRefresh:()=>Promise<void>;onRemoved:(id:string)=>void;processing:ProcessingGroup[];
}){
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
  const recaptureIntent = useRef(requestIntents());
  const operationLocked = useRef(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [operationNotice,setOperationNotice]=useState('');
  const mounted=useRef(true);
  useEffect(()=>{mounted.current=true;return()=>{mounted.current=false;};},[]);
  useEffect(() => {
    if (!dirty) {
      setDate(active.startTime.slice(0, 10)); setClock(active.startTime.slice(11, 19));
      setMinutes(String(active.durationSeconds / 60)); setDraftRevision(video.revision);
    }
    setPrepared(null);
  }, [video.activeRevision, video.revision]);
  // Polling revisions reflect progress too; only a different capture closes the player.
  useEffect(() => {setReviewed(false);setPlaying(null);}, [video.activeRevision]);
  // A withdrawn version must release its stream so it cannot block its own cleanup.
  useEffect(() => {
    if (active.state === 'withdrawing') setPlaying(current => current === active.revision ? null : current);
  }, [active.state, active.revision]);
  useEffect(() => {
    try {
      if (dirty) sessionStorage.setItem(draftKey, JSON.stringify({date,clock,minutes,revision:draftRevision,dirty:true}));
      else sessionStorage.removeItem(draftKey);
    } catch { /* Editing still works when browser storage is unavailable. */ }
  }, [date,clock,minutes,draftRevision,dirty,draftKey]);
  let draftEnd = '';
  try { draftEnd = correctionInterval({date,clock,minutes}).endTime; } catch { /* handled on review */ }
  const clearing = ['pending','error'].includes(video.deletion?.state || '');
  const recoveryOptions = videoRecovery(active);
  const recovery = video.recovery || active.recovery;
  const recoveryRunning = recoveryInProgress(video) || ['queued','moving','slicing'].includes(recovery?.state || '');
  const recoveryPaused = recovery?.state === 'paused'
    || (video.operation?.kind === 'recover-decodable' && video.operation.state === 'paused');
  const recoveryProofReady=!!active.downloadRequested&&!!active.totalBytes&&!!active.etag&&!active.etag.startsWith('W/');
  const canPartialRecover = recoveryOptions.failedMedia && ['invalid_video','media_incomplete'].includes(active.errorCode || '')
    && recoveryProofReady && !recoveryRunning && !recoveryPaused && !clearing
    && (!recovery || recovery.state === 'attention');
  const canCorrect = (['ready','saved','waiting','submitted','attention','failed'].includes(active.state)
      || (!!video.finalProcessing&&['downloading','publishing'].includes(active.state)))
    && (!active.downloadRequested || active.state === 'failed' || !!video.finalProcessing) && (!active.writeAttempted || !!active.taskId || active.submissionOutcome==='rejected') && !clearing && (!recoveryRunning || !!video.finalProcessing);
  const withdrawing=video.operation?.kind==='withdraw'&&['queued','running','reconciling','waiting_connection'].includes(video.operation.state);
  const canWithdraw=video.capabilities?.canWithdraw===true&&!withdrawing;
  const recapturing=video.operation?.kind==='recapture'&&['queued','running','reconciling','waiting_connection'].includes(video.operation.state);
  const canRecapture=video.capabilities?.canForceRecapture||recoveryOptions.canRecapture;
  const hasMoreActions=['attention','uncertain','waiting','failed'].includes(active.state)||canWithdraw||recoveryOptions.canRestartDownload||active.downloadRequested||canRecapture;
  const staleDraft = draftRevision !== video.revision;
  const playable = video.versions.filter(v => v.taskId && v.resultId && ['ready','saved','failed','publishing','attention'].includes(v.state)
    && !['result_missing','result_identity_changed','multiple_results'].includes(v.review?.code || ''));
  const playingVersion = video.versions.find(v => v.revision === playing);
  async function act(path: string, body: object) {
    if(operationLocked.current)return false;
    operationLocked.current=true;
    setBusy(true); setError('');setOperationNotice('');
    try {
      const result = await api<Partial<Video>&{recordRemoval?:{removalId:string};operationId?:string}>(`/videos/${video.id}/${path}`, body);
      if(!mounted.current)return true;
      setOperationNotice(result.operationId?'操作已登记，后台继续处理。':'操作已完成，正在更新状态。');
      if (path === 'corrections' || path === 'recapture') {setDirty(false);setPrepared(null);setDraftRevision(video.revision);}
      if (path === 'withdraw') setPlaying(current => current === active.revision ? null : current);
      if (path === 'hide-record' && result.recordRemoval) {
        onRemoved(video.id);
        return true; // The parent clears the focus and refreshes the remaining records.
      }
      void onRefresh().catch(()=>{if(mounted.current)setOperationNotice('操作已登记，等待状态更新。');});
      return true;
    }
    catch (failure) {if(mounted.current)setError((failure as Error).message);return false; }
    finally {operationLocked.current=false;if(mounted.current)setBusy(false); }
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
  function restartFullDownload() {
    if(!video.capabilities?.canRestartDownload)return;
    if (!window.confirm(`重新下载 ${video.name} 第 ${active.revision + 1} 版？只清除这一版的下载断点，重新获取完整视频；其他版本和更正均保留。`)) return;
    if (!restartRequest.current || restartRequest.current.version !== active.revision || restartRequest.current.expectedRevision !== video.revision) {
      restartRequest.current = {version:active.revision,expectedRevision:video.revision,requestId:uid()};
    }
    void act('restart-download', restartRequest.current);
  }
  async function recaptureCurrent() {
    if (!window.confirm(`按当前区间重新采集：${active.startTime} → ${active.endTime}。旧版本保留，是否提交？`)) return;
    const payload = recaptureIntent.current.payload('recapture', {
      version: active.revision, expectedRevision: video.revision,
      ...(video.capabilities?.canForceRecapture ? {mode: 'force'} : {}),
    });
    if (await act('recapture', payload)) recaptureIntent.current.complete('recapture');
  }
  const saved = video.versions.find(version => version.revision === video.acceptedRevision);
  const historical = video.versions.filter(version => version.revision !== active.revision && version.state !== 'withdrawn');
  const renderVersion = (version: Version) => <div key={version.revision} className="version-entry"><div className={version.revision === active.revision ? 'version current' : 'version'}>
    <span>第 {version.revision + 1} 版{version.revision === 0 ? ' · 初采' : ' · 手动更正'}</span>
    <strong>{version.startTime} → {version.endTime}</strong><span>{duration(version.durationSeconds)}</span>
    <span>{partialComplete(version)?'原片不完整 · 可用部分已保存':captureStatusLabel(version)}</span>
    </div>{version.revision !== active.revision && <div className="version-actions">
      {version.historyDownload?.state==='complete'?<span className="success">已保存历史版本</span>:version.historyDownload&&['queued','running','waiting'].includes(version.historyDownload.state)?<span role="status">{version.historyDownload.message||'正在保存历史版本…'}</span>:versionDownloadable(version) && !clearing ? <button className="secondary" disabled={busy} onClick={()=>void act('history-download',{version:version.revision,expectedRevision:video.revision,requestId:uid()})}>保存第 {version.revision + 1} 版</button>
        : <span className="subtle">{version.state === 'cloud_deleted' ? '此版已清理，无法下载' : '此版结果未确认，暂不能下载'}</span>}
    </div>}</div>;
  return <section className="video-details-body">
    <p className="subtle">原始质检：{video.qualityStart} 至 {video.qualityEnd}</p>
    {video.operation&&['withdraw','recapture','terminate-capture'].includes(video.operation.kind)&&
      <p className={['attention','conflict'].includes(video.operation.state)?'notice':'subtle'} role="status">{video.operation.message}</p>}
    <div className="video-versions">{renderVersion(active)}
      {!!historical.length && <details className="version-history"><summary>历史版本（{historical.length}）</summary>
        <p className="subtle">历史版本保存在数据文件夹，不改变当前版本。</p>{historical.map(renderVersion)}</details>}
    </div>
    {active.state==='pending'&&!active.taskId&&!active.message&&
      <p className="notice">这条任务尚未发送到采集平台。可在“日期采集”的采集提交记录中查看本批进度。</p>}
    {recoveryRunning && <div className="recovery-progress" role="status"><strong>{recovery?.stage==='photos_paused'?'等待继续照片上传':recovery?.stage==='photos_processing'?'照片上传与导入中':video.operation?.stage === 'waiting_connection' ? '等待平台连接' : '可用部分处理中'}</strong>
      <p>{recovery?.message || video.operation?.message || '正在识别可解码画面。'}</p>
      {recovery?.slicingItemId && <button className="link" onClick={() => window.dispatchEvent(new CustomEvent('simple-open-slicing'))}>查看切片进度</button>}
      <button className="secondary" disabled={busy} onClick={() => void act('recover-decodable/pause', {version:active.revision,expectedRevision:video.revision,requestId:uid()})}>暂停此条</button>
    </div>}
    {recovery && !recoveryRunning && <div className={`recovery-progress ${recovery.state === 'attention' ? 'needs-review' : ''}`}>
      <strong>{partialComplete(active) ? '可用部分已保存' : recovery.state === 'attention' ? '可用部分需处理' : '部分视频处理状态'}</strong>
      <p>{recovery.message || '坏位置已跳过；原视频仍标记为不完整。'}</p>
      <span>有效判断 {recovery.successfulSamples || 0} · 跳过位置 {recovery.skippedSamples || 0}</span>
      {recoveryPaused && <button disabled={busy} onClick={() => void act('recover-decodable/resume', {version:active.revision,expectedRevision:video.revision,requestId:uid()})}>继续此条</button>}
      {recovery.state==='attention'&&!recoveryProofReady&&recoveryOptions.failedMedia&&<p>当前版本缺少可恢复的下载凭证，请重新下载或更正采集时间。</p>}
      {recovery.state==='attention'&&!recoveryProofReady&&recoveryOptions.failedMedia&&<button className="secondary" disabled={busy||!video.capabilities?.canRestartDownload} title={video.capabilities?.restartDownloadReason} onClick={restartFullDownload}>重新下载完整文件</button>}
    </div>}
    {!recoveryRunning && !recovery && !recoveryOptions.failedMedia && active.message && active.state !== 'attention' && <p className="notice">{active.message}</p>}
    {recoveryOptions.failedMedia && !recoveryRunning && !recovery && <div className="notice"><strong>视频末段缺失或无法解码</strong>
      <p>{recoveryProofReady?'可复用已下载文件，处理可解码部分；坏位置跳过。':'当前版本缺少可恢复的下载凭证，请重新下载或更正采集时间。'}</p>
      {!recoveryProofReady&&recoveryOptions.canRestartDownload&&<button className="secondary" disabled={busy || !video.capabilities?.canRestartDownload} title={video.capabilities?.restartDownloadReason} onClick={restartFullDownload}>重新下载完整文件</button>}</div>}
    {recoveryOptions.checkpointProblem && <div className="notice"><strong>断点校验未通过</strong><p>请在“更多操作”中核对平台任务或重新获取完整文件；历史版本和进度保留。</p></div>}
    {active.state === 'attention' && !recoveryOptions.failedMedia && !recovery && <div className="notice"><strong>需要核对</strong><p>{({task_missing:'先重新核对这一条；任务仍不存在时，可按保留的更正区间重新采集。',task_identity_changed:'平台任务信息已变化，原绑定保留；请先核对平台实际时间。',result_missing:'先核对平台结果ID；也可查看可用旧版。',platform_failed:'平台采集失败或终止，可核对时间后重新采集。',result_identity_changed:'平台结果ID不同，请在原平台核对确切视频；原绑定已保留。',multiple_results:'平台返回多个视频，需要人工选择正确结果。',submission_ambiguous:'提交后出现多个相同任务，请核对确切任务ID，暂勿重新提交。'} as Record<string,string>)[active.review?.code || ''] || active.message || '请核对本条的任务和结果绑定。'}</p>
      {active.review?.actual && <p>平台实际：{active.review.actual.videoName} · {active.review.actual.startTime} → {active.review.actual.endTime} · {active.review.actual.taskStatus}</p>}</div>}
    {video.finalProcessing&&<p className="notice" role="status">{video.finalProcessing.issue?.message||video.finalProcessing.message}{!!video.finalProcessing.skippedSamples&&` · 跳过 ${video.finalProcessing.skippedSamples} 个坏位置`}
      {video.finalProcessing.issue&&video.finalProcessing.environmentBlocker?.message&&<><br/><span>{video.finalProcessing.environmentBlocker.message}</span></>}</p>}
    {video.error && !active.message && <p className="notice">{video.error}</p>}
    {active.state === 'downloading' && <p className="notice">已下载 {size(active.receivedBytes || 0)} / {size(active.totalBytes || 0)} · 第 {active.attempts || 1} 次尝试</p>}
    {saved?.filePath && <p className="subtle">{saved.localCleanupState==='complete'?'本地文件已清理，云端成果保留。':'本地文件已保存。'}</p>}
    {video.cleanupPending && <p className="notice">新版已保存，旧版仍在核对清理。</p>}
    {!clearing && <div className="actions primary-video-actions">
      {video.capabilities?.canContinueProcessing&&<button disabled={busy} onClick={()=>void act('continue-processing',{
        version:active.revision,expectedRevision:video.revision,requestId:uid()})}>继续本条处理</button>}
      {playable.some(v => v.revision === active.revision) && <button className="secondary" onClick={() => {setPlaying(playing === active.revision ? null : active.revision); setReviewed(false);}}>{playing === active.revision ? '关闭视频' : '查看视频'}</button>}
      {canPartialRecover && <button disabled={busy} onClick={() => void act('recover-decodable', {expectedRevision:video.revision,requestId:uid()})}>处理可解码部分</button>}
      {playable.some(v => v.revision !== active.revision) && <select aria-label={`查看${video.name}的其他版本`} value={playing ?? ''} onChange={e => {setPlaying(e.target.value === '' ? null : Number(e.target.value));setReviewed(false);}}><option value="">查看其他可用版本</option>{playable.map(v => <option key={v.revision} value={v.revision}>第{v.revision+1}版 · {v.startTime}</option>)}</select>}
    </div>}
    {!clearing && hasMoreActions && <details className="more-video-actions"><summary>重试与版本操作</summary><div className="actions">
      {['attention','uncertain','waiting','failed'].includes(active.state) && <button className="secondary" disabled={busy || recoveryRunning} onClick={() => {void act('check', {expectedRevision:video.revision,requestId:uid()});}}>重新核对这一条</button>}
      {canRecapture && !recoveryRunning && <button disabled={busy||recapturing||withdrawing} onClick={()=>void recaptureCurrent()}>按当前区间重新采集</button>}
      {canWithdraw && !recoveryRunning && <button className="secondary" disabled={busy} onClick={() => {void act('withdraw', {version: active.revision, expectedRevision: video.revision, requestId: uid()});}}>{active.state === 'withdrawing' ? '重试撤回' : '撤回本次更正'}</button>}
      {active.state === 'failed' && active.downloadRequested && !recoveryRunning && !recoveryPaused && !partialComplete(active) && <button disabled={busy} onClick={() => {void act('retry-download', {});}}>重试下载</button>}
      {recoveryOptions.canRestartDownload && (recoveryProofReady||partialComplete(active)||recoveryOptions.checkpointProblem) && <button className="secondary" disabled={busy || !video.capabilities?.canRestartDownload || recoveryRunning} title={video.capabilities?.restartDownloadReason} onClick={restartFullDownload}>重新下载完整文件</button>}
      {active.downloadRequested && !recoveryRunning && !recoveryPaused && <button className="secondary" disabled={busy || recoveryRunning} onClick={() => void act('cancel-download', {version:active.revision,expectedRevision:video.revision,requestId:uid()})}>停止本条下载，保留断点</button>}
    </div></details>}
    {playingVersion && !clearing && <div className="player"><VideoPreview key={playingVersion.revision} src={`/api/videos/${video.id}/versions/${playingVersion.revision}/stream`} onPlay={() => setReviewed(true)} />
      <p className="subtle">正在查看第{playingVersion.revision+1}版：{playingVersion.startTime} → {playingVersion.endTime}</p>
      {playingVersion.state === 'attention' && <p className="notice">此视频仅供核对，异常解除后才能确认下载。</p>}
      {playing !== active.revision && versionDownloadable(playingVersion) && <p><button className="secondary" disabled={busy} onClick={()=>void act('history-download',{version:playingVersion.revision,expectedRevision:video.revision,requestId:uid()})}>保存第 {playingVersion.revision + 1} 版</button></p>}
      {playing === active.revision && !video.finalProcessing && <><label className="review-check"><input type="checkbox" checked={reviewed} onChange={e => setReviewed(e.target.checked)} />我已查看并确认这一版视频</label><button disabled={!reviewed || !canConfirmFinal(video) || busy} title={video.capabilities?.confirmFinalReason} onClick={() => {void act('confirm-and-process', {version: active.revision, expectedRevision: video.revision, requestId: uid()});}}>确认最终区间并处理</button>{!canConfirmFinal(video)&&<p className="notice">{video.capabilities?.confirmFinalReason||'正在核对本版是否可以确认。'}</p>}</>}</div>}
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
          {prepared && <button type="button" disabled={busy || !correctionReviewCurrent(prepared,video.revision)} onClick={confirmCorrection}>确认重新采集</button>}
          {(dirty||prepared)&&<button type="button" className="secondary" disabled={busy} onClick={()=>{
            setDate(active.startTime.slice(0,10));setClock(active.startTime.slice(11,19));setMinutes(String(active.durationSeconds/60));
            setDirty(false);setPrepared(null);setDraftRevision(video.revision);setError('');
          }}>取消草稿</button>}</div>
      </form></details>}

    {video.capabilities?.canTerminateCapture?<RecordDelete name={video.name} version={`${video.revision}:${active.revision}`} disabled={busy}
      label="终止采集并删除记录" confirmLabel="确认终止并删除"
      description="将终止当前平台任务，核实停止后删除该任务和网页记录。已经生成视频结果时，记录会保留。"
      onDelete={requestId=>act('terminate-capture',{version:active.revision,expectedRevision:video.revision,requestId})}/>:
      !video.capabilities?.captureInFlight&&<RecordDelete name={video.name} version={video.revision} disabled={busy}
        onDelete={requestId=>act('hide-record',{expectedRevision:video.revision,requestId})}/>}
    {operationNotice&&<p className="subtle" role="status">{operationNotice}</p>}
    {error && <p className="error" role="alert">{error}</p>}
  </section>;
}

export default function App() {
  const [tab, setTab] = useState<Page>(() => {
    let saved=null;
    try {saved=sessionStorage.getItem('simple-active-tab');}catch{/* Optional preference. */}
    return initialPage(location.search,saved);
  });
  useEffect(() => {try {sessionStorage.setItem('simple-active-tab',tab);} catch { /* Optional preference. */ }}, [tab]);
  const [openDetails,setOpenDetails]=useState<Map<string,Video>>(()=>new Map());
  const detailReads=useRef(createReadCycle());
  const [session, setSession] = useState<Session>(emptySession);
  const [state, setState] = useState<VideoState>(emptyVideos);
  const [loaded,setLoaded]=useState(false),[sessionLoaded,setSessionLoaded]=useState(false);
  const [processing,setProcessing]=useState<ProcessingGroup[]>([]);
  const [localSelection,setLocalSelection]=useState<Map<string,RecordChoice>>(new Map());
  const [focus,setFocus]=useState<{videoId?:string;mediaId?:string;key:string}|null>(()=>{
    const target=initialVideoTarget(location.search);
    return target?{...target,key:uid()}:null;
  });
  const focusDone=useRef('');
  const focusElement=useRef<HTMLElement|null>(null);
  const focusStarted=useRef<{key:string;at:number}|null>(null);
  const [focusError,setFocusError]=useState('');
  const [focusedRecord,setFocusedRecord]=useState<{key:string;video:Video}|null>(null);
  const [focusLoading,setFocusLoading]=useState(false);
  const focusReads=useRef(createReadCycle());
  const clearFocus=useCallback(()=>{
    focusReads.current.cancel();setFocus(null);setFocusedRecord(null);setFocusError('');setFocusLoading(false);
    focusDone.current='';focusElement.current=null;focusStarted.current=null;
    try {
      if(initialVideoTarget(location.search))history.replaceState(history.state,'',location.pathname+navigationSearch(location.search,'videos')+location.hash);
    }catch{/* Closing a detail still works when history is unavailable. */}
  },[]);
  const [jobs, setJobs] = useState<Job[]>([]);
  const [busy, setBusy] = useState('');
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [readError,setReadError]=useState('');
  const [videoFilters, setVideoFilters] = useState(blankVideoFilters);
  const [appliedVideoFilters, setAppliedVideoFilters] = useState(blankVideoFilters);
  const [videoSelection, setVideoSelection] = useState<Map<string,VideoSelection>>(new Map());
  const [selectingVideos,setSelectingVideos]=useState(false);
  const [bulkErrors, setBulkErrors] = useState<string[]>([]);
  const bulkIntents = useRef(requestIntents());
  const actionLocked = useRef(false);
  const refreshSequence = useRef(0);
  const reads=useRef(createReadCycle());
  const tabRef=useRef<Page>(tab),viewVisit=useRef(0);
  const navigate=useCallback((next:Page)=>{
    if(next===tabRef.current)return;
    // A detail jump belongs to that visit. Ordinary navigation back must not
    // replay an old scroll intent; a new detail action supplies a fresh key.
    if(tabRef.current==='videos'){
      detailReads.current.cancel();
      setOpenDetails(new Map());
      clearFocus();
    }
    reads.current.cancel();
    tabRef.current=next;++viewVisit.current;++refreshSequence.current;
    // Reloading a bookmark must retain the tab the user actually navigated to.
    try {history.replaceState(history.state,'',location.pathname+navigationSearch(location.search,next)+location.hash);}catch{/* Navigation still works if history is unavailable. */}
    setError('');setReadError('');setMessage('');setBulkErrors([]);setVideoSelection(new Map());setSelectingVideos(false);setLocalSelection(new Map());setTab(next);
  },[clearFocus]);
  const refresh = useCallback(async () => {
    if (tab !== tabRef.current) return;
    const page = tab;
    const query = new URLSearchParams(appliedVideoFilters);
    if (tab !== 'videos') query.set('summary', 'true');
    else query.set('compact','true');
    await reads.current.run(`${page}:${query}`,async context=>{
      const sequence=++refreshSequence.current;
      const current=()=>context.current()&&sequence===refreshSequence.current&&page===tabRef.current;
      const sections=[];
      if(tab!=='slicing')sections.push(stateSection('平台连接',()=>api<Session>('/session',undefined,context.signal),value=>{setSession(value);setSessionLoaded(true);}));
      if(tab==='settings'||tab==='videos'){
        sections.push(stateSection('视频记录',()=>api<VideoState>(`/videos?${query}`,undefined,context.signal),value=>{
          if(page==='videos'){
            setState(value);setLoaded(true);setVideoSelection(before=>syncVideoSelection(before,value.videos));
          }else{
            // Collection summaries carry no video rows; keep the last full view.
            setState(before=>({...value,videos:before.videos}));
          }
        }));
        if(tab==='videos')sections.push(stateSection('提交记录',()=>api<Job[]>('/jobs?summary=true',undefined,context.signal),setJobs));
      }
      if(tab==='videos')sections.push(stateSection('处理进度',()=>api<{groups:ProcessingGroup[]}>('/video-processing',undefined,context.signal),value=>setProcessing(value.groups)));
      await refreshSections(sections,current,errors=>setReadError(errors.length?`部分状态读取失败：${errors.join('；')}`:''));
    });
  }, [appliedVideoFilters, tab]);
  const refreshAfterChange=useCallback(async()=>{
    reads.current.cancel();++refreshSequence.current;
    await refresh();
  },[refresh]);
  useEffect(() => {void refresh().catch(() => undefined);return()=>reads.current.cancel();}, [refresh]);
  useEffect(() => {
    const openSlicing = () => navigate('slicing');
    const openVideo=(event:Event)=>{
      const detail=(event as CustomEvent<{videoId?:string;mediaId?:string}>).detail;
      setFocusError('');setFocus({...detail,key:uid()});setVideoFilters(blankVideoFilters);
      if(JSON.stringify(appliedVideoFilters)!==JSON.stringify(blankVideoFilters))setLoaded(false);
      setAppliedVideoFilters(blankVideoFilters);navigate('videos');
    };
    window.addEventListener('simple-open-slicing', openSlicing);
    window.addEventListener('simple-open-video',openVideo);
    return () => {window.removeEventListener('simple-open-slicing', openSlicing);window.removeEventListener('simple-open-video',openVideo);};
  }, [navigate,appliedVideoFilters]);
  const focusedVideoId=focus?.videoId||processing.find(group=>group.id===focus?.mediaId)?.videoId;
  const refreshFocused=useCallback(async()=>{
    if(tab!=='videos'||!focusedVideoId||!focus||tabRef.current!=='videos')return;
    const key=focus.key;
    await focusReads.current.run(`${key}:${focusedVideoId}`,async context=>{
      if(context.current())setFocusLoading(true);
      try{
        const result=await api<{video:Video}>(`/videos/${encodeURIComponent(focusedVideoId)}`,undefined,context.signal);
        if(!context.current()||tabRef.current!=='videos')return;
        if(result.video?.id!==focusedVideoId)throw new Error('返回的记录身份不一致，未打开其他视频。');
        setFocusedRecord({...result,key});setFocusError('');
      }catch(failure){
        if(context.current()&&!cancelledRead(failure)){
          setFocusError((failure as Error).message);
          if((failure as Error&{status?:number}).status===404)setFocusedRecord(null);
        }
      }finally{if(context.current())setFocusLoading(false);}
    });
  },[focus?.key,focusedVideoId,tab]);
  useEffect(()=>{void refreshFocused();return()=>focusReads.current.cancel();},[refreshFocused]);
  const externalFocus=tab==='videos'&&focus&&focusedRecord?.key===focus.key&&focusedRecord.video.id===focusedVideoId
    &&!state.videos.some(video=>video.id===focusedVideoId)?focusedRecord:null;
  const localGroups=processing.filter(group=>!group.videoId);
  const localChoices=localGroups.flatMap(group=>group.items.map(item=>({key:`item:${item.id}`,id:item.id,revision:item.revision,kind:'item' as const})));
  useEffect(()=>{setLocalSelection(previous=>reconcileRecordChoices(previous,
    processing.filter(group=>!group.videoId).flatMap(group=>group.items.map(item=>({key:`item:${item.id}`,id:item.id,revision:item.revision,kind:'item' as const})))));},[processing]);
  const focusedActive=!!externalFocus&&(['queued','running'].includes(externalFocus.video.finalProcessing?.state||'')
    ||['downloading','publishing','submitting','terminating'].includes(externalFocus.video.versions.find(version=>version.revision===externalFocus.video.activeRevision)?.state||''));
  useRecordChanges(refresh);
  useMediaChanges(()=>{reads.current.cancel();void refresh();});
  usePolling(refreshFocused,externalFocus?readInterval(focusedActive,externalFocus.video.finalProcessing?.environmentBlocker?.code==='disk'):null,{immediate:false});
  useEffect(()=>{
    if(tab!=='videos'||!focus||focusError)return;
    let stopped=false, timer:ReturnType<typeof setTimeout>;
    if(focusStarted.current?.key!==focus.key)focusStarted.current={key:focus.key,at:Date.now()};
    const openTarget=()=>{
      if(stopped)return;
      const target=(focus.mediaId?document.getElementById(`processing-${focus.mediaId}`):null)
        ||(focusedVideoId?document.getElementById(`video-${focusedVideoId}`):null);
      if(target){
        // Slow independent responses can relocate a group into its collection card.
        // Reopen a replacement node once, without reopening a manually closed existing node.
        if(focusDone.current===focus.key&&focusElement.current===target)return;
        for(let parent=target.parentElement;parent;parent=parent.parentElement)
          if(parent instanceof HTMLDetailsElement)parent.open=true;
        if(target instanceof HTMLDetailsElement)target.open=true;
        const detail=target.querySelector<HTMLDetailsElement>(':scope > .video-card-details');
        if(detail)detail.open=true;
        const chosen=state.videos.find(v=>v.id===focusedVideoId)
          ||(focusedRecord?.key===focus.key&&focusedRecord.video.id===focusedVideoId?focusedRecord.video:undefined);
        if(chosen)setOpenDetails(before=>before.has(chosen.id)?before:new Map(before).set(chosen.id,chosen));
        focusDone.current=focus.key;focusElement.current=target;setFocusError('');target.scrollIntoView({behavior:'smooth',block:'center'});return;
      }
      if(!focusLoading&&Date.now()-(focusStarted.current?.at||Date.now())>=10000){setFocusError('没有找到对应的视频处理记录，请重新读取本条详情。');return;}
      timer=setTimeout(openTarget,120);
    };
    openTarget();
    return()=>{stopped=true;clearTimeout(timer);};
  },[focus,tab,state.videos,processing,focusedVideoId,focusedRecord,focusLoading,focusError]);
  const visibleIds=new Set(state.videos.map(video=>video.id));
  const retainedDetails=[...openDetails.values()].filter(video=>!visibleIds.has(video.id)&&video.id!==externalFocus?.video.id);
  useEffect(()=>{
    setOpenDetails(before=>{
      if(!before.size)return before;
      const next=new Map(before);
      let changed=false;
      for(const video of state.videos){
        if(!before.has(video.id)||before.get(video.id)===video)continue;
        next.set(video.id,video);
        changed=true;
      }
      return changed?next:before;
    });
  },[state.videos]);
  useEffect(()=>()=>detailReads.current.cancel(),[]);
  usePolling(async()=>{
    await detailReads.current.run(JSON.stringify(retainedDetails.map(video=>video.id)),async context=>{
      await Promise.allSettled(retainedDetails.map(async video=>{
        let latest:Video|undefined;
        try{
          const result=await api<{video:Video}>(`/videos/${encodeURIComponent(video.id)}`,undefined,context.signal);
          if(result.video.id!==video.id)return;
          latest=result.video;
        }catch(failure){
          if((failure as {status?:number}).status!==404)return;
        }
        if(!context.current())return;
        setOpenDetails(before=>{
          if(before.get(video.id)!==video)return before;
          const next=new Map(before);
          if(latest)next.set(video.id,latest);
          else next.delete(video.id);
          return next;
        });
      }));
    });
  },tab==='videos'&&retainedDetails.length?4000:null,{immediate:false});
  const displayVideos=retainedDetails.length?[...state.videos,...retainedDetails]:state.videos;
  function closeDetail(id:string){
    setOpenDetails(before=>{const next=new Map(before);next.delete(id);return next;});
  }
  function removeVideoFromView(id:string){
    closeDetail(id);
    if(focusedVideoId===id)clearFocus();
    setVideoSelection(before=>{const next=new Map(before);next.delete(id);return next;});
    void refreshAfterChange();
  }
  function toggleDetail(video:Video){
    setOpenDetails(before=>{
      const next=new Map(before);
      if(next.has(video.id))next.delete(video.id);
      else next.set(video.id,video);
      return next;
    });
  }
  const videoActive=!!(state.downloadWorkers?.active||state.counts.download||state.counts.processing||state.counts.deleting);
  const offlineWait=!videoActive&&session.target.availability?.reachability==='offline'&&!!state.counts.waiting;
  usePolling(async()=>{if(!actionLocked.current)await refresh();},tab==='slicing'?null:readInterval(tab==='videos'&&videoActive,tab==='videos'&&offlineWait),{immediate:false});
  async function run(action: string, fn: () => Promise<void>) {
    if(actionLocked.current)return;
    const actionVisit=viewVisit.current;
    actionLocked.current=true;
    reads.current.cancel();
    ++refreshSequence.current;
    setBusy(action); setError(''); setMessage('');
    try { await fn(); } catch (failure) { if(viewVisit.current===actionVisit)setError((failure as Error).message); }
    finally { ++refreshSequence.current;actionLocked.current=false;setBusy(''); }
  }
  function pickVideo(video: Video) {

    setVideoSelection(before => {const next = new Map(before); if (next.has(video.id)) next.delete(video.id);
      else next.set(video.id, {id:video.id,version:video.activeRevision,expectedRevision:video.revision}); return next;});
  }
  function applyVideoFilters(value: typeof blankVideoFilters) {
    detailReads.current.cancel();
    setOpenDetails(new Map());
    setLoaded(false);setAppliedVideoFilters({...value});setVideoSelection(new Map());setBulkErrors([]);
  }
  function filterVideoStatus(status: string) {
    const next={...videoFilters,status};
    setVideoFilters(next);applyVideoFilters(next);
  }
  function selectBatch(records: Video[], select: boolean) {

    setVideoSelection(before => {const next = new Map(before); records.forEach(video => {
      if (select) next.set(video.id, {id:video.id,version:video.activeRevision,expectedRevision:video.revision});
      else next.delete(video.id);
    }); return next;});
  }
  function selectLocal(groups:ProcessingGroup[],checked:boolean){setLocalSelection(before=>{const next=new Map(before);groups.forEach(group=>group.items.forEach(item=>{
    const key=`item:${item.id}`;if(checked)next.set(key,{key,id:item.id,revision:item.revision,kind:'item'});else next.delete(key);
  }));return next;});}
  async function hideLocal(choices:RecordChoice[],requestId:string){
    const current=new Map(localChoices.map(choice=>[choice.key,choice.revision]));
    if(choices.some(choice=>current.get(choice.key)!==choice.revision))throw new Error('本地记录已有新进度，请重新选择。');
    await api<{recordRemoval:{removalId:string}}>('/slicing/records/remove',{requestId,selections:choices.map(c=>({id:c.id,expectedRevision:c.revision}))});

    setLocalSelection(before=>{const next=new Map(before);choices.forEach(choice=>next.delete(choice.key));return next;});await refreshAfterChange();
  }
  async function bulk(action: 'confirm' | 'remove' | 'classify-saved', explicit?: VideoSelection[]) {
    const actionVisit=viewVisit.current;
    const selections = visibleVideoSelections(explicit || videoSelection.values(),state.videos,action==='confirm');
    if (!selections.length) return;
    if (action === 'confirm' && !window.confirm(`确认所选 ${selections.length} 条视频的最终采集区间？将自动下载、识别并导入照片；未完成采集或有异常的项目会保留。`)) return;
    await run(`bulk-${action}`, async () => {
      setBulkErrors([]);
      const result = await api<BulkResult>(`/videos/bulk-${action==='confirm'?'confirm-and-process':action}`, bulkIntents.current.payload(action,{selections,deleteCloud:false,deleteLocalFiles:false}));
      bulkIntents.current.complete(action);
      const good = result.outcomes.filter(o => o.ok);
      if(tabRef.current==='videos'&&viewVisit.current===actionVisit){
        setMessage(`${good.length} 条${action==='classify-saved'?'已归入“已保存”':`已加入${action === 'confirm' ? '全流程处理' : '清理'}队列`}${result.outcomes.length > good.length ? `，${result.outcomes.length - good.length} 条保持原状，原因见下方` : ''}。`);
        setBulkErrors(result.outcomes.filter(o => !o.ok).map(o => `${state.videos.find(v => v.id === o.id)?.name || o.id}：${o.message}`));
      }
      if(viewVisit.current===actionVisit)setVideoSelection(before => {const next = new Map(before);good.forEach(o => next.delete(o.id));return next;});
      void refresh().catch(()=>{});
    });
  }
  return <div className="app-shell"><header className="site-header"><div className="brand"><span className="brand-symbol">映</span><div><strong>采集助手</strong><small>采集 · 更正 · 切片</small></div></div>
    <nav aria-label="主导航"><button className={tab === 'collect' ? 'selected' : ''} aria-current={tab==='collect'?'page':undefined} onClick={() => navigate('collect')}>日期采集</button><button className={tab === 'videos' ? 'selected' : ''} aria-current={tab==='videos'?'page':undefined} onClick={() => navigate('videos')}>视频确认</button><button className={tab === 'slicing' ? 'selected' : ''} aria-current={tab==='slicing'?'page':undefined} onMouseEnter={warmSlicing} onFocus={warmSlicing} onClick={() => navigate('slicing')}>处理结果</button><button className={tab === 'settings' ? 'selected' : ''} aria-current={tab==='settings'?'page':undefined} onMouseEnter={warmSlicing} onFocus={warmSlicing} onClick={() => navigate('settings')}>设置</button></nav></header>
    <main><RecordOperations/><section className="hero compact"><div><h1>{tab==='slicing'?'处理结果':tab==='collect'?'日期采集':tab==='settings'?'设置':'视频确认'}</h1><p>{tab==='slicing'?'识别视频，上传并导入照片。':tab==='collect'?'先查询并核对采集计划，再确认提交。':tab==='settings'?'连接平台，调整处理参数。':'可多次更正；确认最终区间后自动处理照片。'}</p></div></section>
    {(error||readError) && <p className="error global" role="alert">{error||readError}</p>}{message && <p className="success global" role="status">{message}</p>}
    {bulkErrors.length > 0 && <div className="notice"><strong>这些条目已保留</strong><ul>{bulkErrors.map((item,i) => <li key={i}>{item}</li>)}</ul></div>}
    <PageBoundary key={tab}><Suspense fallback={<section className="panel" role="status">正在打开页面…</section>}>
    {tab === 'settings' ? <><CollectionSettings/><Slicing settingsOnly downloadControl={<DownloadManager workers={state.downloadWorkers} directory={state.directory} checkError={state.checkError} onRefresh={refreshAfterChange} onSave={async payload=>{
          reads.current.cancel();
          const workers=await api<DownloadWorkers>('/videos/download-limit',payload);
          ++refreshSequence.current;
          setState(before=>({...before,downloadWorkers:workers}));
          return workers;
        }} />} onOpenSettings={()=>navigate('settings')}/><Workspace onChanged={refreshAfterChange} /><section className="panel"><div className="section-title"><h2>平台连接</h2></div>{sessionLoaded?<div className="connections"><Login service="source" connected={session.source.connected} user={session.source.user} savedAccount={session.source.savedAccount} hasSavedCredentials={session.source.hasSavedCredentials} credentialWarning={session.source.credentialWarning} availability={session.source.availability} onDone={refreshAfterChange}/><Login service="target" connected={session.target.connected} user={session.target.user} savedAccount={session.target.savedAccount} hasSavedCredentials={session.target.hasSavedCredentials} credentialWarning={session.target.credentialWarning} availability={session.target.availability} onDone={refreshAfterChange}/><VisualConnection/></div>:<p role="status">正在读取平台连接…</p>}</section></> : tab === 'slicing' ? <Slicing onOpenSettings={()=>navigate('settings')} /> : tab === 'collect' ? <><DateCollection onVideos={()=>navigate('videos')} onSettings={()=>navigate('settings')}/><SubmissionRecords kind="collection" onChanged={() => {void refreshAfterChange();}} /></>
      : <>{(!loaded||!!state.downloadWorkers?.active||!!state.downloadWorkers?.queued)&&<p className="subtle" role="status">{state.downloadWorkers?`正在下载 ${state.downloadWorkers.active} 条，排队 ${state.downloadWorkers.queued} 条。`:"正在读取视频…"} <button className="link" onClick={()=>navigate('settings')}>并行设置</button></p>}
        <section className="panel record-controls"><div className="section-title"><h2>采集记录</h2><span role="status">{loaded?`${state.filteredCount} / ${state.totalCount} 条`:'正在读取…'}</span>
          <RecordSelection videos={state.videos} selectedIds={new Set(videoSelection.keys())} busy={!!busy||!loaded}
            selecting={selectingVideos} onSelectingChange={setSelectingVideos}
            onSelect={records=>{setVideoSelection(new Map(records.map(v=>[v.id,{id:v.id,version:v.activeRevision,expectedRevision:v.revision}])));}}
            onClear={()=>{setVideoSelection(new Map());}} onDownload={()=>void bulk('confirm')}
            onSave={()=>void bulk('classify-saved')} onRemove={async requestId=>{
              const actionVisit=viewVisit.current;
              const selectedRows=visibleVideoSelections(videoSelection.values(),state.videos).map(choice=>({id:choice.id,revision:choice.expectedRevision}));
              if(!selectedRows.length){setVideoSelection(new Map());return;}
              await api<{recordRemoval:{removalId:string}}>('/videos/records/remove',{requestId,selections:selectedRows.map(row=>({id:row.id,expectedRevision:row.revision}))});
              if(viewVisit.current!==actionVisit)return;

              setVideoSelection(new Map());void refresh().catch(()=>{});
            }} />
        </div>
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


        </section>
    <div id="video-record-results">{focusLoading&&!externalFocus&&<p className="subtle" role="status">正在读取指定视频…</p>}
    {focusError&&<p className="notice" role="status">{focusError} <button className="link" onClick={()=>{setFocusError('');setFocus(before=>before?{...before,key:uid()}:null);void refresh();}}>重新查找</button></p>}
    {externalFocus&&<section className="panel focused-video-record" aria-label="指定视频处理入口">
      <div className="section-title"><h2>本次打开的视频</h2>
        <button className="secondary" onClick={()=>{closeDetail(externalFocus.video.id);clearFocus();}}>关闭详情</button></div>
      <VideoCard video={externalFocus.video} onOpen={()=>toggleDetail(externalFocus.video)}>
        {openDetails.has(externalFocus.video.id)&&<VideoDetails video={externalFocus.video}
          processing={processing.filter(group=>group.videoId===externalFocus.video.id)}
          onRefresh={async()=>{await Promise.all([refreshAfterChange(),refreshFocused()]);}}
          onRemoved={removeVideoFromView}/>}
      </VideoCard>
    </section>}
    {!!localGroups.length&&<section data-selection-scope="local-videos" aria-label="本地视频记录">
    <RecordBatch selected={localSelection} disabled={!!busy} total={localGroups.length} scope="本地记录"
      onSelectAll={()=>selectLocal(localGroups,true)} onClear={()=>setLocalSelection(new Map())} onDelete={hideLocal}/>
    {groupSubmissionDays(localGroups,g=>g.createdAt).map(day=>{
      const choices=day.records.flatMap(group=>group.items.map(item=>({key:`item:${item.id}`,id:item.id,revision:item.revision,kind:'item' as const})));
      return <DateSection key={day.day} day={day.day} rememberKey={`local-videos:${day.day}`} expandedKey={day.records.some(g=>g.id===focus?.mediaId)?focus?.key:undefined} summary={`${day.records.length} 个本地视频待处理`}>
      <div className="date-tools"><p>无关联采集记录的视频</p><button className="secondary" disabled={!!busy||!choices.length} onClick={()=>selectLocal(day.records,true)}>选择当天记录</button></div>
      <div className="batch-video-grid">{day.records.map(group=>{const groupChoices=choices.filter(choice=>group.items.some(item=>item.id===choice.id));return <article className="video-card" key={group.id}>
        <label className="record-checkbox"><input type="checkbox" aria-label={`选择本地视频 ${group.name}`} checked={group.items.length>0&&group.items.every(item=>localSelection.has(`item:${item.id}`))} onChange={event=>selectLocal([group],event.target.checked)}/>选择</label>
        <h3 className="local-processing-name">{group.name}</h3><p className="subtle">本地视频 · 无关联质检记录</p><VideoProcessing groups={[group]} onRefresh={refreshAfterChange} candidates={state.videos}/>
        <RecordDelete name={`${group.name} · 本地视频记录`} version={groupChoices.map(c=>`${c.id}:${c.revision}`).sort().join('|')} disabled={!!busy||!groupChoices.length} onDelete={requestId=>hideLocal(groupChoices,requestId)}/>
      </article>;})}</div></DateSection>;})}</section>}
    {!loaded?<section className="panel empty" role="status" aria-busy="true"><p>正在读取采集记录，已有进度保留。</p></section>:displayVideos.length ? <VideoBatches videos={displayVideos} visibleIds={visibleIds} jobs={jobs} selecting={selectingVideos} focusedVideoId={focusedVideoId} focusKey={focus?.key}
      disabled={!!busy} onSelectBatch={selectBatch}>
      {video => <VideoCard video={video} onOpen={()=>toggleDetail(video)} selected={videoSelection.has(video.id)} onSelect={selectingVideos&&visibleIds.has(video.id)?() => pickVideo(video):undefined}>
        {openDetails.has(video.id)&&<>
          {!visibleIds.has(video.id)&&<p className="subtle">本条状态已更新；关闭详情后按当前筛选隐藏。</p>}
          <VideoDetails video={video} processing={processing.filter(g=>g.videoId===video.id)}
            onRefresh={refreshAfterChange} onRemoved={removeVideoFromView}/>
        </>}
      </VideoCard>}
    </VideoBatches> : <section className="panel empty"><h2>{state.totalCount ? '没有符合条件的记录' : state.hiddenRecordCount ? '当前没有可显示的记录' : '还没有采集记录'}</h2><p>{state.totalCount ? '可以调整日期、车牌和状态，或重置筛选。' : state.hiddenRecordCount ? '已删除记录的文件、更正和后台进度保留。' : '在“日期采集”中核对并提交后，这里会显示每辆车的采集进度。'}</p></section>}</div></>}
    </Suspense></PageBoundary>
    </main><footer className="site-footer">本地运行 · 时间按北京时间显示 · 操作进度保存在本机</footer></div>;
}
