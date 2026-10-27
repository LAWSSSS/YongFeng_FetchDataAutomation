import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { usePolling } from './usePolling';
import { physicalCategories, physicalFiles, physicalState, slicePage, sliceTitle, slicingControls } from './slicing-view';
import DateSection from './DateSection';
import { groupSubmissionDays, submissionDay, submissionInstant, submissionTime } from './submission-dates';
import RecordDelete from './RecordDelete';
import ProcessLimitControl from './ProcessLimitControl';
import './slicing.css';

type Category = {
  state: string; submittedAt?:string; intervals: {start: number; end: number}[]; intervalSeconds: number | null;
  estimatedImages: number | null; actualImages: number | null; targetDatasetId?: string | null;
  taskId?: string; importJobId?: string; packageId?: string; platformStatus?: string; message?: string; errorCode?: string; lastPolledAt?: string;
  observations?: number; samplingIntervalSeconds?: number; outputImageIds?: string[];
  partialRecovery?: boolean; skippedSamples?: number;
  imageQueryProgress?: {read: number; total: number | null; pages: number; phase: string};
  failure?: {stage?: string; time?: number | null};
};
type Item = {
  id: string; runId?: string; sourceDatasetId: string; sourceVideoId: string; name: string; path: string; state: string;
  submittedAt?:string; revision: number; progress: number; message: string; errorCode?: string;
  partialRecovery?: boolean; localCleanupState?:string;
  analysisStage?: string; analysisTotal?: number; analysisDone?: number; analysisPosition?: number;
  remote: {duration: number | null; size: number | null}; metadata?: {duration: number; width: number; height: number};
  categories: {bed: Category; magnet: Category};
};
type Settings = {
  revision: number; directories: string[]; magnetDeployDirectory: string; magnetModelPath: string;
  bedDeployDirectory: string; bedModelPath: string; bedGeometryPath: string; frameToolPath: string;
  workerPythonPath: string; sharedSampling: boolean; sampleIntervalSeconds: number;
  magnetIntervalSeconds: number; bedIntervalSeconds: number; magnetConfidence: number;
  autoArchive: boolean; reviewBeforeUpload: boolean; sliceOutputDirectory: string;
  archiveCompletedDirectory: string; archiveEmptyDirectory: string;
  maxLocalSlicingProcesses: number;
  targetImages: number; bedTargetImages: number; targetDatasetIds: Record<string, string | null>;
};
type Candidate = {id: string; name: string; category: string; namespace: string; namespaceInferred?: boolean; verified?: boolean; selected?: boolean};
type DatasetChoice = {name: string; candidates: Candidate[]; selectedId: string | null; resolved: boolean};
type Run = {executeAll?:boolean; id: string; state: string; message: string; itemCount?: number; items?: string[];
  singleMode?: boolean; singleItemId?: string; finishedAt?: string; settingsRevision?: number;
  excluded?: Record<string, number>; targetDatasets?: Record<string, DatasetChoice>; issues?: {path: string; message: string}[]};
type SlicePackage = {id: string; revision: number; dataset: string; category: string; name: string; state: string; message?: string;
  images?: string[]; imageCount?: number; bytes?:number; size?:number; retries?:number; cleanupMessage?:string; jobId?:string; errorCode?:string};
type OutputOrganization = {id:string;revision:number;state:string;root:string;message:string;
  counts:Record<string,number>;items:{packageId:string;name:string;category:string;state:string;message:string;
    size:number;fileCount:number;source:string;destination:string}[]};
type State = {workspaceManaged?:boolean;phase?:string; connected: boolean; user: string; settings: Settings; settingsRevision: number;
  hiddenRecordCount?:number;
  submissionReceipts?:{id:string;title:string;revision:number;submittedAt:string;itemIds:string[];message?:string;notice?:string}[];
  packages?: {targets: {id: string; name: string; importing: number; uploading: number; queued: number}[]; items: SlicePackage[]; paused?:boolean; busy?:boolean};
  localWorkers?: {active:number; processes:number; max:number; revision?:number; queued:number};
  sliceOutputDirectoryEffective?:string;
  outputOrganization?: OutputOrganization | null;
  archive?: {items: {id:string; path:string; archiveState:string; archivePath?:string; archiveMessage?:string;
    cleanup?:{state:string;message?:string}}[]; counts:Record<string,number>};
  run: Run | null; items: Item[]; counts: Record<string, number>; busy: boolean; enabled: boolean};

const emptySettings: Settings = {revision: 0, directories: [], magnetDeployDirectory: '', magnetModelPath: '',
  bedDeployDirectory: '', bedModelPath: '', bedGeometryPath: '', frameToolPath: '', workerPythonPath: '', sharedSampling: true,
  sampleIntervalSeconds: 30, magnetIntervalSeconds: 30, bedIntervalSeconds: 30, magnetConfidence: .25,
  autoArchive:true, reviewBeforeUpload:false, sliceOutputDirectory:'', archiveCompletedDirectory:'切片完成', archiveEmptyDirectory:'未检出目标',
  maxLocalSlicingProcesses:3, targetImages: 150, bedTargetImages: 180, targetDatasetIds: {'27912': null, '27944': null}};
const empty: State = {connected: false, user: '', settings: emptySettings, settingsRevision: 0, run: null, items: [], counts: {}, busy: false, enabled: false};
const labels: Record<string,string> = {pending:'待处理',analyzing:'识别中',exporting:'导出照片',platform_processing:'平台处理中',review:'待校对',queued:'排队上传',uploading:'正在上传',uploaded:'等待导入',importing:'正在导入',waiting:'等待平台结果',waiting_slot:'等待平台空位',
  submitting:'提交核对中',ready:'准备切片',pilot_deferred:'待批量切片',no_target:'未检出目标',not_applicable:'不适用',complete:'完成',attention:'需处理',waiting_connection:'等待连接',paused:'已暂停',excluded:'平台已有任务'};
const filters = [['all','全部'],['unsliced','未切片'],['pending','待处理'],['waiting_connection','等待连接'],['analyzing','识别中'],['platform_processing','平台处理中'],['attention','需处理'],['complete','已完成'],['no_target','未检出']];
const uid = () => crypto.randomUUID();
const formatDuration = (seconds?: number | null) => seconds == null ? '—' : `${Math.floor(Math.round(seconds) / 60)}分${Math.round(seconds) % 60}秒`;
const stageLabels: Record<string,string> = {checking_media:'检查视频末段',loading_model:'加载模型',sampling:'抽帧识别'};
const formatWindows = (items: Category['intervals'] = []) => items.map(row => `${row.start.toFixed(1)}–${row.end.toFixed(1)}秒`).join(' · ') || '—';

async function request<T>(path: string, body?: object): Promise<T> {
  let response: Response;
  try { response = await fetch(`/api/slicing${path}`, body === undefined ? undefined : {
    method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body),
  }); } catch { throw new Error('本地程序暂时无法连接。'); }
  const data = await response.json();
  if (!response.ok) throw new Error(data.message || data.detail || `请求失败（${response.status}）`);
  return data as T;
}

function intervalText(value: Category['intervals']) { return value.map(row => `${row.start.toFixed(1)}-${row.end.toFixed(1)}`).join(', '); }
function parseIntervals(value: string) {
  return value.split(',').map(part => part.trim()).filter(Boolean).map(part => {
    const match = part.match(/^(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)$/);
    if (!match) throw new Error('区间请按“开始-结束秒”填写，多个区间用逗号分开。');
    return {start:Number(match[1]), end:Number(match[2])};
  });
}

function SliceLogin({onConnected}: {onConnected: (state: State) => void}) {
  const [account, setAccount] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  async function submit(event: FormEvent) {
    event.preventDefault();
    setBusy(true); setError('');
    try {
      const response = await fetch('/api/upload-audit/login', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({account, password})});
      const data = await response.json();
      if (!response.ok) throw new Error(data.message || data.detail || '连接失败。');
      setPassword('');
      onConnected(await request<State>(''));
    } catch (failure) { setError((failure as Error).message); }
    finally { setBusy(false); }
  }
  return <form className="login-form" onSubmit={submit}><p className="notice">连接视觉AI平台后才能上传切片压缩包。这里不处理视频上传。</p>
    <input aria-label="视觉AI账号" placeholder="账号" value={account} onChange={event => setAccount(event.target.value)} required />
    <input aria-label="视觉AI密码" type="password" placeholder="密码" value={password} onChange={event => setPassword(event.target.value)} required />
    <button disabled={busy}>{busy ? '连接中…' : '连接平台'}</button>
    {error && <p className="error">{error}</p>}</form>;
}


function CategoryCard({item, categoryName, category, packages = [], onUpdated}: {item: Item; categoryName: 'bed'|'magnet'; category: Category; packages?: SlicePackage[]; onUpdated: (state: State) => void}) {
  const [editing, setEditing] = useState(false);
  const [intervals, setIntervals] = useState(() => intervalText(category.intervals));
  const [path, setPath] = useState(item.path);
  const [error, setError] = useState('');
  const [frameTime, setFrameTime] = useState<number | null>(null);
  const [frameUrl, setFrameUrl] = useState('');
  const [working, setWorking] = useState(false);
  const [frameBusy, setFrameBusy] = useState(false);
  const actionLock = useRef(false);
  const title = categoryName === 'bed' ? '车斗' : '磁盘吊';
  const savedIntervals = intervalText(category.intervals);
  useEffect(() => {if (!editing) setIntervals(savedIntervals);}, [savedIntervals,editing]);
  useEffect(() => {if (!editing) setPath(item.path);}, [item.path,editing]);
  useEffect(() => () => {if (frameUrl) URL.revokeObjectURL(frameUrl);}, [frameUrl]);
  async function act(name: string, body: object) {
    if (actionLock.current) return;
    actionLock.current=true;
    setError('');setWorking(true);
    try {onUpdated(await request<State>(`/${item.id}/${name}`, body));setEditing(false);}
    catch (failure) {setError((failure as Error).message);}
    finally {actionLock.current=false;setWorking(false);}
  }
  async function packageAction(name:'confirm'|'retry', pack:SlicePackage) {
    if(actionLock.current) return;
    actionLock.current=true;setWorking(true);setError('');
    try {onUpdated(await request<State>(`/packages/${pack.id}/${name}`, {requestId:uid(),expectedRevision:pack.revision}));}
    catch(failure) {setError((failure as Error).message);}
    finally {actionLock.current=false;setWorking(false);}
  }
  async function showFrame(timestamp: number) {
    setFrameBusy(true);setError('');
    try {
      const response = await fetch(`/api/slicing/${item.id}/frame?time_seconds=${timestamp}`);
      if (!response.ok) throw new Error('关键画面读取失败。');
      const blob = await response.blob();setFrameUrl(URL.createObjectURL(blob));setFrameTime(timestamp);
    } catch (failure) {setError((failure as Error).message);}
    finally {setFrameBusy(false);}
  }
  const pack = packages.find(row => row.id === category.packageId);
  const expected = category.estimatedImages == null ? '未计算' : `${category.estimatedImages} 张`;
  const actual = category.actualImages == null ? '尚未完成' : `${category.actualImages} 张`;
  const analyzing = item.state === 'analyzing' && category.state === 'pending';
  return <div className={`slice-category ${category.state === 'attention' ? 'needs-review' : ''}`}>
    <div className="slice-category-head"><strong>{title}</strong><span className={`slice-state ${analyzing ? 'analyzing' : category.state}`}>{analyzing ? '识别中' : category.state === 'waiting' && category.platformStatus === '3' ? '核验图片' : labels[category.state] || category.state}</span></div>
    {category.submittedAt&&<p className="slice-detail">提交于 {submissionDay(category.submittedAt)} {submissionTime(category.submittedAt)}</p>}
    {category.partialRecovery && <p className="slice-detail">可解码片段模式 · 已跳过 {category.skippedSamples || 0} 个无法解码的抽样位置</p>}
    {!!category.intervals?.length && <>{category.intervals.length > 2 ? <details className="slice-observations"><summary>有效区间 · {category.intervals.length} 段</summary><p className="slice-detail">{formatWindows(category.intervals)}</p></details> : <p className="slice-detail">{['pending','attention'].includes(category.state) && !pack && !category.taskId ? '暂存区间（尚未提交）' : '有效区间'}：{formatWindows(category.intervals)}</p>}
      <p className="slice-detail">每 {category.intervalSeconds} 秒一张 · {category.actualImages == null ? `预计 ${expected}` : `已核验 ${actual}`}</p></>}
    {category.taskId && <p className="slice-detail">平台任务 {category.taskId}{category.state === 'complete' && category.targetDatasetId && <> · <a href={`http://pre-athena-mv.cisdigital.cn/#/dataset/${category.targetDatasetId}?tab=content&namespace=1`} target="_blank" rel="noopener noreferrer">查看结果图片</a></>}</p>}
    {pack && <><p className="slice-detail">{pack.jobId ? `导入任务 ${pack.jobId} · ` : ''}{pack.state === 'uploading' ? `已确认 ${Math.round((pack.bytes || 0)/1024**2)} / ${Math.round((pack.size || 0)/1024**2)} MB · ` : ''}重试 {pack.retries || 0} 次
      {pack.state === 'complete' && <>{' · '}<a href={`http://pre-athena-mv.cisdigital.cn/#/dataset/${pack.dataset}?tab=content&namespace=1`} target="_blank" rel="noopener noreferrer">查看结果图片</a></>}</p>
      {pack.cleanupMessage && <p className="slice-detail">{pack.cleanupMessage}</p>}
      {pack.state === 'attention' && <button className="secondary" disabled={working} onClick={()=>void packageAction('retry',pack)}>{working ? '处理中…' : pack.errorCode === 'archive_layout' ? '修复压缩包并重传' : '重新核对并继续'}</button>}
    </>}
    {category.state === 'waiting' && category.lastPolledAt && <p className="slice-detail">最近核对：{new Date(category.lastPolledAt).toLocaleTimeString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false})}</p>}
    {category.message && !analyzing && category.state !== 'pending' && category.state !== 'complete' && <p className={category.state === 'attention' ? 'error' : 'slice-detail'}>{category.message}</p>}
    {category.failure?.time != null && category.state === 'attention' && <p className="slice-detail">{category.failure.stage === 'checking_media' ? '末段检查位置' : '失败位置'}：{formatDuration(category.failure.time)} · 已保存 {category.observations || 0} 个有效判断</p>}
    {category.imageQueryProgress && ['waiting','ready','submitting'].includes(category.state) && category.imageQueryProgress.total != null && category.imageQueryProgress.total > 0 &&
      <progress aria-label="图片核验进度" max={category.imageQueryProgress.total} value={category.imageQueryProgress.read} />}
    {!item.localCleanupState && category.intervals?.length > 0 && <details className="slice-observations"><summary>查看抽样边界 · {category.observations ?? 0} 个判断位置</summary>
      <div className="slice-times">{category.intervals.flatMap((window, index) => [
        <button className="link" disabled={frameBusy} key={`${index}-start`} onClick={() => void showFrame(window.start)}>{window.start.toFixed(1)} 秒</button>,
        <button className="link" disabled={frameBusy} key={`${index}-end`} onClick={() => void showFrame(window.end)}>{window.end.toFixed(1)} 秒</button>,
      ])}</div>
    </details>}
    {frameUrl && <div className="slice-frame"><img src={frameUrl} alt={`${title} ${frameTime} 秒关键画面`} /><span>{frameTime?.toFixed(1)} 秒</span></div>}
    {category.state === 'review' && pack && <div className="slice-review-photos">
      <div className="slice-photos">{(pack.images || []).slice(0, 12).map(name =>
        <img key={name} src={`/api/slicing/packages/${pack.id}/images/${encodeURIComponent(name)}`} alt={name} />)}</div>
      <p className="slice-detail">共 {pack.imageCount || pack.images?.length || 0} 张。确认后才会上传。</p>
      <button disabled={working} onClick={() => void packageAction('confirm',pack)}>{working ? '确认中…' : '确认并上传这个压缩包'}</button>
    </div>}
    {!item.localCleanupState && !pack && (category.state === 'attention' || category.state === 'ready' || category.state === 'no_target') && <details className="slice-review" open={editing} onToggle={event => setEditing(event.currentTarget.open)}>
      <summary>核对原片或调整区间</summary>
      {editing && <div className="slice-review-body">
        {item.path ? <video controls preload="metadata" src={`/api/slicing/${item.id}/stream`} /> : <p className="notice">先手动绑定一个本地视频文件。</p>}
        <label>区间（秒；多个区间以逗号分开）<input value={intervals} onChange={event => setIntervals(event.target.value)} placeholder="例如 12-385, 520-880" /></label>
        <div className="actions"><button disabled={working} onClick={() => {
          try {void act('review', {requestId:uid(),expectedRevision:item.revision,category:categoryName,intervals:parseIntervals(intervals)});}
          catch (failure) {setError((failure as Error).message);}
        }}>保存区间</button>
        <button className="secondary" disabled={working} onClick={() => void act('review', {requestId:uid(),expectedRevision:item.revision,category:categoryName,confirmedNoTarget:true})}>确认未检出</button></div>
        <label>本地原视频完整路径<input value={path} onChange={event => setPath(event.target.value)} /></label>
        {path !== item.path && <button className="secondary" disabled={working} onClick={() => void act('bind',{requestId:uid(),expectedRevision:item.revision,path})}>{item.path ? '重新绑定视频文件' : '绑定视频文件'}</button>}
      </div>}
    </details>}
    {error && <p className="error" role="alert">{error}</p>}
  </div>;
}

export default function Slicing() {
  const [state, setState] = useState<State>(empty);
  const [settings, setSettings] = useState<Settings>(emptySettings);
  const [settingsDirty, setSettingsDirty] = useState(false);
  const [busy, setBusy] = useState('');
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [removed,setRemoved]=useState<{name:string;id:string;restoreRequestId:string}|null>(null);
  const [filter, setFilter] = useState('all');
  const [search, setSearch] = useState('');
  const [source, setSource] = useState('all');
  const [pollError, setPollError] = useState('');
  const [processDraft, setProcessDraft] = useState(3);
  const [processDirty, setProcessDirty] = useState(false);
  const dirty = useRef(false);
  const settingsBase = useRef<number | null>(null);
  const refreshSequence = useRef(0);
  const acceptState = useCallback((result: State) => {
    ++refreshSequence.current;
    setState(result);
    if (!dirty.current) {settingsBase.current=result.settingsRevision;setSettings(result.settings);}
  }, []);
  const refresh = useCallback(async () => {
    const sequence = ++refreshSequence.current;
    const result = await request<State>('');
    if (sequence === refreshSequence.current) {acceptState(result);setPollError('');}
  }, [acceptState]);
  useEffect(() => {void refresh().catch(failure => setError((failure as Error).message));}, [refresh]);
  useEffect(() => {
    if (!processDirty) setProcessDraft(state.localWorkers?.max ?? state.settings.maxLocalSlicingProcesses ?? 3);
  }, [processDirty, state.localWorkers?.max, state.settings.maxLocalSlicingProcesses]);
  usePolling(refresh, state.enabled || state.busy ? 4000 : 15000,
    {immediate:false,onError:failure=>setPollError((failure as Error).message)});
  useEffect(() => {
    if (!state.enabled && state.run && ['complete','attention','pilot_complete'].includes(state.run.state)) setNotice('');
  }, [state.enabled,state.run?.state]);
  async function run(name: string, task: () => Promise<State>, done: string) {
    setBusy(name);setError('');setNotice('');
    try {acceptState(await task());setNotice(done);}
    catch (failure) {setError((failure as Error).message);}
    finally {setBusy('');}
  }
  function beginSettingsEdit() {if(!dirty.current) settingsBase.current=state.settingsRevision;dirty.current=true;setSettingsDirty(true);}
  function set<K extends keyof Settings>(key: K, value: Settings[K]) {beginSettingsEdit();setSettings(current => ({...current,[key]:value}));}
  async function saveSettings() {
    const result = await request<State>('/settings',{...settings,directories:state.settings.directories,sliceOutputDirectory:state.settings.sliceOutputDirectory,archiveCompletedDirectory:state.settings.archiveCompletedDirectory,archiveEmptyDirectory:state.settings.archiveEmptyDirectory,expectedRevision:settingsBase.current ?? state.settingsRevision,requestId:uid()});
    dirty.current=false;setSettingsDirty(false);
    return result;
  }
  const groups = useMemo(() => physicalFiles(state.items), [state.items]);
  const groupState = (group:typeof groups[number]) => state.archive?.items.some(file=>file.path === group.records[0].path && file.archiveState === 'attention') ? 'attention' : physicalState(group.records);
  const filtered = groups.filter(group => {
    const overall = groupState(group);
    if (filter === 'unsliced' && ['complete','no_target'].includes(overall)) return false;
    if (source === 'all' && filter !== 'all' && filter !== 'unsliced' && overall !== filter) return false;
    const recordStatus = filter === 'unsliced' ? 'unsliced' : source === 'all' ? 'all' : filter;
    return slicePage(group.records,{status:recordStatus,source,search,page:1,pageSize:group.records.length}).total > 0;
  });
  const datedGroups=groupSubmissionDays(filtered,group=>group.records.map(row=>row.submittedAt).filter(Boolean)
    .sort((a,b)=>(submissionInstant(a)?.getTime()||0)-(submissionInstant(b)?.getTime()||0))[0]);
  const visibleIds=new Set(filtered.flatMap(group=>group.records.map(row=>row.id)));
  const filtering=filter!=='all'||source!=='all'||!!search.trim();
  const datedReceipts=groupSubmissionDays((state.submissionReceipts||[]).filter(row=>!filtering||row.itemIds.some(id=>visibleIds.has(id))),row=>row.submittedAt);
  const dates=[...new Set([...datedGroups.map(row=>row.day),...datedReceipts.map(row=>row.day)])].sort((a,b)=>b.localeCompare(a)).map(day=>({
    day,groups:datedGroups.find(row=>row.day===day)?.records||[],receipts:datedReceipts.find(row=>row.day===day)?.records||[],
  }));
  async function removeRecords(items:Item[],receipts:NonNullable<State['submissionReceipts']>,name:string,requestId:string) {
    const result=await request<State&{recordRemoval:{removalId:string}}>('/records/remove-group',{
      requestId,selections:items.map(item=>({id:item.id,expectedRevision:item.revision})),
      receipts:receipts.map(row=>({id:row.id,expectedRevision:row.revision})),
    });
    acceptState(result);setRemoved({name,id:result.recordRemoval.removalId,restoreRequestId:uid()});
  }
  const controls = slicingControls(state);
  const managementApiReady = state.localWorkers !== undefined && state.sliceOutputDirectoryEffective !== undefined;
  const settingsStale = state.run?.settingsRevision != null && state.run.settingsRevision !== state.settingsRevision;
  const canSingleSlice = (item: Item) => controls.scanReady && item.runId === state.run?.id && !!item.path &&
    (item.state === 'pending' || (item.state === 'platform_processing' &&
      Object.values(item.categories).some(category=>['ready','waiting_slot'].includes(category.state) && !category.taskId)));
  const targetRows = [
    {id:'27912', title:'车斗', name:'永锋车斗检测'},
    {id:'27944', title:'磁盘吊', name:'永锋-磁盘吊检测'},
  ] as const;
  const excluded = state.run?.excluded || {};
  const recoverableCount = new Set(state.items
    .filter(item=>item.state === 'attention' && item.errorCode === 'media_incomplete')
    .map(item=>item.path || item.id)).size;
  const runMessage = state.run?.message?.replace(
    '请核对目标图片集和需要手动绑定的本地视频，再开始自动切片。',
    '本地原片已按唯一完整文件名自动匹配；扫描时会自动定位两个图片保存位置。',
  );
  const selected = state.run?.singleMode ? state.items.find(item=>item.id === state.run?.singleItemId) : undefined;
  const selectedTitle = selected ? sliceTitle(selected.name) : undefined;
  const statusMessage = state.phase === 'scanning' ? '正在扫描并补充新视频…' : selected ? `${selectedTitle?.title} · ${selectedTitle?.when}：${selected.message || labels[selected.state]}` :
    !state.enabled && !state.busy && state.hiddenRecordCount && ['complete','attention'].includes(state.run?.state||'') ? '本轮处理已结束。已删除记录的任务和成果仍保留。' : runMessage;
  const statusTitle = state.phase === 'waiting_connection' ? '等待连接' : state.phase === 'exporting' ? '导出照片' : state.phase === 'archiving' ? '正在归档' : state.phase === 'platform_processing' && state.enabled ? '等待平台结果' : state.phase === 'scanning' || state.run?.state === 'scanning' ? '扫描中' : state.enabled ? (state.run?.singleMode ? '单条处理中' : '队列运行中') :
    state.busy ? '正在停止' : selected?.state === 'complete' ? '本条已完成' : selected?.state === 'no_target' ? '未检出目标' :
    selected?.state === 'attention' ? '本条需处理' : state.run?.state === 'paused' ? '已暂停' : state.run?.state === 'complete' && state.run.executeAll ? '已完成' : '准备就绪';
  return <>
    <section className="panel slicing-intro"><div className="section-title"><h2>切片工作台</h2><span className={`slice-connection ${state.connected ? 'connected' : ''}`}>{state.connected ? '平台已连接' : '平台未连接'}</span></div>
      <div className="slice-destination" aria-label="图片保存位置">
        <strong>图片保存位置</strong>
        {targetRows.map(({id,title,name}) => {
          const target = state.run?.targetDatasets?.[id];
          return <span key={id}>{title}：{target?.name || name}{target?.selectedId ? ` · 数据集 ${target.selectedId}` : ''}</span>;
        })}
      </div>
      {!state.connected && <SliceLogin onConnected={acceptState} />}
      <div className="actions slice-main-actions">
        <button disabled={!!busy || settingsDirty} onClick={() => void run('execute', () => request('/execute-all',{requestId:uid(),expectedRevision:state.settingsRevision}), '已加入全部切片流程。')}>{busy === 'execute' ? '正在启动…' : '全部切片'}</button>
        {recoverableCount > 0 && <button className="secondary" disabled={!state.connected || !!busy || state.busy || settingsDirty || settingsStale} onClick={() => void run('recover-decodable', () => request('/recover-decodable-all',{requestId:uid(),expectedRevision:state.settingsRevision}), `正在恢复 ${recoverableCount} 个视频的可解码片段。`)}>恢复可解码片段 · {recoverableCount}</button>}
        <button className="secondary" disabled={!state.enabled || !!busy} onClick={() => void run('pause', () => request('/pause',{}),'已暂停，进度保留。')}>暂停</button>
        <button className="secondary" disabled={!controls.canResume || !!busy || settingsDirty} onClick={() => void run('resume', () => request('/resume',{requestId:uid()}),'正在继续。')}>继续</button>
        {state.localWorkers && <button className="secondary" disabled={!!busy || !state.packages} onClick={() => void run('package-uploads', () => request(state.packages?.paused ? '/packages/resume' : '/packages/pause',{}), state.packages?.paused ? '图片上传已继续；切片队列独立运行。' : '图片上传已暂停；切片队列独立运行。')}>{state.packages?.paused ? '继续图片上传' : '暂停图片上传'}</button>}
        <button className="link" disabled={!!busy} onClick={() => void run('refresh', () => request<State>(''),'状态已更新。')}>刷新</button>
      </div>
      <p className="slice-flow">{settings.reviewBeforeUpload ? '已打开审核：磁盘吊照片确认后才上传。' : '自动模式：切片完成后直接上传压缩包。车斗导入 27892，磁盘吊导入 27893，不需要审核。'}</p>
      {state.localWorkers ? <p className="slice-flow">本地识别任务 {state.localWorkers.active}/{state.localWorkers.max} · 当前进程 {state.localWorkers.processes} · 待识别 {state.localWorkers.queued}；图片上传与导入独立运行。</p> : <p className="slice-flow">正在使用当前后台版本；并行设置会在安全升级服务后启用。</p>}
      {!!state.packages?.targets.length && <div className="slice-overall">{state.packages.targets.map(target =>
        <span key={target.id}>{target.name} · 上传 {target.uploading} · 导入 {target.importing} · 排队 {target.queued}</span>)}</div>}
      <div className="slice-overall"><span>{groups.length} 个视频</span><span>完成 {groups.filter(group=>group.records.every(item=>['complete','no_target'].includes(item.state))).length}</span><span>需处理 {groups.filter(group=>groupState(group) === 'attention').length}</span><span>已归档 {state.archive?.counts.complete || 0}</span></div>
      {settingsDirty && <p className="notice">设置有未保存修改，请先保存，或重新载入已保存的设置。</p>}
      {settingsDirty && settingsBase.current !== state.settingsRevision && <p className="notice">其他页面已保存新设置。当前草稿保留，请重新载入后再修改。</p>}
      {settingsStale && <p className="notice">模型或目录设置已变化，请重新扫描，再选择待处理视频。已有结果会保留。</p>}
      {state.run && <div className={`slice-run-status ${state.run.state === 'attention' ? 'attention' : ''}`} role="status"><strong>{statusTitle}</strong><span>{statusMessage}</span>{!state.enabled && state.run.singleMode && !controls.canResume && state.run.state === 'attention' && <span>可重试该条，也可以选择下方其他待处理视频。</span>}</div>}
      {Object.keys(excluded).length > 0 && <details className="slice-scan-details"><summary>扫描与去重明细</summary><p className="slice-detail">已排除：平台处理中 {excluded.active || 0} · 平台已完成 {excluded.complete || 0} · 已切片视频 {excluded.cut || 0} · 程序已有任务 {excluded.submitted || 0}</p></details>}
      {(error || pollError) && <p className="error" role="alert">{error || pollError}</p>}{notice && <p className="success" role="status">{notice}</p>}
    </section>

    <section className="panel slicing-management" aria-label="进程与文件目录管理">
      <div className="section-title"><h2>进程管理</h2><span>本地识别与上传独立运行</span></div>
      <div className="slicing-management-grid">
        <section className="slice-management-card">
          <h3>本地切片进程</h3>
          <p className="slice-management-summary">{state.localWorkers
            ? `正在运行 ${state.localWorkers.active} 个 · 后台进程 ${state.localWorkers.processes} 个 · 排队 ${state.localWorkers.queued} 个`
            : '当前后台尚未提供进程状态。现有任务会继续运行，后台安全更新后即可在此调整。'}</p>
          <ProcessLimitControl label="同时识别视频数（1–8）" value={processDraft} dirty={processDirty}
            disabled={!managementApiReady || !!busy} buttonLabel="保存进程数"
            onChange={value=>{setProcessDraft(value);setProcessDirty(true);}}
            onSave={()=>void run('process-limit',async()=>{
                const result=await request<State>('/process-limit',{requestId:uid(),expectedRevision:state.localWorkers?.revision ?? 0,maxLocalSlicingProcesses:processDraft});
                setProcessDirty(false);return result;
              },`并行识别数已设为 ${processDraft}。`)} />
          <div className="slice-independent-queues"><strong>图片上传与导入（独立队列）</strong>{state.packages?.targets.map(target=><span key={target.id}>{target.name} · 上传 {target.uploading} · 导入 {target.importing} · 排队 {target.queued}</span>)}</div>
          <p className="subtle">上传和导入不占用本地识别进程。增加识别进程会提高并行度，也会增加内存和显存占用。</p>
        </section>

        <section className="slice-management-card"><h3>统一目录</h3><p>原片、照片和压缩包由上方“文件工作区”管理，处理完成后自动流转。</p><p className="slice-path-current">待核验照片：<code>{state.sliceOutputDirectoryEffective || '启用工作区后显示'}</code></p></section>
      </div>

    </section>

    <details className="panel slicing-settings"><summary>模型与切片参数</summary>
      <div className="slice-settings-grid">
        <label>磁盘吊部署目录<input value={settings.magnetDeployDirectory} onChange={event => set('magnetDeployDirectory',event.target.value)} /></label>
        <label>磁盘吊模型文件<input value={settings.magnetModelPath} onChange={event => set('magnetModelPath',event.target.value)} /></label>
        <label>车斗部署目录<input value={settings.bedDeployDirectory} onChange={event => set('bedDeployDirectory',event.target.value)} /></label>
        <label>车斗分类模型文件<input value={settings.bedModelPath} onChange={event => set('bedModelPath',event.target.value)} /></label>
        <label className="wide">车斗定位文件<input value={settings.bedGeometryPath} onChange={event => set('bedGeometryPath',event.target.value)} /></label>
        <label>抽帧工具路径<input value={settings.frameToolPath} onChange={event => set('frameToolPath',event.target.value)} /></label>
        <label>Python 3.11 推理程序路径<input value={settings.workerPythonPath} onChange={event => set('workerPythonPath',event.target.value)} /></label>
        <label className="wide check-label"><input type="checkbox" checked={settings.sharedSampling} onChange={event => set('sharedSampling',event.target.checked)} />两个模型共用抽帧</label>
        {settings.sharedSampling ? <label>共用抽样间隔（秒）<input type="number" min={1} max={3600} value={settings.sampleIntervalSeconds} onChange={event => set('sampleIntervalSeconds',Number(event.target.value))} /></label> : <>
          <label>磁盘吊抽样间隔（秒）<input type="number" min={1} max={3600} value={settings.magnetIntervalSeconds} onChange={event => set('magnetIntervalSeconds',Number(event.target.value))} /></label>
          <label>车斗抽样间隔（秒）<input type="number" min={1} max={3600} value={settings.bedIntervalSeconds} onChange={event => set('bedIntervalSeconds',Number(event.target.value))} /></label></>}
        <label className="wide check-label"><input type="checkbox" checked={!settings.reviewBeforeUpload} onChange={event=>set('reviewBeforeUpload', !event.target.checked)} />不需要审核：切片完成后直接上传压缩包并按数据集导入</label>
        <label>车斗目标图片数（100–200，仅后续视频）<input type="number" min={100} max={200} value={settings.bedTargetImages ?? 180} onChange={event => set('bedTargetImages',Number(event.target.value))} /></label>
        <label>磁盘吊目标图片数（100–200）<input type="number" min={100} max={200} value={settings.targetImages} onChange={event => set('targetImages',Number(event.target.value))} /></label>
        <label>磁盘吊置信度<input type="number" min={0} max={1} step="0.01" value={settings.magnetConfidence} onChange={event => set('magnetConfidence',Number(event.target.value))} /></label>
        {(['27912','27944'] as const).map(id => {
          const target = state.run?.targetDatasets?.[id];
          if (target?.resolved && target.selectedId === settings.targetDatasetIds[id]) return null;
          return <label key={id}> {id === '27912' ? '车斗' : '磁盘吊'}图片目标集（{target?.name || (id === '27912' ? '永锋车斗检测' : '永锋-磁盘吊检测')}）
            <select value={settings.targetDatasetIds[id] || ''} onChange={event => set('targetDatasetIds',{...settings.targetDatasetIds,[id]:event.target.value || null})}>
              <option value="">扫描时自动查找</option>{target?.candidates.map(candidate => <option key={candidate.id} value={candidate.id}>
                {candidate.name} · ID {candidate.id} · {candidate.category === 'image' ? '图片集' : '类型未识别'} · 命名空间 {candidate.namespaceInferred ? `${candidate.namespace}（按查询范围）` : candidate.namespace || '未知'}
              </option>)}
            </select><input inputMode="numeric" aria-label={`${id}图片目标数据集编号`} value={settings.targetDatasetIds[id] || ''} placeholder="无法自动定位时填写数据集编号" onChange={event => set('targetDatasetIds',{...settings.targetDatasetIds,[id]:event.target.value || null})} />
            <small>{target?.resolved ? `扫描时已确认：${target.selectedId}` : '连接平台并扫描后会按名称、图片类型和命名空间自动匹配。'}</small></label>;
        })}
      </div>
      {(state.busy || state.enabled) && <p className="notice">处理中暂不能保存模型设置。暂停并等待本条停止后，可修改设置；有效观察会继续保留。</p>}
      <p className="subtle">视频、照片和压缩包由“文件工作区”管理。模型参数在此保存，Python 推理环境独立运行。</p>
      <div className="actions"><button disabled={!state.connected || state.busy || state.enabled || !!busy || !settingsDirty} onClick={()=>void run('settings',saveSettings,'模型设置已保存。')}>保存模型设置</button>{settingsDirty && <button className="secondary" onClick={()=>{dirty.current=false;setSettingsDirty(false);setSettings(state.settings);}}>撤销修改</button>}</div>
    </details>

    <section className="panel slice-records"><div className="section-title"><h2>切片记录</h2><span>按提交日期展开 · 提交与视频合并显示</span></div>
      {removed&&<div className="record-deleted-notice" role="status"><span>已删除 {removed.name} 的网页记录，原视频和任务进度保留。</span>
        <button className="link" disabled={!!busy} onClick={()=>void run('restore-record',async()=>{
          const result=await request<State>('/records/restore-group',{requestId:removed.restoreRequestId,removalId:removed.id});
          setRemoved(null);return result;
        },'记录已恢复。')}>撤销删除</button></div>}
      {!!state.hiddenRecordCount&&<p className="subtle">已从列表删除 {state.hiddenRecordCount} 项类别记录，后台处理和平台成果保留。</p>}
      <div className="slice-controls"><div className="video-status-filters" role="group" aria-label="切片记录状态筛选">{filters.map(([key,label]) => <button key={key} aria-pressed={filter === key} aria-controls="slicing-record-results" onClick={() => setFilter(key)}>{label}</button>)}</div>
        <div className="slice-search"><select aria-label="筛选识别类别" value={source} onChange={event=>setSource(event.target.value)}><option value="all">全部类别</option><option value="27912">车斗</option><option value="27944">磁盘吊</option></select>
        <input aria-label="搜索视频" placeholder="搜索名称或编号" value={search} onChange={event => setSearch(event.target.value)} /></div></div>
      {!dates.length ? <div className="empty" id="slicing-record-results"><p>{groups.length?'没有符合条件的记录。':state.hiddenRecordCount?'当前没有可显示的记录，已删除项的任务和成果保留。':'点击“全部切片”，自动扫描并处理两个视频集。'}</p>{groups.length>0&&<button className="secondary" onClick={()=>{setFilter('all');setSource('all');setSearch('');}}>清除筛选</button>}</div> :
      <div className="date-list" id="slicing-record-results">{dates.map(date=>{
        const items=date.groups.flatMap(group=>group.records);
        const receiptDetails=date.receipts.map(row=><div className="receipt-operation" key={row.id}><div><span><strong>{submissionTime(row.submittedAt)} · {row.title}</strong></span>
            <RecordDelete name={`${submissionTime(row.submittedAt)} · 切片提交`} version={row.id} disabled={!!busy}
              onDelete={requestId=>removeRecords([],[row],row.title,requestId)}/></div>
            {row.notice&&<p>{row.notice}</p>}{row.message&&<p>当时记录：{row.message}</p>}</div>);
        return <DateSection key={date.day} day={date.day} summary={date.groups.length?`${date.groups.length} 个视频 · ${date.receipts.length} 次提交`:`${date.receipts.length} 次提交 · 视频明细未关联`}>
          <div className="date-tools"><p>{date.day?'当天提交及关联的视频详情':'旧记录缺少可确认的提交日期'}</p>
            <RecordDelete name={`${date.day||'日期未记录'} · 当前全部切片记录`} disabled={!!busy}
              version={JSON.stringify([items.map(row=>`${row.id}:${row.revision}`).sort(),date.receipts.map(row=>row.id).sort()])}
              description={`只删除当前列出的 ${date.groups.length} 个视频和 ${date.receipts.length} 次提交记录；文件、成果及后台进度保留。`}
              onDelete={requestId=>removeRecords(items,date.receipts,date.day||'日期未记录',requestId)}/></div>
          <div className="batch-video-grid">{date.groups.map(group => {
        const first = group.records[0];
        const heading = sliceTitle(first.name);
        const archive = state.archive?.items.find(file=>file.path === first.path);
        const overall = groupState(group);
        return <div className="batch-record" key={group.key}><article className="video-card slice-video-card">
          <div className="video-head slice-video-head"><div><h3 title={first.name}>{heading.title}{heading.restored && <small className="slice-restored">恢复版</small>}</h3>{heading.when && <p className="slice-record-time">{heading.when}</p>}<span className="slice-video-source">{formatDuration(first.metadata?.duration || first.remote?.duration)}</span></div><span className={`slice-state ${overall}`}>{labels[overall] || overall}</span></div>
          <div className="slice-categories">{physicalCategories(group.records,source).map(({item,categoryName:name}) => {
            return <div key={name}>
              {item.state === 'analyzing' && <div className="slice-analysis"><p>{stageLabels[item.analysisStage || ''] || '准备识别'}{item.analysisTotal ? ` · ${item.analysisDone || 0}/${item.analysisTotal}` : ''}</p><progress max="100" value={item.progress} aria-label="识别进度" /></div>}
              <CategoryCard item={item} categoryName={name} category={item.categories[name]} packages={state.packages?.items} onUpdated={acceptState} />
              <div className="actions">
                {canSingleSlice(item) && <button className="secondary" disabled={!state.connected || !controls.idle || settingsDirty || settingsStale || !!busy} onClick={() => void run(`single-${item.id}`, () => request('/start',{requestId:uid(),expectedRevision:state.settingsRevision,mode:'single',itemId:item.id}),'本条已启动。')}>执行本类</button>}
                {!item.localCleanupState && item.state === 'attention' && !item.categories[name].packageId && <button className="secondary" disabled={!state.connected || (state.busy && !state.enabled) || !!busy || settingsDirty} onClick={() => void run(`retry-${item.id}`, () => request(`/${item.id}/retry`,{requestId:uid(),expectedRevision:item.revision}),'本条恢复中。')}>重试本类</button>}
              </div>
            </div>;
          })}</div>
          {physicalCategories(group.records).length < 2 && <p className="slice-detail">另一类别尚无处理记录，暂不归档。</p>}
          {archive?.archiveState === 'complete' && archive.cleanup?.state !== 'complete' && <p className="slice-archive">已统一归档 · {archive.archivePath}</p>}
          {archive?.cleanup?.state === 'complete' && <p className="slice-detail">已清理本地视频和照片；平台结果与处理记录保留。</p>}
          {!state.workspaceManaged && archive?.archiveState === 'complete' && archive.cleanup?.state !== 'complete' && <div className="slice-archive">
            {archive.cleanup?.state === 'attention' && <p className="error">{archive.cleanup.message || '清理未完成；剩余文件保留。'}</p>}
            <button className="secondary" disabled={!!busy || state.busy} onClick={async()=>{
              if (!window.confirm('程序已核验切片结果。确定删除本地原视频、程序生成的切片照片和压缩包？平台结果与记录会保留。')) return;
              try {
                const response = await fetch(`/api/files/${archive.id}/cleanup-confirmed`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({requestId:uid(),confirm:true})});
                if (!response.ok) {const body = await response.json().catch(()=>({})); throw new Error(body.detail || '清理未完成，文件已保留或部分清理。');}
                await refresh();
              } catch(failure) {setError((failure as Error).message);}
            }}>清理已核验视频和照片</button>
          </div>}
          {archive?.archiveState === 'attention' && <div className="slice-archive"><p>{archive.archiveMessage}</p><button className="secondary" onClick={async()=>{
            try {const response = await fetch(`/api/files/${archive.id}/archive-retry`,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});if(!response.ok) throw new Error('归档重试失败。');await refresh();} catch(failure) {setError((failure as Error).message);}
          }}>重试归档</button></div>}
          <details className="slice-file"><summary>文件位置</summary><p className="slice-detail">{first.name}<br/>{first.path || '未绑定文件'}</p></details>
          <RecordDelete name={`${heading.title}${heading.when?` · ${heading.when}`:''}`} version={group.records.map(i=>`${i.id}:${i.revision}`).sort().join('|')} disabled={!!busy}
            description="删除本视频的全部类别记录，原视频、平台成果和任务进度保留。正在进行的任务继续。"
            onDelete={requestId=>removeRecords(group.records,[],heading.title,requestId)}/>
        </article></div>;
      })}</div>
          {date.receipts.length>0&&(date.groups.length?<details className="slice-receipts"><summary>提交记录 · {date.receipts.length}</summary>{receiptDetails}</details>:receiptDetails)}
        </DateSection>;
      })}</div>}
    </section>
  </>;
}
