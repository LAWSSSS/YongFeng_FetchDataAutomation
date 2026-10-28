import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { FormEvent } from 'react';
import { usePolling } from './usePolling';
import { physicalCategories, physicalFiles, physicalState, slicePage, sliceTitle, slicingControls } from './slicing-view';
import DateSection from './DateSection';
import { groupSubmissionDays, submissionInstant, submissionTime } from './submission-dates';
import RecordDelete from './RecordDelete';
import ProcessLimitControl from './ProcessLimitControl';
import './slicing.css';
import {localJson} from './local-api';
import {requestIntents} from './request-intent';
import {uiSequence} from './ui-sequence';

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
  id: string; mediaId?:string; mediaGeneration?:number; collectionRevision?:number; collectionVideoId?:string; runId?: string; sourceDatasetId: string; sourceVideoId: string; name: string; path: string; state: string;
  submittedAt?:string; revision: number; progress: number; message: string; errorCode?: string;
  partialRecovery?: boolean; localCleanupState?:string; processingBlockedReason?:string; superseded?:boolean;
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
  images?: string[]; imageCount?: number; bytes?:number; size?:number; retries?:number; cleanupMessage?:string; cleanupState?:string; localCleanupState?:string; jobId?:string; errorCode?:string};
type OutputOrganization = {id:string;revision:number;state:string;root:string;message:string;
  counts:Record<string,number>;items:{packageId:string;name:string;category:string;state:string;message:string;
    size:number;fileCount:number;source:string;destination:string}[]};
type State = {workspaceManaged?:boolean;phase?:string;resumeAvailable?:boolean; connected: boolean; user: string; settings: Settings; settingsRevision: number;
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
  submitting:'提交核对中',ready:'准备切片',pilot_deferred:'待批量切片',no_target:'未检出目标',not_applicable:'不适用',complete:'完成',attention:'需处理',waiting_connection:'等待连接',paused:'已暂停',excluded:'平台已有任务',superseded:'旧版已停止'};
const filters = [['all','全部'],['unsliced','未切片'],['pending','待处理'],['waiting_connection','等待连接'],['analyzing','识别中'],['platform_processing','平台处理中'],['attention','需处理'],['complete','已完成'],['no_target','未检出']];
const uid = () => crypto.randomUUID();
const formatDuration = (seconds?: number | null) => seconds == null ? '—' : `${Math.floor(Math.round(seconds) / 60)}分${Math.round(seconds) % 60}秒`;
const stageLabels: Record<string,string> = {checking_media:'检查视频末段',loading_model:'加载模型',sampling:'抽帧识别'};
const formatWindows = (items: Category['intervals'] = []) => items.map(row => `${row.start.toFixed(1)}–${row.end.toFixed(1)}秒`).join(' · ') || '—';

async function request<T>(path: string, body?: object): Promise<T> {
  return localJson<T>(`/api/slicing${path}`,body);
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
      await localJson('/api/upload-audit/login',{account,password});
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


function CategoryCard({categoryName, category, packages = []}: {categoryName:'bed'|'magnet'; category:Category; packages?:SlicePackage[]}) {
  const pack=packages.find(row=>row.id===category.packageId);
  const title=categoryName==='bed'?'车斗':'磁盘吊';
  const cleaned=pack?.cleanupState==='complete'||pack?.localCleanupState==='complete';
  return <div className={`slice-category ${category.state==='attention'?'needs-review':''}`}>
    <div className="slice-category-head"><strong>{title}</strong><span className={`slice-state ${category.state}`}>{labels[category.state]||category.state}</span></div>
    {!!category.intervals?.length&&<details className="slice-observations"><summary>有效区间 · {category.intervals.length} 段</summary><p className="slice-detail">{formatWindows(category.intervals)}</p></details>}
    {category.actualImages!=null&&!pack&&<p className="slice-detail">已核验 {category.actualImages} 张</p>}
    {category.message&&category.state!=='complete'&&<p className={category.state==='attention'?'error':'slice-detail'}>{category.message}</p>}
    {pack&&<p className="slice-detail">{category.actualImages!=null?`已核验 ${category.actualImages}`:pack.imageCount||0} 张 · {cleaned?'本地照片已清理':<a href={`/package-view/${pack.id}`} target="_blank" rel="noopener noreferrer">查看结果图片</a>}</p>}
    {pack?.jobId&&<details className="slice-observations"><summary>导入凭证</summary><p className="slice-detail">任务 {pack.jobId}</p></details>}
    {!pack&&category.taskId&&<p className="slice-detail">平台任务 {category.taskId} · 没有本地照片包</p>}
    {category.failure?.time!=null&&<p className="slice-detail">失败位置：{formatDuration(category.failure.time)}</p>}
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
  const intents=useRef(requestIntents());
  const updates = useRef(uiSequence());
  const acceptState = useCallback((result: State) => {
    updates.current.invalidate();
    setState(result);
    if (!dirty.current) {settingsBase.current=result.settingsRevision;setSettings(result.settings);}
  }, []);
  const refresh = useCallback(async () => {
    const sequence = updates.current.read();
    if(sequence===null)return;
    try{
      const result = await request<State>('');
      if (updates.current.accepts(sequence)) {acceptState(result);setPollError('');}
    }catch(error){if(updates.current.accepts(sequence))throw error;}
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
    if(!updates.current.begin())return;
    setBusy(name);setError('');setNotice('');
    try {acceptState(await task());intents.current.complete(name);setNotice(done);}
    catch (failure) {setError((failure as Error).message);}
    finally {updates.current.finish();setBusy('');}
  }
  function beginSettingsEdit() {if(!dirty.current) settingsBase.current=state.settingsRevision;dirty.current=true;setSettingsDirty(true);}
  function set<K extends keyof Settings>(key: K, value: Settings[K]) {beginSettingsEdit();setSettings(current => ({...current,[key]:value}));}
  async function saveSettings() {
    const result = await request<State>('/settings',intents.current.payload('settings',{...settings,directories:state.settings.directories,sliceOutputDirectory:state.settings.sliceOutputDirectory,archiveCompletedDirectory:state.settings.archiveCompletedDirectory,archiveEmptyDirectory:state.settings.archiveEmptyDirectory,expectedRevision:settingsBase.current ?? state.settingsRevision}));
    dirty.current=false;setSettingsDirty(false);
    return result;
  }
  const groups = useMemo(() => physicalFiles(state.items), [state.items]);
  const groupState = (group:typeof groups[number]) => group.records.some(i=>!!i.processingBlockedReason&&!i.superseded)||state.archive?.items.some(file=>file.path === group.records[0].path && file.archiveState === 'attention') ? 'attention' : physicalState(group.records);
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
  const targetRows = [
    {id:'27912', title:'车斗', name:'永锋车斗检测'},
    {id:'27944', title:'磁盘吊', name:'永锋-磁盘吊检测'},
  ] as const;
  const excluded = state.run?.excluded || {};
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
      {!state.connected && <SliceLogin onConnected={value=>{updates.current.invalidate();acceptState(value);}} />}
      <div className="actions slice-main-actions">
        <button disabled={!!busy || settingsDirty} onClick={() => void run('execute', () => request('/execute-all',intents.current.payload('execute',{expectedRevision:state.settingsRevision})), '已加入全部切片流程。')}>{busy === 'execute' ? '正在启动…' : '全部切片'}</button>
        {(state.enabled||controls.canResume)&&<button className="secondary" disabled={!!busy||(!state.enabled&&settingsDirty)} onClick={()=>void run(state.enabled?'pause':'resume',()=>request(state.enabled?'/pause':'/resume',state.enabled?{}:intents.current.payload('resume',{})),state.enabled?'已暂停，进度保留。':'正在继续。')}>{state.enabled?'暂停切片':'继续切片'}</button>}
      </div>
      <p className="slice-flow">{settings.reviewBeforeUpload?'磁盘吊照片需确认后上传。':'切片照片自动上传并导入。'}</p>
      <div className="slice-overall"><span>{groups.length} 个视频</span><span>完成 {groups.filter(group=>group.records.every(item=>['complete','no_target'].includes(item.state))).length}</span><span>需处理 {groups.filter(group=>groupState(group) === 'attention').length}</span><span>已归档 {state.archive?.counts.complete || 0}</span></div>
      {settingsDirty && <p className="notice">设置有未保存修改，请先保存，或重新载入已保存的设置。</p>}
      {settingsDirty && settingsBase.current !== state.settingsRevision && <p className="notice">其他页面已保存新设置。当前草稿保留，请重新载入后再修改。</p>}
      {settingsStale && <p className="notice">模型或目录设置已变化，请重新扫描，再选择待处理视频。已有结果会保留。</p>}
      {state.run && <div className={`slice-run-status ${state.run.state === 'attention' ? 'attention' : ''}`} role="status"><strong>{statusTitle}</strong><span>{statusMessage}</span>{!state.enabled && state.run.singleMode && !controls.canResume && state.run.state === 'attention' && <span>异常请前往“视频查看与更正”处理。</span>}</div>}
      {Object.keys(excluded).length > 0 && <details className="slice-scan-details"><summary>扫描与去重明细</summary><p className="slice-detail">已排除：平台处理中 {excluded.active || 0} · 平台已完成 {excluded.complete || 0} · 已切片视频 {excluded.cut || 0} · 程序已有任务 {excluded.submitted || 0}</p></details>}
      {(error || pollError) && <p className="error" role="alert">{error || pollError}{pollError&&<button className="link" disabled={!!busy} onClick={()=>void refresh().catch(f=>setPollError((f as Error).message))}>重试读取</button>}</p>}{notice && <p className="success" role="status">{notice}</p>}
    </section>

    <section className="panel slicing-management" aria-label="切片进程管理">
      <div className="section-title"><h2>切片与上传进程</h2><span>独立运行</span></div>
      <div className="slicing-management-grid">
        <section className="slice-management-card">
          <p className="slice-management-summary">{state.localWorkers
            ? `正在运行 ${state.localWorkers.active} 个 · 后台进程 ${state.localWorkers.processes} 个 · 排队 ${state.localWorkers.queued} 个`
            : '当前后台尚未提供进程状态。现有任务会继续运行，后台安全更新后即可在此调整。'}</p>
          <ProcessLimitControl label="同时识别视频数（1–8）" value={processDraft} dirty={processDirty}
            disabled={!managementApiReady || !!busy} buttonLabel="保存进程数"
            onChange={value=>{setProcessDraft(value);setProcessDirty(true);}}
            onSave={()=>void run('process-limit',async()=>{
                const result=await request<State>('/process-limit',intents.current.payload('process-limit',{expectedRevision:state.localWorkers?.revision ?? 0,maxLocalSlicingProcesses:processDraft}));
                setProcessDirty(false);return result;
              },`并行识别数已设为 ${processDraft}。`)} />
        </section>
        <section className="slice-management-card"><h3>图片上传与导入</h3><div className="slice-independent-queues">{state.packages?.targets.map(target=><span key={target.id}>{target.name} · 上传 {target.uploading} · 导入 {target.importing} · 排队 {target.queued}</span>)}</div>
          {state.localWorkers&&<button className="secondary" disabled={!!busy||!state.packages} onClick={()=>void run('package-uploads',()=>request(state.packages?.paused?'/packages/resume':'/packages/pause',{}),state.packages?.paused?'图片上传已继续。':'图片上传已暂停。')}>{state.packages?.paused?'继续图片上传':'暂停图片上传'}</button>}
        </section>
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
              <CategoryCard categoryName={name} category={item.categories[name]} packages={state.packages?.items} />

            </div>;
          })}</div>
          {physicalCategories(group.records).length < 2 && <p className="slice-detail">另一类别尚无处理记录，暂不归档。</p>}
          {first.processingBlockedReason&&<p className="slice-detail">{first.processingBlockedReason}</p>}
          {archive?.archiveState === 'complete' && archive.cleanup?.state !== 'complete' && <p className="slice-archive">已统一归档 · {archive.archivePath}</p>}
          {archive?.cleanup?.state === 'complete' && <p className="slice-detail">已清理本地视频和照片；平台结果与处理记录保留。</p>}
          {group.records.some(item=>item.collectionVideoId||!['complete','no_target','excluded','superseded'].includes(item.state))&&<button className="secondary" onClick={()=>window.dispatchEvent(new CustomEvent('simple-open-video',{detail:{mediaId:first.mediaId,videoId:first.collectionVideoId}}))}>前往处理</button>}
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
