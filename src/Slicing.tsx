import { useCallback, useEffect, useRef, useState } from 'react';
import type { FormEvent, ReactNode } from 'react';
import { usePolling } from './usePolling';
import {sliceTitle,slicingControls} from './slicing-view';
import ProcessLimitControl from './ProcessLimitControl';
import './slicing.css';
import {cancelledRead,createReadCycle,localJson} from './local-api';
import {platformStatus} from './platform-availability';
import type {PlatformAvailability} from './platform-availability';
import {readDraft,writeDraft} from './draft-storage';
import {processingLabels} from './processing-status';
import {requestIntents} from './request-intent';
import {useMediaChanges} from './useMediaChanges';
import {mediaRevision} from './media-updates';
import {uiSequence} from './ui-sequence';
import PipelinePanel from './PipelinePanel';
import SliceHistory from './SliceHistory';
import type {SliceHistorySummary} from './SliceHistory';
import {slicingReadInterval} from './view-preferences';

type Category = {
  state: string; submittedAt?:string; intervals: {start: number; end: number}[]; intervalSeconds: number | null;
  estimatedImages: number | null; actualImages: number | null; targetDatasetId?: string | null;
  taskId?: string; operationId?:string; importJobId?: string; packageId?: string; platformStatus?: string; message?: string; errorCode?: string; lastPolledAt?: string;
  observations?: number; samplingIntervalSeconds?: number; outputImageIds?: string[];
  partialRecovery?: boolean; skippedSamples?: number;
  imageQueryProgress?: {read: number; total: number | null; pages: number; phase: string};
  failure?: {stage?: string; time?: number | null};
};
type Item = {
  id: string; mediaId?:string; mediaGeneration?:number; collectionRevision?:number; collectionVideoId?:string; runId?: string; sourceDatasetId: string; sourceVideoId: string; name: string; path: string; state: string;
  submittedAt?:string; revision: number; progress: number; message: string; errorCode?: string;
  partialRecovery?: boolean; localCleanupState?:string; processingBlockedReason?:string; superseded?:boolean; coveredByRecovery?:string;
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
type SlicePackage = {id: string; itemId?:string; revision: number; dataset: string; category: string; name: string; state: string; message?: string;
  images?: string[]; imageCount?: number; bytes?:number; size?:number; retries?:number; cleanupMessage?:string; cleanupState?:string; localCleanupState?:string; jobId?:string; errorCode?:string};
type OutputOrganization = {id:string;revision:number;state:string;root:string;message:string;
  counts:Record<string,number>;items:{packageId:string;name:string;category:string;state:string;message:string;
    size:number;fileCount:number;source:string;destination:string}[]};
type State = SliceHistorySummary & {workspaceManaged?:boolean;phase?:string;resumeAvailable?:boolean; connected: boolean; availability?:PlatformAvailability; user: string; settings: Settings; settingsRevision: number;
  hiddenRecordCount?:number;
  submissionReceipts?:{id:string;title:string;revision:number;submittedAt:string;itemIds:string[];message?:string;notice?:string}[];
  packages?: {targets: {id: string; name: string; importing: number; uploading: number; queued: number}[]; items: SlicePackage[]; paused?:boolean; busy?:boolean};
  localWorkers?: {active:number; processes:number; max:number; revision?:number; queued:number};
  sliceOutputDirectoryEffective?:string;
  outputOrganization?: OutputOrganization | null;
  archive?: {items: {id:string; path:string; archiveState:string; archivePath?:string; archiveMessage?:string;
    archiveRetry?:{state?:string};
    cleanup?:{state:string;message?:string}}[]; counts:Record<string,number>};
  run: Run | null; items: Item[]; counts: Record<string, number>; busy: boolean; enabled: boolean};

const emptySettings: Settings = {revision: 0, directories: [], magnetDeployDirectory: '', magnetModelPath: '',
  bedDeployDirectory: '', bedModelPath: '', bedGeometryPath: '', frameToolPath: '', workerPythonPath: '', sharedSampling: true,
  sampleIntervalSeconds: 30, magnetIntervalSeconds: 30, bedIntervalSeconds: 30, magnetConfidence: .25,
  autoArchive:true, reviewBeforeUpload:false, sliceOutputDirectory:'', archiveCompletedDirectory:'切片完成', archiveEmptyDirectory:'未检出目标',
  maxLocalSlicingProcesses:3, targetImages: 150, bedTargetImages: 180, targetDatasetIds: {'27912': null, '27944': null}};
const empty: State = {connected: false, user: '', settings: emptySettings, settingsRevision: 0, run: null, items: [], counts: {}, busy: false, enabled: false};
// Keep the last confirmed view while returning to this tab; always refresh it.
let recentState:State|null=null;
let recentRevision=-1;
const labels: Record<string,string> = {pending:'待处理',analyzing:'识别中',exporting:'导出照片',platform_processing:'平台处理中',review:'待校对',queued:'排队上传',uploading:'正在上传',uploaded:'等待导入',importing:'正在导入',waiting:'等待平台结果',waiting_slot:'等待平台空位',
  submitting:'提交核对中',ready:'准备切片',pilot_deferred:'待批量切片',...processingLabels,not_applicable:'不适用',attention:'需处理',waiting_connection:'等待连接',paused:'已暂停',excluded:'平台已有任务',superseded:'旧版已停止'};

async function request<T>(path: string, body?: object,signal?:AbortSignal): Promise<T> {
  return localJson<T>(`/api/slicing${path}`,body,{signal});
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
      onConnected(await request<State>('?summary=true'));
    } catch (failure) { setError((failure as Error).message); }
    finally { setBusy(false); }
  }
  return <form className="login-form" onSubmit={submit}><p className="notice">连接视觉AI平台后才能上传切片压缩包。这里不处理视频上传。</p>
    <input aria-label="视觉AI账号" placeholder="账号" value={account} onChange={event => setAccount(event.target.value)} required />
    <input aria-label="视觉AI密码" type="password" placeholder="密码" value={password} onChange={event => setPassword(event.target.value)} required />
    <button disabled={busy}>{busy ? '连接中…' : '连接平台'}</button>
    {error && <p className="error">{error}</p>}</form>;
}


export function VisualConnection() {
  const [status,setStatus]=useState<{connected:boolean;user:string;availability?:PlatformAvailability}|null>(null);
  const [error,setError]=useState('');
  const reads=useRef(createReadCycle());
  const refresh=useCallback(async()=>{await reads.current.run('visual-connection',async context=>{try {const value=await request<State>('?summary=true',undefined,context.signal);if(context.current()){setStatus(value);setError('');}}
    catch(failure){if(context.current()&&!cancelledRead(failure))setError((failure as Error).message);}});},[]);
  useEffect(()=>{void refresh();return()=>reads.current.cancel();},[refresh]);
  const connection=platformStatus(status?.availability,status?.connected);
  return <div className="login-card"><div className="login-title"><strong>视觉AI平台</strong>
    <span className={connection.connected?'dot connected':'dot'}/>{!status?'读取连接中':connection.label}{connection.authenticated&&status?.user?` · ${status.user}`:''}</div>
    {status&&!connection.authenticated&&<SliceLogin onConnected={value=>setStatus(value)}/>}
    {error&&<p className="error" role="alert">{error} <button className="link" onClick={()=>void refresh()}>重新读取</button></p>}</div>;
}


export default function Slicing({onOpenSettings,settingsOnly=false,downloadControl}:{onOpenSettings:()=>void;settingsOnly?:boolean;downloadControl?:ReactNode}) {
  const [state, setState] = useState<State>(()=>recentRevision===mediaRevision()?recentState||empty:empty);
  const [savedDraft]=useState(()=>readDraft<{settings:Settings;revision:number}|null>('slicing-settings',null,(value):value is {settings:Settings;revision:number}=>!!value&&typeof value==='object'&&Number.isInteger((value as {revision:unknown}).revision)&&typeof (value as {settings?:Settings}).settings?.workerPythonPath==='string'));
  const [settings, setSettings] = useState<Settings>(()=>savedDraft?.settings||recentState?.settings||emptySettings);
  const [refreshing,setRefreshing]=useState(true);
  const [settingsDirty, setSettingsDirty] = useState(!!savedDraft);
  const [busy, setBusy] = useState('');
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [pollError, setPollError] = useState('');
  const [processDraft, setProcessDraft] = useState(3);
  const [processDirty, setProcessDirty] = useState(false);
  const dirty = useRef(!!savedDraft);
  const settingsBase = useRef<number | null>(savedDraft?.revision??null);
  const reads=useRef(createReadCycle()),mounted=useRef(true);
  const intents=useRef(requestIntents());
  const updates = useRef(uiSequence());
  const acceptState = useCallback((result: State) => {
    updates.current.invalidate();
    recentState=result;
    recentRevision=mediaRevision();
    setState(result);
    if (!dirty.current) {settingsBase.current=result.settingsRevision;setSettings(result.settings);}
  }, []);
  const refresh = useCallback(async () => {
    await reads.current.run('slicing-summary',async context=>{
      const sequence=updates.current.read();if(sequence===null)return;
      try{
        const result=await request<State>('?summary=true',undefined,context.signal);
        if(context.current()&&updates.current.accepts(sequence)){acceptState(result);setPollError('');setRefreshing(false);}
      }catch(failure){if(context.current()&&!cancelledRead(failure)){setRefreshing(false);setPollError((failure as Error).message);}}
    });
  }, [acceptState]);
  useEffect(()=>{mounted.current=true;void refresh();return()=>{mounted.current=false;reads.current.cancel();updates.current.invalidate();};},[refresh]);
  useMediaChanges(()=>{reads.current.cancel();void refresh();});
  useEffect(() => {
    if (!processDirty) setProcessDraft(state.localWorkers?.max ?? state.settings.maxLocalSlicingProcesses ?? 3);
  }, [processDirty, state.localWorkers?.max, state.settings.maxLocalSlicingProcesses]);
  usePolling(refresh, slicingReadInterval(state),
    {immediate:false,onError:failure=>setPollError((failure as Error).message)});
  useEffect(() => {
    if (!state.enabled && state.run && ['complete','attention','pilot_complete'].includes(state.run.state)) setNotice('');
  }, [state.enabled,state.run?.state]);
  async function run(name: string, task: () => Promise<State>, done: string) {
    if(!updates.current.begin())return;
    reads.current.cancel();setBusy(name);setError('');setNotice('');
    try {const result=await task();intents.current.complete(name);if(mounted.current){acceptState(result);setNotice(done);}}
    catch (failure) {if(mounted.current)setError((failure as Error).message);}
    finally {updates.current.finish();if(mounted.current)setBusy('');}
  }
  function beginSettingsEdit() {if(!dirty.current) settingsBase.current=state.settingsRevision;dirty.current=true;setSettingsDirty(true);}
  function set<K extends keyof Settings>(key: K, value: Settings[K]) {beginSettingsEdit();setSettings(current => {const next={...current,[key]:value};writeDraft('slicing-settings',{settings:next,revision:settingsBase.current});return next;});}
  async function saveSettings() {
    const result = await request<State>('/settings',intents.current.payload('settings',{...settings,directories:state.settings.directories,sliceOutputDirectory:state.settings.sliceOutputDirectory,archiveCompletedDirectory:state.settings.archiveCompletedDirectory,archiveEmptyDirectory:state.settings.archiveEmptyDirectory,maxLocalSlicingProcesses:state.settings.maxLocalSlicingProcesses,expectedRevision:settingsBase.current ?? state.settingsRevision}));
    dirty.current=false;writeDraft('slicing-settings',undefined);setSettingsDirty(false);
    return result;
  }
  const controls = slicingControls(state);
  const connection=platformStatus(state.availability,state.connected);
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
  const maintenance = (<details className="panel slicing-intro"><summary>高级维护</summary><p className="subtle">仅用于接入历史平台视频。日常确认视频后会自动处理，无需扫描。</p><div className="section-title"><h2>历史平台视频</h2><span className={`slice-connection ${connection.connected ? 'connected' : ''}`}>{connection.label}</span></div>
      <div className="slice-destination" aria-label="图片保存位置">
        <strong>图片保存位置</strong>
        {targetRows.map(({id,title,name}) => {
          const target = state.run?.targetDatasets?.[id];
          return <span key={id}>{title}：{target?.name || name}{target?.selectedId ? ` · 数据集 ${target.selectedId}` : ''}</span>;
        })}
      </div>
      {!connection.connected && <p className="notice">平台连接尚未核实或暂不可达。本地识别继续，照片等待连接恢复。<button className="link" onClick={onOpenSettings}>连接平台</button></p>}
      <div className="actions slice-main-actions">
        <button disabled={!!busy || settingsDirty} onClick={() => void run('execute', () => request('/execute-all',intents.current.payload('execute',{expectedRevision:state.settingsRevision})), '已加入全部切片流程。')}>{busy === 'execute' ? '正在扫描…' : '扫描平台视频'}</button>
        {(state.enabled||controls.canResume)&&<button className="secondary" disabled={!!busy||(!state.enabled&&settingsDirty)} onClick={()=>void run(state.enabled?'pause':'resume',()=>request(state.enabled?'/pause':'/resume',state.enabled?{}:intents.current.payload('resume',{})),state.enabled?'已暂停，进度保留。':'正在继续。')}>{state.enabled?'暂停切片':'继续切片'}</button>}
      </div>
      <p className={state.packages?.paused?'notice':'slice-flow'} role="status">{state.packages?.paused?'照片上传与新导入已暂停，本地识别可继续；已提交的平台任务仍会核对。':'最终确认的视频自动上传照片并导入，无需再次审核。'}</p>
      {settingsDirty && <p className="notice">设置有未保存修改，请先保存，或重新载入已保存的设置。</p>}
      {settingsDirty && settingsBase.current !== state.settingsRevision && <p className="notice">其他页面已保存新设置。当前草稿保留，请重新载入后再修改。</p>}
      {settingsStale && <p className="notice">参数已更新，请刷新状态后继续。</p>}
      {state.run&&(state.enabled||state.busy||controls.canResume)&&<div className={`slice-run-status ${state.run.state === 'attention' ? 'attention' : ''}`} role="status"><strong>{statusTitle}</strong><span>{statusMessage}</span></div>}
      {Object.keys(excluded).length > 0 && <details className="slice-scan-details"><summary>扫描与去重明细</summary><p className="slice-detail">已排除：平台处理中 {excluded.active || 0} · 平台已完成 {excluded.complete || 0} · 已切片视频 {excluded.cut || 0} · 程序已有任务 {excluded.submitted || 0}</p></details>}
            <label className="wide check-label"><input type="checkbox" checked={!settings.reviewBeforeUpload} onChange={event=>set('reviewBeforeUpload', !event.target.checked)} />旧版手动切片免审核（最终确认的视频始终自动导入）</label>
      <details className="record-advanced"><summary>核对目标图片集</summary><div className="slice-settings-grid">{(['27912','27944'] as const).map(id=>{
        const target=state.run?.targetDatasets?.[id];
        return <label key={id}>{id==='27912'?'永锋车斗检测':'永锋-磁盘吊检测'}
          {target?.candidates.length?<select value={settings.targetDatasetIds[id]||''} onChange={e=>set('targetDatasetIds',{...settings.targetDatasetIds,[id]:e.target.value||null})}>
            <option value="">自动匹配</option>{target.candidates.map(candidate=><option key={candidate.id} value={candidate.id}>{candidate.name}</option>)}</select>:
          <input inputMode="numeric" aria-label={`${id}图片目标数据集编号`} value={settings.targetDatasetIds[id]||''} placeholder="仅匹配异常时填写编号" onChange={e=>set('targetDatasetIds',{...settings.targetDatasetIds,[id]:e.target.value||null})}/>}
        </label>;
      })}</div><p className="subtle">修改后请点击上方“保存识别参数”。</p></details>
    </details>);
  return <>
    {!settingsOnly&&<><PipelinePanel compact onChanged={refresh}/>
    {refreshing&&<p className="subtle" role="status">正在刷新处理进度…</p>}
    </>}
    {settingsOnly&&<><div className="settings-process-row">{downloadControl}<section className="panel slicing-management" aria-label="切片进程管理">
      <div className="section-title"><h2>切片与上传进程</h2><span>独立运行</span></div>
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
        <section className="panel slicing-management photo-management" aria-label="图片上传与导入"><div className="section-title"><h2>图片上传与导入</h2></div>
          <p className="slice-management-summary" role="status">{state.packages?.paused?'上传已暂停；已提交的导入继续核对。':'照片自动上传、导入并核验。'}</p>
          <div className="slice-independent-queues">{state.packages?.targets.map(target=><span key={target.id}>{target.name} · 上传 {target.uploading} · 导入 {target.importing} · 排队 {target.queued}</span>)}</div>
          {state.localWorkers&&<button className="secondary" disabled={!!busy||!state.packages} onClick={()=>void run('package-uploads',()=>request(state.packages?.paused?'/packages/resume':'/packages/pause',{}),state.packages?.paused?'图片上传已继续。':'图片上传已暂停。')}>{state.packages?.paused?'继续图片上传':'暂停图片上传'}</button>}
        </section></div>

    <details className="panel slicing-settings"><summary>识别参数</summary>
      <label className="parameter-toggle"><input type="checkbox" checked={settings.sharedSampling} onChange={event => set('sharedSampling',event.target.checked)} />两个模型共用抽帧</label>
      <div className="slice-settings-grid">
        {settings.sharedSampling ? <label>共用抽样间隔（秒）<input type="number" min={1} max={3600} value={settings.sampleIntervalSeconds} onChange={event => set('sampleIntervalSeconds',Number(event.target.value))} /></label> : <>
          <label>磁盘吊抽样间隔（秒）<input type="number" min={1} max={3600} value={settings.magnetIntervalSeconds} onChange={event => set('magnetIntervalSeconds',Number(event.target.value))} /></label>
          <label>车斗抽样间隔（秒）<input type="number" min={1} max={3600} value={settings.bedIntervalSeconds} onChange={event => set('bedIntervalSeconds',Number(event.target.value))} /></label></>}

        <label>车斗图片数（100–200）<input type="number" min={100} max={200} value={settings.bedTargetImages ?? 180} onChange={event => set('bedTargetImages',Number(event.target.value))} /></label>
        <label>磁盘吊目标图片数（100–200）<input type="number" min={100} max={200} value={settings.targetImages} onChange={event => set('targetImages',Number(event.target.value))} /></label>
        <label>磁盘吊置信度<input type="number" min={0} max={1} step="0.01" value={settings.magnetConfidence} onChange={event => set('magnetConfidence',Number(event.target.value))} /></label>
        <div className="wide dataset-target-summary"><span>车斗照片：永锋车斗检测</span><span>磁盘吊照片：永锋-磁盘吊检测</span></div>
      </div>
      {(state.busy || state.enabled) && <p className="notice">处理中暂不能保存识别参数。暂停并等待本条停止后，可修改设置；有效观察会继续保留。</p>}
      <p className="subtle">参数仅用于后续处理，已有结果保留。</p>
      <div className="actions"><button disabled={state.busy || state.enabled || !!busy || !settingsDirty} onClick={()=>void run('settings',saveSettings,'识别参数已保存。')}>保存识别参数</button>{settingsDirty && <button className="secondary" onClick={()=>{dirty.current=false;writeDraft('slicing-settings',undefined);setSettingsDirty(false);setSettings(state.settings);}}>撤销修改</button>}</div>
    </details>
    {maintenance}
    </>}
    {(error||pollError)&&<p className="error" role="alert">{error||pollError}{pollError&&<button className="link" disabled={!!busy} onClick={()=>void refresh()}>重试读取</button>}</p>}{notice&&<p className="success" role="status">{notice}</p>}
    {!settingsOnly&&<SliceHistory snapshot={state} onChanged={()=>void refresh().catch(failure=>setPollError((failure as Error).message))}/>}
  </>;
}
