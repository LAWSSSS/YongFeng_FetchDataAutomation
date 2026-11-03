import {useCallback,useEffect,useRef,useState} from 'react';
import {cancelledRead,createReadCycle,localJson} from './local-api';
import {usePolling} from './usePolling';
import {requestIntents} from './request-intent';
import {sliceTitle} from './slicing-view';
import {pipelineBusy,pipelineSelection} from './pipeline-view';
import type {PipelineAction,PipelineVideo} from './pipeline-view';
import {pipelineReadInterval,transferProgress} from './view-preferences';
import './pipeline.css';
import CompletedCleanup from './CompletedCleanup';
import {useMediaChanges} from './useMediaChanges';
import {mediaRevision} from './media-updates';
import DateSection from './DateSection';
import {groupSubmissionDays} from './submission-dates';

type State={enabled:boolean;paused:boolean;revision:number;workspaceOnline:boolean;offlineWaiting?:boolean;items:PipelineVideo[];
  backgroundCounts?:{processing:number;waiting:number;attention:number;complete:number};
  photoUploads?:{paused:boolean;queued:number;videos:number;images:number};
  counts:{processing:number;waiting:number;attention:number;complete:number};page:number;pages:number;total:number};
type Operation={operationId:string;state:string;total:number;finished:number;outcomes:{mediaId:string;ok:boolean;message?:string}[]};
const api=<T,>(path='',body?:unknown,signal?:AbortSignal)=>localJson<T>(`/api/pipeline${path}`,body,{signal});
const labels={all:'全部',processing:'自动处理中',waiting:'等待恢复',attention:'需要你处理',complete:'已完成'};
let lastState:State|null=null;
let lastQuery='';
let lastRevision=-1;
let lastView:{filter:keyof typeof labels;search:string;appliedSearch:string;page:number}={filter:'all',search:'',appliedSearch:'',page:1};

export default function PipelinePanel({onChanged,compact=false}:{onChanged:()=>Promise<void>;compact?:boolean}){
  const [state,setState]=useState<State|null>(()=>lastRevision===mediaRevision()?lastState:null),[filter,setFilter]=useState<keyof typeof labels>(lastView.filter);
  const [search,setSearch]=useState(lastView.search),[appliedSearch,setAppliedSearch]=useState(lastView.appliedSearch),[page,setPage]=useState(lastView.page);
  const [loadedKey,setLoadedKey]=useState(lastQuery),[busy,setBusy]=useState(''),[error,setError]=useState(''),[notice,setNotice]=useState('');
  const [photoError,setPhotoError]=useState('');
  const [operation,setOperation]=useState<Operation|null>(null),[refreshing,setRefreshing]=useState(true);
  const [operationId,setOperationId]=useState(()=>{try{return sessionStorage.getItem('pipeline-operation')||'';}catch{return '';}});
  const reads=useRef(createReadCycle()),intents=useRef(requestIntents()),locked=useRef(false);
  const query=new URLSearchParams({filter,search:appliedSearch,page:String(page),pageSize:'20'}).toString();
  const refresh=useCallback(async()=>{
    if(locked.current)return;
    await reads.current.run(query,async context=>{
      try{const value=await api<State>(`?${query}`,undefined,context.signal);
        if(context.current()){
          const acceptedQuery=new URLSearchParams(query);acceptedQuery.set('page',String(value.page));
          lastState=value;lastRevision=mediaRevision();lastQuery=acceptedQuery.toString();setState(value);setPage(value.page);
          setLoadedKey(lastQuery);setError('');setRefreshing(false);
        }}
      catch(failure){if(context.current()&&!cancelledRead(failure)){setError((failure as Error).message);setRefreshing(false);}}
    });
  },[query]);
  useEffect(()=>{void refresh();return()=>reads.current.cancel();},[refresh]);
  useMediaChanges(()=>{reads.current.cancel();void refresh();});
  const signature=JSON.stringify(state&&[state.counts,state.photoUploads,state.items.map(row=>[row.id,row.version,row.generation,row.stage,row.hasResultRecord])]);
  const previous=useRef<string|null>(null);
  useEffect(()=>{const changed=previous.current!==null&&previous.current!==signature;previous.current=signature;
    if(changed)void onChanged().catch(()=>{});},[signature,onChanged]);
  useEffect(()=>{lastView={filter,search,appliedSearch,page};},[filter,search,appliedSearch,page]);
  useEffect(()=>{if(search.trim()===appliedSearch)return;
    const timer=setTimeout(()=>{setAppliedSearch(search.trim());setPage(1);},250);return()=>clearTimeout(timer);
  },[search,appliedSearch]);
  usePolling(refresh,pipelineReadInterval(state?.counts,!!state&&(!!state.offlineWaiting||!state.workspaceOnline)),{immediate:false});
  const currentOperation=useRef(operationId);currentOperation.current=operationId;
  const refreshOperation=useCallback(async(signal?:AbortSignal)=>{
    if(!operationId)return;
    let value:Operation;
    try{value=await api<Operation>(`/operations/${encodeURIComponent(operationId)}`,undefined,signal);}
    catch(failure){
      if(signal?.aborted||currentOperation.current!==operationId)return;
      if((failure as Error).message.includes('不存在')){
        setOperationId('');setOperation(null);try{sessionStorage.removeItem('pipeline-operation');}catch{/* Optional preference. */}
        setError('这次登记记录已不可用，请按视频的当前状态操作。');void refresh();return;
      }
      throw failure;
    }
    if(signal?.aborted||currentOperation.current!==operationId)return;
    setOperation(value);
    if(!pipelineBusy(value.state)){
      setOperationId('');try{sessionStorage.removeItem('pipeline-operation');}catch{/* Optional preference. */}
      setNotice(`已登记 ${value.outcomes.filter(row=>row.ok).length} 条${value.outcomes.some(row=>!row.ok)?'，部分项目需查看结果':''}。`);
      void refresh();void onChanged().catch(()=>{});
    }
  },[operationId,refresh,onChanged]);
  usePolling(refreshOperation,operationId?operation?.state==='waiting_connection'?30000:4000:null,{onError:failure=>setError((failure as Error).message)});
  async function control(action:'start'|'pause'|'resume'){
    if(!state||locked.current)return;
    locked.current=true;reads.current.cancel();setBusy(action);setError('');
    try{await api(`/${action}`,intents.current.payload(action,{expectedRevision:state.revision}));
      intents.current.complete(action);setNotice(action==='pause'?'自动处理已暂停，断点保留。':'已启用持续自动处理，新确认下载的视频会自动接入。');}
    catch(failure){setError((failure as Error).message);}
    finally{locked.current=false;setBusy('');void refresh();void onChanged().catch(()=>{});}
  }
  async function enqueue(rows:PipelineVideo[],action?:PipelineAction){
    if(!rows.length||locked.current||operationId)return;
    locked.current=true;reads.current.cancel();setBusy('enqueue');setError('');setNotice('');
    try{const selections=rows.map(row=>pipelineSelection(row,action));
      const result=await api<{operationId:string;state:string}>('/continue-batch',intents.current.payload('continue',{selections}));
      intents.current.complete('continue');setOperationId(result.operationId);
      setOperation({operationId:result.operationId,state:result.state,total:rows.length,finished:0,outcomes:[]});
      try{sessionStorage.setItem('pipeline-operation',result.operationId);}catch{/* Backend keeps operation progress. */}
      setNotice('操作已登记，后台继续处理。');}
    catch(failure){setError((failure as Error).message);}
    finally{locked.current=false;setBusy('');}
  }
  async function resumePhotos(){
    if(locked.current||operationId)return;
    locked.current=true;reads.current.cancel();setBusy('resume-photos');setError('');setPhotoError('');setNotice('');
    try{
      await localJson('/api/slicing/packages/resume',{});
      setNotice('照片上传已继续，将自动导入并核验；单独暂停的视频保持暂停。');
    }catch(failure){setPhotoError((failure as Error).message);}
    finally{locked.current=false;setBusy('');void refresh();void onChanged().catch(()=>{});}
  }
  const current=loadedKey===query;
  const rows=current?state?.items||[]:[];
  const beforeSlicing=rows.filter(row=>row.hasResultRecord===false);
  const hiddenActive=Object.entries(state?.backgroundCounts||{}).filter(([key])=>key!=='complete').reduce((sum,[,value])=>sum+value,0);
  const continuable=rows.filter(row=>row.nextAction.canContinue);
  const disabled=!!busy||!!operationId||!current||search.trim()!==appliedSearch;
  const open=(row:PipelineVideo)=>window.dispatchEvent(new CustomEvent('simple-open-video',{detail:{videoId:row.videoId,mediaId:row.mediaId}}));
  return <><CompletedCleanup/><section className="panel pipeline-panel" aria-labelledby="pipeline-title">
    <div className="pipeline-heading"><div><h2 id="pipeline-title">自动处理</h2></div>
      <div className="actions"><button disabled={!state||!state.enabled||!!busy} onClick={()=>void control(!state?.enabled?'start':state.paused?'resume':'pause')}>
        {busy&&busy!=='enqueue'?'正在登记…':!state?.enabled?'等待视频确认':state.paused?'继续自动处理':'暂停自动处理'}</button></div></div>
    {state&&<p className="pipeline-mode" role="status">{!state.enabled?'确认视频后自动开始，无需另行开启':state.paused?'已暂停':'持续自动处理已开启'}{!state.workspaceOnline?' · 等待工作区磁盘':''}</p>}
    {state?.photoUploads?.paused&&state.photoUploads.queued>0&&<div className="pipeline-photo-pause" role="status">
      <div><strong>照片上传已暂停</strong><p>{state.photoUploads.videos} 个视频、{state.photoUploads.queued} 个照片包等待上传或导入。继续后自动核验并整理。</p></div>
      <button disabled={!!busy||!!operationId} onClick={()=>void resumePhotos()}>{busy==='resume-photos'?'正在继续…':'继续照片上传'}</button>
    </div>}
    {photoError&&<p className="error" role="alert">{photoError} <button className="link" disabled={!!busy||!!operationId} onClick={()=>void resumePhotos()}>重试继续上传</button></p>}
    {refreshing&&state&&loadedKey===query&&<p className="subtle pipeline-refresh" role="status">正在更新进度，先显示上次状态。</p>}
    {!compact&&<><div className="pipeline-filters" aria-label="处理状态筛选">{(Object.entries(labels) as [keyof typeof labels,string][]).map(([value,label])=><button key={value} className={filter===value?'selected':'secondary'} aria-pressed={filter===value}
      onClick={()=>{setFilter(value);setPage(1);}}>{label}{state&&value!=='all'?` ${state.counts[value]}`:''}</button>)}</div>
    <div className="pipeline-tools"><label>查找视频<input type="search" value={search} onChange={event=>setSearch(event.target.value)} placeholder="输入车牌或视频名"/></label>
      {continuable.length>1&&<button className="secondary" disabled={disabled} onClick={()=>void enqueue(continuable)}>继续本页可恢复的 {continuable.length} 条</button>}</div>
    </>}{compact&&state&&<p className="pipeline-counts">处理中 {state.counts.processing} · 等待恢复 {state.counts.waiting} · 需处理 {state.counts.attention} · 已完成 {state.counts.complete}</p>}
    {!!hiddenActive&&<p className="subtle">另有 {hiddenActive} 条已隐藏记录在后台处理或等待，原进度保留。</p>}
    {error&&<p className="error" role="alert">{error}<button className="link" onClick={()=>{if(operationId)void refreshOperation().catch(f=>setError((f as Error).message));else void refresh();}}>重试读取</button></p>}
    {notice&&<p className="subtle" role="status">{notice}</p>}
    {operation&&(pipelineBusy(operation.state)||operation.outcomes.some(row=>!row.ok))&&<div className="pipeline-operation" role="status"><p>{pipelineBusy(operation.state)?`正在登记 ${operation.finished}/${operation.total}`:'部分项目需要核对'}</p>
      {operation.outcomes.some(row=>!row.ok)&&<details><summary>查看未执行项目</summary><ul>{operation.outcomes.filter(row=>!row.ok).map((row,index)=><li key={index}>{row.message||'状态已变化，请重新核对。'}</li>)}</ul></details>}</div>}
    {compact&&groupSubmissionDays(beforeSlicing,row=>row.confirmedAt).map(day=><DateSection key={day.day} day={day.day}
      rememberKey={`slice-history:${day.day}`} expandedKey={`confirmed:${day.day}`} summary={`${day.records.length} 条已确认视频`}>
      <div className="batch-record-list">{day.records.map(row=><article className="video-card" key={`${row.mediaId}:${row.generation}`} data-processing-id={row.mediaId}>
        <div className="video-head"><h3>{row.name}</h3><span className={`slice-state ${row.state}`}>{row.stage}</span></div>
        {row.version!=null&&<p className="subtle">第 {row.version+1} 版 · 已确认</p>}<p className="subtle">{row.message}</p>
        {row.state!=='complete'&&row.nextAction.progress&&<div className="pipeline-progress"><progress aria-label={`${row.name} ${row.stage}`} value={row.nextAction.progress.done} max={row.nextAction.progress.total}/>
          <small>{row.nextAction.progress.unit==='bytes'?transferProgress(row.nextAction.progress.done,row.nextAction.progress.total):`${row.nextAction.progress.done}/${row.nextAction.progress.total} ${row.nextAction.progress.unit}`}</small></div>}
        <button className="secondary" onClick={()=>open(row)}>查看详情</button>
      </article>)}</div>
    </DateSection>)}
    {!compact&&<>{!current?<p className="subtle" role="status">正在读取处理状态…</p>:rows.length?<ul className="attention-list">{rows.map(row=>{
      const title=sliceTitle(row.name),next=row.nextAction;
      return <li className="attention-row" key={`${row.mediaId}:${row.generation}`}><div className="attention-copy"><div className="attention-name"><strong>{title.title}</strong>
        {title.when&&<span>{title.when}</span>}<span className={`pipeline-label ${next.kind}`}>{next.stage||row.stage}</span></div><p>{next.issue?.message||next.reason||row.message}</p>
        {next.issue&&next.environmentBlocker?.message&&<p className="subtle">{next.environmentBlocker.message}</p>}
        {next.kind!=='complete'&&!!next.progress?.total&&<div className="pipeline-progress"><progress aria-label={`${title.title} ${next.stage}`} max={next.progress.total} value={Math.min(next.progress.done,next.progress.total)}/>
          {next.progress.unit==='bytes'&&<span>{transferProgress(next.progress.done,next.progress.total)}</span>}</div>}</div>
        <div className="attention-actions">{next.canContinue?<button disabled={disabled} onClick={()=>void enqueue([row])}>{next.action==='archive'?'重试整理':'继续处理'}</button>:
          next.action==='resume'?<button disabled={disabled} onClick={()=>void enqueue([row],'resume')}>继续此视频</button>:
          next.kind==='automatic'&&next.canPause!==false?<button className="secondary" disabled={disabled} onClick={()=>void enqueue([row],'pause')}>暂停此视频</button>:
          next.kind==='manual'||next.action==='open'?<button disabled={!!busy} onClick={()=>open(row)}>{next.kind==='manual'?'处理此视频':'查看详情'}</button>:null}
          {next.kind!=='complete'&&next.kind!=='manual'&&next.action!=='open'&&<button className="secondary" onClick={()=>open(row)}>查看详情</button>}</div></li>;
    })}</ul>:<p className="attention-empty">{filter==='attention'?'没有需要你处理的视频。':'当前没有符合条件的视频。'}</p>}
    {state&&state.pages>1&&<div className="pagination"><button className="secondary" disabled={!current||state.page<=1} onClick={()=>setPage(state.page-1)}>上一页</button>
      <span>第 {state.page}/{state.pages} 页 · {state.total} 个视频</span><button className="secondary" disabled={!current||state.page>=state.pages} onClick={()=>setPage(state.page+1)}>下一页</button></div>}
    </>}
  </section></>;
}
