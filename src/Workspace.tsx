import {useCallback,useEffect,useRef,useState} from 'react';
import {FolderOpen,ArrowRight,CheckCircle} from '@phosphor-icons/react';
import {usePolling} from './usePolling';
import {localJson} from './local-api';
import './workspace.css';

type Plan={id:string;root:string;parent:string;revision:number;state:string;message:string;bytes:number;totalItems:number;counts:Record<string,number>;preserved?:number};
type Asset={id:string;name:string;path:string;status:string;stage:string;message:string;cleanup:{state?:string;message?:string}};
type Action={state:string;stage?:string;message:string;total?:number;finished?:number;waiting?:number;failed?:number};
type State={enabled:boolean;revision:number;root:string;parent:string;online:boolean;maintenance:boolean;
  preservedUploadedAt?:string;lastAction?:Action|null;activity?:{stage:string;message:string;currentName?:string};
  totalBytes:number;branches:{name:string;files:number;bytes:number}[];assets:Asset[];migration:Plan|null;counts:Record<string,number>};
const size=(bytes:number)=>bytes>=1024**3?`${(bytes/1024**3).toFixed(1)} GB`:`${(bytes/1024**2).toFixed(1)} MB`;
const active=(action?:Action|null)=>!!action&&['queued','running','waiting'].includes(action.state);
const api=<T,>(path='',body?:object)=>localJson<T>(`/api/workspace${path}`,body);

export default function Workspace({onChanged}:{onChanged:()=>Promise<void>}){
  const [state,setState]=useState<State|null>(null),[parent,setParent]=useState(''),[plan,setPlan]=useState<Plan|null>(null);
  const [open,setOpen]=useState(false),[busy,setBusy]=useState(false),[error,setError]=useState(''),[readError,setReadError]=useState('');
  const [watching,setWatching]=useState(false);
  const dirty=useRef(false),locked=useRef(false),request=useRef<{key:string;id:string}|null>(null);
  const refresh=useCallback(async()=>{
    const value=await api<State>();setState(value);setReadError('');
    if(!dirty.current&&value.parent)setParent(value.parent);
    if(value.migration?.state!=='preview')setPlan(null);
    else if(!dirty.current&&value.migration.revision===value.revision)setPlan(value.migration);
  },[]);
  useEffect(()=>{void refresh().catch(f=>setReadError((f as Error).message));},[refresh]);
  usePolling(refresh,state?.maintenance||active(state?.lastAction)?2000:15000,{immediate:false,onError:f=>setReadError((f as Error).message)});
  async function act(key:string,fn:(id:string)=>Promise<void>){
    if(locked.current)return;
    locked.current=true;setBusy(true);setError('');
    if(request.current?.key!==key)request.current={key,id:crypto.randomUUID()};
    try{await fn(request.current.id);request.current=null;await onChanged().catch(()=>setReadError('操作已登记，等待状态刷新。'));}
    catch(failure){setError((failure as Error).message);}finally{locked.current=false;setBusy(false);}
  }
  const organizing=active(state?.lastAction),disabled=busy||!!state?.maintenance;
  const issues=(state?.assets||[]).filter(a=>['waiting','attention'].includes(a.status)||a.cleanup?.state==='attention');
  const organize=()=>{setWatching(true);void act('organize',async id=>setState(await api<State>('/reconcile',{expectedRevision:state!.revision,requestId:id})));};
  return <section className="panel workspace-panel" aria-label="文件工作区">
    <div className="workspace-heading"><div><h2><FolderOpen size={22} aria-hidden="true"/>文件工作区</h2>
      <p>{state?.enabled?state.root:'选择一个位置，统一管理视频、照片和压缩包。'}</p></div>
      <div className="workspace-status"><span>{state?.maintenance?'正在变更位置':state?.enabled?(state.online?'磁盘已连接':'等待原磁盘'):'尚未启用'}</span>
        {state?.enabled&&<button disabled={disabled||organizing} onClick={organize}>{organizing?'整理中…':'整理视频'}</button>}
        <button className="secondary" aria-expanded={open} onClick={()=>setOpen(!open)}>{open?'收起':'管理文件'}</button>
      </div></div>
    {state?.lastAction&&(open||watching||organizing||state.lastAction.state==='attention')&&<div className={`workspace-operation ${state.lastAction.state==='attention'?'needs-review':''}`} role="status">
      <p>{state.lastAction.message}</p>
      {organizing&&state.activity?.currentName&&<small>{state.activity.currentName} · {state.activity.message}</small>}
      {organizing&&state.lastAction.total!=null&&<progress aria-label="视频整理进度" max={Math.max(1,state.lastAction.total)} value={(state.lastAction.finished||0)+(state.lastAction.failed||0)}/>}
    </div>}
    {open&&<div className="workspace-body">
      {state?.preservedUploadedAt&&<p className="success">已上传历史视频保留原位。</p>}
      <details className="workspace-location-settings" open={state?.enabled?undefined:true}><summary>{state?.enabled?'更换保存位置':'设置保存位置'}</summary>
        <form className="workspace-location" onSubmit={event=>{event.preventDefault();void act(`preview-${parent}`,async id=>{
          setPlan(await api<Plan>('/preview',{parent:parent.trim(),expectedRevision:state?.revision??0,requestId:id}));
        });}}>
          <label>父目录<input value={parent} placeholder="/Volumes/你的外置盘" disabled={disabled} required onChange={event=>{
            dirty.current=true;setParent(event.target.value);setPlan(null);request.current=null;
          }}/></label><button className="secondary" disabled={disabled||!state||!parent.trim()||(state.enabled&&parent.trim()===state.parent)}>预览位置变更</button>
        </form>
      </details>
      {plan&&<div className="workspace-preview"><strong>整理到 {plan.root}</strong><p>{plan.totalItems} 个文件 · {size(plan.bytes)}。同盘移动，跨盘校验后转移。</p>
        <div className="actions"><button disabled={disabled} onClick={()=>void act(`apply-${plan.id}`,async id=>{
          setState(await api<State>(`/${plan.id}/apply`,{expectedRevision:plan.revision,requestId:id}));setPlan(null);dirty.current=false;
        })}>启用并整理</button><button className="link" disabled={disabled} onClick={()=>void act(`cancel-${plan.id}`,async id=>{
          setState(await api<State>(`/${plan.id}/cancel`,{expectedRevision:state?.revision,requestId:id}));setPlan(null);
        })}>取消预览</button></div></div>}
      {state?.maintenance&&state.migration&&<div className="workspace-migration" role="status"><p>{state.migration.message}</p>
        <progress aria-label="位置变更进度" max={Math.max(1,state.migration.totalItems)} value={state.migration.counts.complete||0}/></div>}
      <ul className="workspace-tree" aria-label="目录分支">{state?.branches.filter(b=>!b.name.startsWith('.')).map(branch=><li key={branch.name}>
        <span><ArrowRight size={16} aria-hidden="true"/>{branch.name}</span><small>{branch.files} 个文件 · {size(branch.bytes)}</small></li>)}</ul>
      <div className="workspace-foot"><p><CheckCircle size={18} aria-hidden="true"/>文件释放后自动整理，满足全部核验条件后清理。</p>
        <p>登记文件 {size(state?.totalBytes||0)} · 保留 {state?.counts.preserved||0} 条 · 已清理 {state?.counts.cleaned||0} 条</p></div>
      {!!issues.length&&<details className="workspace-issues"><summary>等待与异常 · {issues.length} 条</summary><ul>{issues.map(a=><li key={a.id}>
        <strong>{a.name}</strong><p>{a.message||a.cleanup?.message}</p><code>{a.path}</code></li>)}</ul></details>}
      <details className="workspace-issues"><summary>文件保留原因</summary><ul>{state?.assets.filter(a=>a.cleanup?.state==='waiting').map(a=><li key={a.id}>
        <strong>{a.name}</strong><p>{a.cleanup.message}</p></li>)}</ul></details>
      {state?.migration&&!state.maintenance&&state.migration.state!=='preview'&&<details className="workspace-issues"><summary>历史位置变更记录</summary>
        <p>{state.migration.message} {state.migration.counts.complete||0}/{state.migration.totalItems}</p><p className="subtle">这是当时的记录，当前整理状态以上方为准。</p></details>}
    </div>}
    {(error||readError)&&<p className="error" role="alert">{error||readError}</p>}
  </section>;
}
