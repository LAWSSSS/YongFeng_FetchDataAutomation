import { useCallback, useEffect, useRef, useState } from 'react';
import { FolderOpen, ArrowRight, CheckCircle } from '@phosphor-icons/react';
import { usePolling } from './usePolling';
import './workspace.css';

type Move = {id:string;source:string;destination:string;state:string;message?:string;size:number};
type Plan = {id:string;root:string;parent:string;revision:number;state:string;message:string;bytes:number;totalItems:number;
  counts:Record<string,number>;items:Move[];preserved?:number};
type Asset = {id:string;name:string;path:string;status:string;stage:string;message:string;
  cleanup:{state?:string;message?:string}};
type State = {enabled:boolean;revision:number;root:string;parent:string;online:boolean;maintenance:boolean;
  preservedUploadedAt?:string;
  totalBytes:number;branches:{name:string;path:string;files:number;bytes:number}[];assets:Asset[];
  migration:Plan|null;counts:Record<string,number>};
const size = (bytes:number) => bytes >= 1024**3 ? `${(bytes/1024**3).toFixed(1)} GB` : `${(bytes/1024**2).toFixed(1)} MB`;
async function api<T>(path='', body?:object):Promise<T> {
  const response = await fetch(`/api/workspace${path}`,body===undefined ? undefined :
    {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  if (!response.ok) {const data=await response.json();throw new Error(data.message||'工作区暂不可用，请稍后重试。');}
  return response.json();
}

export default function Workspace({onChanged}:{onChanged:()=>Promise<void>}) {
  const [state,setState]=useState<State|null>(null);
  const [parent,setParent]=useState('');
  const dirty=useRef(false);
  const [plan,setPlan]=useState<Plan|null>(null);
  const [busy,setBusy]=useState(false);
  const [error,setError]=useState('');
  const [open,setOpen]=useState(false);
  const request=useRef<{key:string;id:string}|null>(null);
  const refresh=useCallback(async()=>{
    const value=await api<State>();setState(value);
    if (!dirty.current && value.parent) setParent(value.parent);
    if (value.migration?.state!=='preview') setPlan(null);
    else if (!dirty.current && value.migration.revision===value.revision) setPlan(value.migration);
  },[]);
  useEffect(()=>{void refresh().catch(e=>setError((e as Error).message));},[refresh]);
  usePolling(refresh,state?.maintenance?2000:15000,{immediate:false,onError:e=>setError((e as Error).message)});
  async function action(key:string, fn:(id:string)=>Promise<void>) {
    if (!request.current || request.current.key!==key) request.current={key,id:crypto.randomUUID()};
    setBusy(true);setError('');
    try {await fn(request.current.id);request.current=null;await onChanged();}
    catch(e){setError((e as Error).message);}
    finally{setBusy(false);}
  }
  const migration=plan||state?.migration;
  const disabled=busy||!!state?.maintenance;
  return <section className="panel workspace-panel" aria-label="文件工作区">
    <div className="workspace-heading">
      <div><h2><FolderOpen size={22} aria-hidden="true"/>文件工作区</h2>
        <p>{state?.enabled?state.root:'选一个位置，统一管理原视频、照片和压缩包。'}</p></div>
      <div className="workspace-status"><span>{state?.maintenance?'正在整理':state?.enabled?(state.online?'磁盘已连接':'等待原磁盘'):'尚未启用'}</span>
        <button className="secondary" type="button" aria-expanded={open} onClick={()=>setOpen(!open)}>{open?'收起':'管理文件'}</button></div>
    </div>
    {open&&<div className="workspace-body">
      {state?.preservedUploadedAt&&<p className="success" role="status">已上传历史视频保留原位，跳过整理和本地清理。切片和图片导入继续。</p>}
      <form className="workspace-location" onSubmit={e=>{e.preventDefault();void action(`preview-${parent}`,async id=>{
        const value=await api<Plan>('/preview',{parent:parent.trim(),expectedRevision:state?.revision??0,requestId:id});setPlan(value);
      });}}>
        <label>保存位置<input value={parent} onChange={e=>{dirty.current=true;setParent(e.target.value);setPlan(null);request.current=null;}} placeholder="/Volumes/你的外置盘" disabled={disabled} required /></label>
        <button type="submit" className="secondary" disabled={disabled||!state||!parent.trim()}>预览整理</button>
      </form>
      {plan&&<div className="workspace-preview" role="status">
        <strong>整理到 {plan.root}</strong><p>{plan.totalItems} 个已登记文件 · {size(plan.bytes)}。{(plan.preserved||0)>0&&`已跳过 ${plan.preserved} 条保留视频。`}同盘剪切，跨盘校验后再移除源文件。</p>
        <div className="actions"><button disabled={disabled} onClick={()=>void action(`apply-${plan.id}`,async id=>{
          setState(await api<State>(`/${plan.id}/apply`,{expectedRevision:plan.revision,requestId:id}));setPlan(null);dirty.current=false;
        })}>启用并整理</button><button className="link" disabled={disabled} onClick={()=>void action(`cancel-${plan.id}`,async id=>{
          setState(await api<State>(`/${plan.id}/cancel`,{expectedRevision:state?.revision,requestId:id}));setPlan(null);
        })}>取消预览</button></div>
      </div>}
      {migration&&migration.state!=='preview'&&<div className="workspace-migration" role="status">
        <p>{migration.message} {!['superseded','cancelled'].includes(migration.state)&&<strong>{migration.counts.complete||0}/{migration.totalItems}</strong>}</p>
        {state?.maintenance&&<progress aria-label="整理进度" max={Math.max(1,migration.totalItems)} value={migration.counts.complete||0}/>}
        {migration.state==='attention'&&<button className="secondary" disabled={disabled} onClick={()=>void action(`retry-${migration.id}`,async id=>{
          setState(await api<State>(`/${migration.id}/retry`,{expectedRevision:state?.revision,requestId:id}));
        })}>重新核对并整理</button>}
      </div>}
      <ul className="workspace-tree" aria-label="目录分支">
        {(state?.branches||[]).filter(b=>!b.name.startsWith('.')).map(branch=><li key={branch.name}>
          <span><ArrowRight size={16} aria-hidden="true"/>{branch.name}</span><small>{branch.files} 个文件 · {size(branch.bytes)}</small>
        </li>)}
      </ul>
      <div className="workspace-foot"><p><CheckCircle size={18} aria-hidden="true"/>新视频只保留一份，全部核验后自动清理。已设为保留的视频保持原位，历史记录保留。</p>
        <p>工作区内 {size(state?.totalBytes||0)} · 保留原位 {state?.counts.preserved||0} 条 · 已清理 {state?.counts.cleaned||0} 条 · 需处理 {state?.counts.attention||0} 条</p>
        {state?.enabled&&<button className="link" disabled={disabled||!state.online} onClick={()=>void action('reconcile',async id=>{
          setState(await api<State>('/reconcile',{expectedRevision:state.revision,requestId:id}));
        })}>重新核对文件</button>}
      </div>
      {!!state?.assets.filter(a=>a.status==='attention'||a.cleanup.state==='attention').length&&<details className="workspace-issues"><summary>查看需处理文件</summary><ul>
        {state.assets.filter(a=>a.status==='attention'||a.cleanup.state==='attention').map(a=><li key={a.id}><strong>{a.name}</strong><p>{a.message||a.cleanup.message}</p><code>{a.path}</code></li>)}
      </ul></details>}
      <details className="workspace-issues"><summary>查看等待清理的原因</summary><ul>
        {(state?.assets||[]).filter(a=>a.cleanup.state==='waiting').map(a=><li key={a.id}><strong>{a.name}</strong><p>{a.cleanup.message}</p></li>)}
      </ul></details>
    </div>}
    {error&&<p className="error" role="alert">{error}</p>}
  </section>;
}
