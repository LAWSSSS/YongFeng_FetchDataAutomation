import {useCallback,useEffect,useRef,useState} from 'react';
import {cancelledRead,createReadCycle,localJson} from './local-api';
import {usePolling} from './usePolling';
import {requestIntents} from './request-intent';
import {readInterval} from './view-preferences';
type Operation={operationId:string;state:string;total:number;finished?:number;message?:string;totalFiles?:number;
  deletedFiles?:number;deletedBytes?:number;remainingBytes?:number;reasonCode?:string;currentName?:string};
type State={revision:number;availability?:{online:boolean;reasonCode?:string;message?:string};
  cleanupCompleted?:{eligibleCount:number;eligibleBytes:number;latestOperation?:Operation}};
const active=(operation:Operation|null)=>!!operation&&['queued','running','waiting','waiting_connection'].includes(operation.state);
const reasons:Record<string,string>={disk_offline:'等待工作区磁盘恢复',disk_io:'等待磁盘读写恢复',file_busy:'等待文件使用结束',paused:'已暂停，清理范围保留',
  file_changed:'文件已变化，保留待核对',receipt_changed:'成果凭证已变化，保留待核对',file_missing:'文件位置需核对'};
const bytes=(value:number)=>value>=1024**3?`${(value/1024**3).toFixed(2)} GB`:`${(value/1024**2).toFixed(1)} MB`;
export default function CompletedCleanup(){
  const [state,setState]=useState<State|null>(null),[busy,setBusy]=useState(false),[error,setError]=useState(''),[operation,setOperation]=useState<Operation|null>(null);
  const reads=useRef(createReadCycle()),locked=useRef(false),intents=useRef(requestIntents());
  const refresh=useCallback(async()=>{if(locked.current)return;await reads.current.run('cleanup',async context=>{
    try{const value=await localJson<State>('/api/workspace/cleanup-summary',undefined,{signal:context.signal});if(context.current()){setState(value);setError('');setOperation(value.cleanupCompleted?.latestOperation||null);}}
    catch(failure){if(context.current()&&!cancelledRead(failure))setError((failure as Error).message);}
  });},[]);
  useEffect(()=>{void refresh();return()=>reads.current.cancel();},[refresh]);
  const offline=state?.availability?.online===false||operation?.reasonCode==='disk_offline';
  usePolling(refresh,readInterval(active(operation)&&!offline,offline),{immediate:false});
  const pending=state?.cleanupCompleted;
  async function clean(){
    if(!state||locked.current||active(operation)||!pending?.eligibleCount)return;
    if(!window.confirm(`清理当前 ${pending.eligibleCount} 个已结束项目（约 ${bytes(pending.eligibleBytes)}）的本地下载视频、照片、ZIP及临时文件。云端成果和历史保留；之后完成的项目不会被加入本次清理。`))return;
    locked.current=true;setBusy(true);setError('');reads.current.cancel();
    try{const result=await localJson<Operation>('/api/workspace/cleanup-completed',intents.current.payload('cleanup',{expectedRevision:state.revision}));
      intents.current.complete('cleanup');setOperation(result);}
    catch(failure){setError((failure as Error).message);}
    finally{locked.current=false;setBusy(false);void refresh();}
  }
  if(!pending?.eligibleCount&&!active(operation)&&!error)return null;
  return <section className="panel cleanup-completed"><div className="section-title"><div><h2>本地文件</h2></div>
    <button className="secondary" disabled={busy||active(operation)||!pending?.eligibleCount} onClick={()=>void clean()}>{busy?'正在登记…':active(operation)?'本次清理已登记':`清理已结束文件${pending?`（${pending.eligibleCount}）`:''}`}</button></div>
    {pending&&<p className="subtle">{active(operation)?'下一次可清理':'可清理'} {pending.eligibleCount} 个项目，约 {bytes(pending.eligibleBytes)}；原始目录、模型和云端成果保留。</p>}
    {operation&&<div role="status"><p>{operation.message||`本次固定范围 ${operation.total} 个项目。`}</p>
      {operation.totalFiles!==undefined&&<><p>已清理 {operation.deletedFiles||0} / {operation.totalFiles} 个文件 · 已释放 {bytes(operation.deletedBytes||0)} · 剩余 {bytes(operation.remainingBytes||0)}</p>
        {active(operation)&&operation.totalFiles>0&&<progress aria-label="本次清理文件进度" max={operation.totalFiles} value={Math.min(operation.deletedFiles||0,operation.totalFiles)}/>}</>}
      {operation.currentName&&<p className="subtle">当前：{operation.currentName}</p>}
      {operation.reasonCode&&<p className="subtle">{reasons[operation.reasonCode]||'本次清理等待核对；文件保留。'}</p>}
    </div>}
    {error&&<p className="error" role="alert">{error}</p>}
  </section>;
}
