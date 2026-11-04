import {useCallback,useEffect,useState} from 'react';
import {localJson} from './local-api';
import {usePolling} from './usePolling';
type Operation={operationId:string;state:string;total:number;finished:number;
  outcomes:{id:string;name:string;state:string;message:string}[]};
const active=(op:Operation)=>['queued','running','waiting'].includes(op.state);
export function useRecordChanges(refresh:()=>unknown){
  useEffect(()=>{const update=()=>{void refresh();};window.addEventListener('records-updated',update);
    return()=>window.removeEventListener('records-updated',update);},[refresh]);
}
export default function RecordOperations(){
  const [operations,setOperations]=useState<Operation[]>(()=>{try{return JSON.parse(sessionStorage.getItem('record-operations')||'[]');}catch{return [];}});
  useEffect(()=>{const accept=(event:Event)=>{const op=(event as CustomEvent<Operation>).detail;
    setOperations(rows=>[...rows.filter(row=>row.operationId!==op.operationId&&active(row)),op]);};
    window.addEventListener('record-operation',accept);return()=>window.removeEventListener('record-operation',accept);},[]);
  useEffect(()=>{try{sessionStorage.setItem('record-operations',JSON.stringify(operations));}catch{/* Server retains accepted work. */}},[operations]);
  const refresh=useCallback(async()=>{
    for(const operation of operations.filter(active)){
      try{const value=await localJson<Operation>(`/api/record-operations/${encodeURIComponent(operation.operationId)}`);
        setOperations(rows=>rows.map(row=>row.operationId===value.operationId?value:row));
        if(!active(value))window.dispatchEvent(new Event('records-updated'));
      }catch(error){
        if((error as {status?:number}).status===404)setOperations(rows=>rows.map(row=>row.operationId===operation.operationId?
          {...row,state:'attention',outcomes:[{id:row.operationId,name:'删除操作',state:'attention',message:'此操作已归入历史，请刷新记录。'}]}:row));
        // Transient failures leave the durable operation pending.
      }
    }
  },[operations]);
  usePolling(refresh,operations.some(active)?2000:null,{immediate:false});
  if(!operations.length)return null;
  const latestComplete=operations.findLast(op=>op.state==='complete')?.operationId;
  const visible=operations.filter(op=>op.state!=='complete'||op.operationId===latestComplete);
  return <aside className="panel record-operation-notice" aria-live="polite">{visible.map(op=><div key={op.operationId}>
    <div className="section-title"><p>{active(op)?`正在处理删除 · ${op.finished}/${op.total}`:op.state==='complete'?`已移除 ${op.total} 条记录`:'部分记录未删除'}
      {op.state==='waiting'&&' · 等待安全清理'}</p>
      {!active(op)&&<button className="link" onClick={()=>setOperations(rows=>rows.filter(row=>op.state==='complete'?row.state!=='complete':row.operationId!==op.operationId))}>关闭提示</button>}</div>
    {op.outcomes?.some(row=>row.state==='attention'||row.message?.includes('本地文件保留'))&&<details><summary>查看处理结果</summary><ul>
      {op.outcomes.filter(row=>row.state==='attention'||row.message?.includes('本地文件保留')).map(row=><li key={row.id}>{row.name}：{row.message}</li>)}</ul></details>}
  </div>)}</aside>;
}
