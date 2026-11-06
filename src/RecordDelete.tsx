import { useRef, useState } from 'react';
import { Trash } from '@phosphor-icons/react';
import './record-actions.css';

export default function RecordDelete({name, version, disabled = false, onDelete, label='删除记录', confirmLabel='确认删除', description = '已结束项目同时清理已核验的本地文件；进行中项目仅移除记录。云端成果保留。'}: {
  name:string; version:string|number; disabled?:boolean;
  label?:string;confirmLabel?:string;description?:string; onDelete:(requestId:string)=>Promise<boolean|void>;
}) {
  const [confirmation,setConfirmation]=useState<{version:string|number;requestId:string}|null>(null);
  const [busy,setBusy]=useState(false);
  const [error,setError]=useState('');
  const trigger=useRef<HTMLButtonElement>(null);
  const locked=useRef(false);
  const stale=!!confirmation && confirmation.version!==version;
  async function remove() {
    if (!confirmation || stale || locked.current || disabled) return;
    locked.current=true;setBusy(true);setError('');
    try {if (await onDelete(confirmation.requestId)!==false) setConfirmation(null);}
    catch(failure){setError((failure as Error).message || '删除未完成，请重试。');}
    finally{locked.current=false;setBusy(false);}
  }
  return <div className="record-delete">
    {!confirmation?<button className="record-delete-trigger" ref={trigger} type="button" disabled={disabled}
      aria-label={`${label} ${name}`} onClick={()=>{setConfirmation({version,requestId:crypto.randomUUID()});setError('');}}>
      <Trash size={16} aria-hidden="true"/>{label}</button>:
      <div className="record-delete-confirm" role="group" aria-label={`确认${label} ${name}`}>
        <strong>{label} · {name}？</strong><p>{description}</p>
        {stale&&<p className="error" role="alert">记录已有新进度，请取消后重新选择。</p>}
        {error&&<p className="error" role="alert">{error}</p>}
        <div className="actions"><button className="secondary" type="button" disabled={busy} onClick={()=>{
          setConfirmation(null);setError('');requestAnimationFrame(()=>trigger.current?.focus());
        }}>取消</button><button className="danger" type="button" disabled={busy||disabled||stale} onClick={()=>void remove()}>
          {busy?'正在登记…':confirmLabel}</button></div>
      </div>}
  </div>;
}
