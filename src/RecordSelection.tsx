import {useState} from 'react';
import RecordDelete from './RecordDelete';
import {canDownload,selectVideoScope} from './record-groups';
import type {BatchVideo} from './record-groups';

export default function RecordSelection<T extends BatchVideo>({videos,selectedIds,busy,selecting,onSelectingChange,onSelect,onClear,onDownload,onSave,onRemove}:{
  videos:T[];selectedIds:ReadonlySet<string>;busy:boolean;onSelect:(videos:T[])=>void;onClear:()=>void;
  onDownload:()=>void;onSave:()=>void;onRemove:(requestId:string)=>Promise<void>;
  selecting:boolean;onSelectingChange:(value:boolean)=>void;
}) {
  const [scope,setScope]=useState('all');
  const [removing,setRemoving]=useState(false);
  const locked=busy||removing;
  const eligible=selectVideoScope(videos,scope);
  const selected=videos.filter(v=>selectedIds.has(v.id));
  const count=selected.length;
  const downloadable=selected.some(canDownload);
  const capturing=selected.some(video=>video.capabilities?.captureInFlight);
  const saveable=selected.some(v=>{const active=v.versions.find(x=>x.revision===v.activeRevision);return active?.state!=='saved'&&!!active?.recovery;});
  if(!videos.length)return null;
  if(!selecting&&!count)return <div className="batch-entry"><button className="secondary" disabled={busy} onClick={()=>onSelectingChange(true)}>批量管理</button></div>;
  return <div data-selecting="true" className="selection-bar record-batch-management" aria-label="批量记录操作">
    <div className="record-batch-summary"><strong>批量管理</strong><span role="status">已选 {count} 条视频</span></div>
    <div className="record-batch-actions"><label className="selection-scope">选择范围<select value={scope} disabled={locked} onChange={e=>{setScope(e.target.value);onClear();}}>
      <option value="all">当前筛选结果</option><option value="downloadable">待确认当前版</option><option value="partial">已完成可用部分</option>
    </select></label>
    <button className="secondary" disabled={locked||!eligible.length} onClick={()=>onSelect(eligible)}>全选 {eligible.length} 条</button>
    <button className="secondary" disabled={locked} onClick={()=>{onClear();onSelectingChange(false);}}>结束选择</button>
    <RecordDelete name={`所选 ${count} 条视频记录`} label="批量删除记录" version={selected.map(v=>`${v.id}:${v.revision}`).sort().join("|")} disabled={locked||!count||capturing}
      onDelete={async requestId=>{setRemoving(true);try{await onRemove(requestId);}finally{setRemoving(false);}}}/>
    {capturing&&<span className="subtle">正在采集的项目可在详情中终止并删除。</span>}
    {!!count&&<div className="actions">{downloadable&&<button disabled={locked} onClick={onDownload}>确认所选并处理</button>}
      {saveable&&<button className="secondary" disabled={locked} onClick={onSave}>归类到已保存</button>}</div>}</div>
  </div>;
}
