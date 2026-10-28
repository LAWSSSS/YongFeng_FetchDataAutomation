import {useState} from 'react';
import {canDownload,selectVideoScope} from './record-groups';
import type {BatchVideo} from './record-groups';

export default function RecordSelection<T extends BatchVideo>({videos,selectedIds,busy,downloadEnabled,onSelect,onClear,onDownload,onSave,onRemove}:{
  videos:T[];selectedIds:ReadonlySet<string>;busy:boolean;downloadEnabled:boolean;onSelect:(videos:T[])=>void;onClear:()=>void;
  onDownload:()=>void;onSave:()=>void;onRemove:()=>void;
}) {
  const [scope,setScope]=useState('all');
  const eligible=selectVideoScope(videos,scope);
  const count=selectedIds.size;
  const selected=videos.filter(v=>selectedIds.has(v.id));
  const downloadable=selected.some(canDownload);
  const saveable=selected.some(v=>{const active=v.versions.find(x=>x.revision===v.activeRevision);return active?.state==='saved'||!!active?.recovery;});
  return <div className="selection-bar" aria-label="批量记录操作">
    <label className="selection-scope">选择范围<select value={scope} disabled={busy} onChange={e=>setScope(e.target.value)}>
      <option value="all">当前筛选结果</option><option value="downloadable">可下载当前版</option><option value="partial">已完成可用部分</option>
    </select></label>
    <button className="secondary" disabled={busy||!eligible.length} onClick={()=>onSelect(eligible)}>全选 {eligible.length} 条</button>
    <span role="status">已选 {count} 条</span>
    {!!count&&<><button className="link" disabled={busy} onClick={onClear}>取消选择</button>
      <div className="actions">{downloadable&&<button disabled={busy||!downloadEnabled} onClick={onDownload}>下载所选</button>}
        {saveable&&<button className="secondary" disabled={busy} onClick={onSave}>归类到已保存</button>}
        <button className="link" disabled={busy} onClick={onRemove}>清除所选</button></div></>}
  </div>;
}
