import RecordDelete from './RecordDelete';
import {useState} from 'react';
import './record-actions.css';

import type {RecordChoice} from './record-choices';
export {toggleRecord,reconcileRecordChoices} from './record-choices';
export type {RecordChoice} from './record-choices';
export default function RecordBatch({selected,disabled,onClear,onDelete,onSelectAll,total=0,scope='当前列表',selecting:controlledSelecting,onSelectingChange,description,entryLabel='批量管理'}:{selected:Map<string,RecordChoice>;disabled:boolean;
  onClear:()=>void;onDelete:(choices:RecordChoice[],requestId:string)=>Promise<void>;
  onSelectAll?:()=>void;total?:number;scope?:string;selecting?:boolean;onSelectingChange?:(value:boolean)=>void;description?:string;entryLabel?:string}) {
  const [removing,setRemoving]=useState(false);
  const [localSelecting,setLocalSelecting]=useState(false);
  const selecting=controlledSelecting??localSelecting;
  const setSelecting=(value:boolean)=>{setLocalSelecting(value);onSelectingChange?.(value);};
  const choices=[...selected.values()];
  const version=choices.map(choice=>`${choice.key}:${choice.revision}`).sort().join('|');
  const locked=disabled||removing;
  if(!total&&!choices.length)return null;
  if(!selecting&&!choices.length)return <div className="batch-entry"><button type="button" className="secondary" disabled={disabled} onClick={()=>setSelecting(true)}>{entryLabel}</button></div>;
  return <div data-selecting="true" className="record-batch record-batch-management" aria-label="批量记录操作">
    <div className="record-batch-summary"><strong>批量管理</strong><span role="status">已选 {choices.length} 条记录</span><small>{scope}</small></div>
    <div className="record-batch-actions">
      {onSelectAll&&<button type="button" className="secondary" disabled={locked||!total} onClick={onSelectAll}>全选{scope}</button>}
      <button type="button" className="secondary" disabled={locked} onClick={()=>{onClear();setSelecting(false);}}>结束选择</button>
      <RecordDelete name={`所选 ${choices.length} 条记录`} label="删除所选记录" description={description} version={version} disabled={locked||!choices.length}
        onDelete={async requestId=>{setRemoving(true);try{await onDelete(choices,requestId);}finally{setRemoving(false);}}}/>
    </div></div>;
}
