import {useRecordChanges} from './RecordOperations';
import { useCallback, useEffect, useRef, useState } from 'react';
import DateSection, {useDateSectionOpen} from './DateSection';
import RecordDelete from './RecordDelete';
import RecordBatch, {toggleRecord,reconcileRecordChoices} from './RecordBatch';
import type {RecordChoice} from './RecordBatch';
import { groupSubmissionDays, submissionTime } from './submission-dates';
import { usePolling } from './usePolling';
import './submission-records.css';
import {createReadCycle,localJson} from './local-api';
import {historyRefreshMs} from './history-polling';
import {viewMemory} from './view-memory';

type Item={id?:string;deletionKey:string;revision:number;videoName?:string;startTime?:string;endTime?:string;status?:string;camera?:string;reason?:string;message?:string;taskId?:string};
type Receipt={id:string;title:string;submittedAt:string;revision:number;status?:string;submitted?:number;total?:number;
  detail?:{message?:string;notice?:string;jobId?:string;items?:Item[];page?:number;pages?:number;total?:number}};
const statusText:Record<string,string>={running:'处理中',complete:'已结束',attention:'待核对',recorded:'已记录',stopped:'已停止',submitted:'已提交',pending:'待处理',submitting:'提交中',uncertain:'待核对',duplicate:'已有任务',failed:'失败'};
async function load<T>(path:string,body?:object,signal?:AbortSignal):Promise<T>{
  return localJson<T>(`/api/submissions${path}`,body,{signal});
}

function ReceiptDetails({row,revision,onRemove,selected,onSelect,disabled}:{row:Receipt;revision:number;onRemove:(item:Item,requestId:string)=>Promise<void>;selected:Map<string,RecordChoice>;onSelect:(choice:RecordChoice)=>void;disabled:boolean}){
  const [page,setPage]=useState(()=>viewMemory.read(`receipt-page:${row.id}`,1));
  const [value,setValue]=useState<Receipt|null>(null);
  const [error,setError]=useState('');
  const [loading,setLoading]=useState(false);
  const reads=useRef(createReadCycle());
  const expanded=useDateSectionOpen();
  const loadedKey=useRef('');
  const key=`${row.id}:${page}:${revision}:${row.revision}:${row.status}:${row.total}:${row.submitted}`;
  const refresh=useCallback(async()=>{
    await reads.current.run(key,async context=>{
      if(context.current()){setLoading(true);setError('');}
      try{const result=await load<Receipt>(`/${row.id}?page=${page}&page_size=20`,undefined,context.signal);
        if(context.current()){setValue(result);setError('');loadedKey.current=key;}}
      catch(failure){if(context.current())setError((failure as Error).message);}
      finally{if(context.current())setLoading(false);}
    });
  },[row.id,page,key]);
  useEffect(()=>{
    // Reopening immutable history is an explicit request for fresh details:
    // another browser or a completed operation may have changed visibility.
    if(expanded)void refresh();
    return()=>reads.current.cancel();
  },[expanded,key,refresh]);
  usePolling(refresh,historyRefreshMs(expanded,[row.status||value?.status]),{immediate:false});
  function changePage(next:number){reads.current.cancel();setValue(null);viewMemory.save(`receipt-page:${row.id}`,next);setPage(next);}
  const detail=value?.detail;
  const actionsDisabled=disabled||loading||!!error||loadedKey.current!==key;
  return <div className="submission-detail" aria-label="采集提交详情" aria-busy={loading}>
    {error&&<div role="alert"><p className="error">{error}</p><button className="link" onClick={()=>void refresh()}>重新读取详情</button></div>}
    {!value&&!error&&<p className="submission-loading" role="status">正在读取提交详情…</p>}
    {detail?.notice&&<p className="notice">{detail.notice}</p>}{detail?.message&&<p className="submission-message">{detail.message}</p>}
    {!!detail?.items?.length&&<div className="submission-items">{detail.items.map(item=><article className="submission-item" key={item.deletionKey}>
      <div className="submission-item-head"><label className="record-checkbox"><input type="checkbox" aria-label={`选择提交明细 ${item.videoName||item.deletionKey}`} disabled={actionsDisabled} checked={selected.has(`item:${item.deletionKey}`)} onChange={()=>onSelect({key:`item:${item.deletionKey}`,id:item.deletionKey,revision:item.revision,kind:'item'})}/><strong>{item.videoName||'未命名视频'}</strong></label><span className={`submission-state ${item.status||''}`}>{statusText[item.status||'']||item.status||'状态未记录'}</span></div>
      <dl><div><dt>采集开始</dt><dd>{item.startTime||'未记录'}</dd></div><div><dt>采集结束</dt><dd>{item.endTime||'未记录'}</dd></div>
        {item.camera&&<div><dt>摄像头</dt><dd>{item.camera}</dd></div>}{item.taskId&&<div><dt>平台任务</dt><dd>{item.taskId}</dd></div>}</dl>
      {(item.reason||item.message)&&<p className="submission-item-message">{item.reason||item.message}</p>}
      <RecordDelete name={`${item.videoName} · 提交明细`} version={`${item.deletionKey}:${item.revision}`} disabled={actionsDisabled} onDelete={requestId=>onRemove(item,requestId)}/>
    </article>)}</div>}
    {value&&!detail?.items?.length&&!detail?.notice&&!error&&<p className="subtle">没有可显示的视频明细，提交凭证仍保留。</p>}
    {(detail?.pages||0)>1&&<nav className="submission-pagination" aria-label="提交详情分页"><span>共 {detail?.total} 项 · {detail?.page} / {detail?.pages} 页</span><div>
      <button className="secondary" disabled={(detail?.page||1)<=1} onClick={()=>changePage((detail?.page||1)-1)}>上一页</button>
      <button className="secondary" disabled={(detail?.page||1)>=(detail?.pages||1)} onClick={()=>changePage((detail?.page||1)+1)}>下一页</button></div></nav>}
  </div>;
}

export default function SubmissionRecords({kind,onChanged}:{kind:'collection'|'slicing';onChanged?:()=>void}){
  const [rows,setRows]=useState<Receipt[]>([]);
  const [error,setError]=useState('');
  const [working,setWorking]=useState(false);
  const [loaded,setLoaded]=useState(false);
  const [revision,setRevision]=useState(0);
  const [selected,setSelected]=useState<Map<string,RecordChoice>>(new Map());
  const reads=useRef(createReadCycle());
  const lock=useRef(false);
  const refresh=useCallback(async()=>{
    await reads.current.run(kind,async context=>{
      try{const result=await load<Receipt[]>(`?kind=${kind}`,undefined,context.signal);
        if(!Array.isArray(result))throw new Error('提交记录暂不可用，请稍后重试读取。');
        if(context.current()){setRows(result);setError('');setLoaded(true);}}
      catch(failure){if(context.current())setError((failure as Error).message);}
    });
  },[kind]);
  useRecordChanges(refresh);
  useEffect(()=>{void refresh();return()=>reads.current.cancel();},[refresh]);
  useEffect(()=>{setSelected(previous=>{
    const current=rows.map(row=>({key:`receipt:${row.id}`,id:row.id,revision:row.revision,kind:'receipt' as const}));
    const next=reconcileRecordChoices(previous,current,new Set([...previous.keys()].filter(key=>key.startsWith('receipt:'))));
    const validParents=new Set(rows.map(row=>row.id));
    const retained=new Map([...next].filter(([,choice])=>choice.kind!=='item'||validParents.has(choice.id.split(':')[0])));
    return retained.size===next.size?next:retained;
  });},[rows]);
  usePolling(async()=>{if(!lock.current)await refresh();},historyRefreshMs(true,rows.map(row=>row.status)),{immediate:false});
  async function action(task:()=>Promise<void>){
    if(lock.current)throw new Error('正在处理，请稍候。');
    lock.current=true;reads.current.cancel();setWorking(true);setError('');
    try{await task();setRevision(before=>before+1);void refresh();onChanged?.();}
    catch(failure){setError((failure as Error).message);throw failure;}
    finally{lock.current=false;setWorking(false);}
  }
  async function remove(scope:'records'|'items',selections:{id:string;expectedRevision:number}[],_name:string,requestId:string){
    await action(async()=>{
      await load<{removalId:string}>(`/${scope}/remove`,{requestId,selections});

      setSelected(before=>{const next=new Map(before);selections.forEach(item=>{next.delete(`${scope==='items'?'item':'receipt'}:${item.id}`);if(scope==='records')for(const key of next.keys())if(key.startsWith(`item:${item.id}:`))next.delete(key);});return next;});
    });
  }
  async function removeSelected(choices:RecordChoice[],requestId:string){
    await action(async()=>{await load<{removalId:string}>('/remove-group',{requestId,
      selections:choices.filter(c=>c.kind==='item').map(c=>({id:c.id,expectedRevision:c.revision})),
      receipts:choices.filter(c=>c.kind==='receipt').map(c=>({id:c.id,expectedRevision:c.revision}))});

      setSelected(new Map());});
  }
  const dates=groupSubmissionDays(rows,row=>row.submittedAt);
  return <section className="panel submission-records" aria-label="采集提交记录">
    <div className="section-title"><h2>采集提交记录</h2></div>
    {error&&<p className="error" role="alert">{error}</p>}
    {!loaded&&!error&&<p className="submission-loading" role="status">正在读取记录…</p>}
    {loaded&&!rows.length&&<p className="subtle">暂无提交记录。</p>}
    <RecordBatch selected={selected} disabled={working||!loaded} total={rows.length} scope="提交记录"
      onSelectAll={()=>setSelected(new Map(rows.map(row=>[`receipt:${row.id}`,{key:`receipt:${row.id}`,id:row.id,revision:row.revision,kind:'receipt'}])))}
      onClear={()=>setSelected(new Map())} onDelete={removeSelected}/>
    <div className="date-list">{dates.map(date=><DateSection key={date.day} day={date.day} rememberKey={`submissions:${kind}:${date.day}`} summary={`${date.records.length} 次提交 · ${date.records.reduce((sum,row)=>sum+(row.total||0),0)} 条视频`}>
      <div className="date-tools"><p>以下为当天提交的采集明细</p><button className="secondary" disabled={working} onClick={()=>setSelected(before=>{const next=new Map(before);date.records.forEach(row=>next.set(`receipt:${row.id}`,{key:`receipt:${row.id}`,id:row.id,revision:row.revision,kind:'receipt'}));return next;})}>选择当天提交</button></div>
      {date.records.map(row=><section className="dated-batch" key={row.id}>
        <div className="dated-batch-heading"><div><label className="record-checkbox"><input type="checkbox" aria-label={`选择提交记录 ${row.title} ${submissionTime(row.submittedAt)}`} checked={selected.has(`receipt:${row.id}`)} onChange={()=>setSelected(before=>toggleRecord(before,{key:`receipt:${row.id}`,id:row.id,revision:row.revision,kind:'receipt'}))}/><strong>{submissionTime(row.submittedAt)} · {row.title}</strong></label><p>{statusText[row.status||'']||row.status||'已记录'} · {row.total||0} 条视频</p></div>
          <RecordDelete name={`${submissionTime(row.submittedAt)} · 本次提交`} disabled={working} version={`${row.id}:${row.revision}`}
            onDelete={requestId=>remove('records',[{id:row.id,expectedRevision:row.revision}],row.title,requestId)}/></div>
        <ReceiptDetails row={row} revision={revision} selected={selected} disabled={working} onSelect={choice=>setSelected(before=>toggleRecord(before,choice))} onRemove={(item,requestId)=>remove('items',[{id:item.deletionKey,expectedRevision:item.revision}],item.videoName||'提交明细',requestId)}/>
      </section>)}
    </DateSection>)}</div>
  </section>;
}
