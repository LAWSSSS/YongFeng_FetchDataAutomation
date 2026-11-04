import {useRecordChanges} from './RecordOperations';
import { useCallback, useEffect, useRef, useState } from 'react';
import DateSection, {useDateSectionOpen} from './DateSection';
import RecordBatch from './RecordBatch';
import {reconcileRecordChoices,reconcileSubmissionChoices,selectSubmissionReceipts,toggleSubmissionChoice} from './record-choices';
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

function ReceiptDetails({row,selecting,selected,onSelect,onObserved,disabled}:{row:Receipt;selecting:boolean;selected:Map<string,RecordChoice>;onSelect:(choice:RecordChoice)=>void;onObserved:(choices:RecordChoice[],known:Set<string>)=>void;disabled:boolean}){
  const [page,setPage]=useState(()=>viewMemory.read(`receipt-page:${row.id}`,1));
  const [value,setValue]=useState<Receipt|null>(null);
  const [error,setError]=useState('');
  const [loading,setLoading]=useState(false);
  const reads=useRef(createReadCycle());
  const expanded=useDateSectionOpen();
  const loadedKey=useRef('');
  const previousItems=useRef(new Set<string>());
  const key=`${row.id}:${page}:${row.revision}:${row.status}:${row.total}:${row.submitted}`;
  const refresh=useCallback(async()=>{
    await reads.current.run(key,async context=>{
      if(context.current()){setLoading(true);setError('');}
      try{const result=await load<Receipt>(`/${row.id}?page=${page}&page_size=20`,undefined,context.signal);
        if(context.current()){
          const choices=(result.detail?.items||[]).map(item=>({key:`item:${item.deletionKey}`,id:item.deletionKey,revision:item.revision,kind:'item' as const,parentId:row.id,parentRevision:row.revision}));
          const known=new Set([...previousItems.current,...choices.map(choice=>choice.key)]);
          previousItems.current=new Set(choices.map(choice=>choice.key));
          onObserved(choices,known);
          setValue(result);setError('');loadedKey.current=key;
        }}
      catch(failure){if(context.current())setError((failure as Error).message);}
      finally{if(context.current())setLoading(false);}
    });
  },[row.id,row.revision,page,key,onObserved]);
  useEffect(()=>{
    // Reopening immutable history is an explicit request for fresh details:
    // another browser or a completed operation may have changed visibility.
    if(expanded)void refresh();
    return()=>reads.current.cancel();
  },[expanded,key,refresh]);
  usePolling(refresh,historyRefreshMs(expanded,[row.status||value?.status]),{immediate:false});
  function changePage(next:number){reads.current.cancel();previousItems.current.clear();setValue(null);viewMemory.save(`receipt-page:${row.id}`,next);setPage(next);}
  const detail=value?.detail;
  const actionsDisabled=disabled||loading||!!error||loadedKey.current!==key;
  return <div className="submission-detail" aria-label="采集提交详情" aria-busy={loading}>
    {error&&<div role="alert"><p className="error">{error}</p><button className="link" onClick={()=>void refresh()}>重新读取详情</button></div>}
    {!value&&!error&&<p className="submission-loading" role="status">正在读取提交详情…</p>}
    {detail?.notice&&<p className="notice">{detail.notice}</p>}{detail?.message&&<p className="submission-message">{detail.message}</p>}
    {!!detail?.items?.length&&<div className="submission-items">{detail.items.map(item=><article className="submission-item" key={item.deletionKey}>
      <div className="submission-item-head"><label className="record-checkbox">{selecting&&<input type="checkbox" aria-label={`选择提交明细 ${item.videoName||item.deletionKey}`} disabled={actionsDisabled||selected.has(`receipt:${row.id}`)} checked={selected.has(`receipt:${row.id}`)||selected.has(`item:${item.deletionKey}`)} onChange={()=>onSelect({key:`item:${item.deletionKey}`,id:item.deletionKey,revision:item.revision,kind:'item',parentId:row.id,parentRevision:row.revision})}/>}<strong>{item.videoName||'未命名视频'}</strong></label><span className={`submission-state ${item.status||''}`}>{statusText[item.status||'']||item.status||'状态未记录'}</span></div>
      <dl><div><dt>采集开始</dt><dd>{item.startTime||'未记录'}</dd></div><div><dt>采集结束</dt><dd>{item.endTime||'未记录'}</dd></div>
      </dl>
      {(item.camera||item.taskId)&&<details className="submission-proof"><summary>任务凭证</summary><dl>
        {item.camera&&<div><dt>摄像头</dt><dd>{item.camera}</dd></div>}{item.taskId&&<div><dt>平台任务</dt><dd>{item.taskId}</dd></div>}
      </dl></details>}
      {(item.reason||item.message)&&<p className="submission-item-message">{item.reason||item.message}</p>}
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
  const [selected,setSelected]=useState<Map<string,RecordChoice>>(new Map());
  const [selecting,setSelecting]=useState(false);
  const observeItems=useCallback((choices:RecordChoice[],known:Set<string>)=>{
    setSelected(previous=>reconcileRecordChoices(previous,choices,known));
  },[]);
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
  useEffect(()=>{
    if(kind!=='collection')return;
    const update=()=>{void refresh();};
    window.addEventListener('collection-submissions-updated',update);
    return()=>window.removeEventListener('collection-submissions-updated',update);
  },[kind,refresh]);
  useEffect(()=>{void refresh();return()=>reads.current.cancel();},[refresh]);
  useEffect(()=>{setSelected(previous=>reconcileSubmissionChoices(previous,rows));},[rows]);
  usePolling(async()=>{if(!lock.current)await refresh();},historyRefreshMs(true,rows.map(row=>row.status)),{immediate:false});
  async function action(task:()=>Promise<void>){
    if(lock.current)throw new Error('正在处理，请稍候。');
    lock.current=true;reads.current.cancel();setWorking(true);setError('');
    try{await task();void refresh();onChanged?.();}
    catch(failure){setError((failure as Error).message);throw failure;}
    finally{lock.current=false;setWorking(false);}
  }
  async function removeSelected(choices:RecordChoice[],requestId:string){
    await action(async()=>{await load<{removalId:string}>('/remove-group',{requestId,
      selections:choices.filter(c=>c.kind==='item').map(c=>({id:c.id,expectedRevision:c.revision})),
      receipts:choices.filter(c=>c.kind==='receipt').map(c=>({id:c.id,expectedRevision:c.revision}))});

      setSelected(new Map());});
  }
  const dates=groupSubmissionDays(rows,row=>row.submittedAt);
  return <section className="panel submission-records" data-selection-scope="submissions" aria-label="采集提交记录">
    <div className="section-title"><h2>采集提交记录</h2>
    <RecordBatch selected={selected} selecting={selecting} onSelectingChange={setSelecting} entryLabel="管理记录"
      description="只移除所选提交记录。视频、平台任务和后台进度保留。" disabled={working||!loaded||!!error} total={rows.length} scope="提交记录"
      onSelectAll={()=>setSelected(new Map(rows.map(row=>[`receipt:${row.id}`,{key:`receipt:${row.id}`,id:row.id,revision:row.revision,kind:'receipt'}])))}
      onClear={()=>setSelected(new Map())} onDelete={removeSelected}/></div>
    {error&&<div role="alert"><p className="error">{error}</p><button className="link" disabled={working} onClick={()=>void refresh()}>重新读取记录</button></div>}
    {!loaded&&!error&&<p className="submission-loading" role="status">正在读取记录…</p>}
    {loaded&&!rows.length&&<p className="subtle">暂无提交记录。</p>}
    <div className="date-list">{dates.map(date=><DateSection key={date.day} day={date.day} rememberKey={`submissions:${kind}:${date.day}`} summary={`${date.records.length} 次提交 · ${date.records.reduce((sum,row)=>sum+(row.total||0),0)} 条视频`}>
      {selecting&&<div className="date-tools"><button className="secondary" disabled={working||!!error} onClick={()=>setSelected(before=>selectSubmissionReceipts(before,date.records.map(row=>({key:`receipt:${row.id}`,id:row.id,revision:row.revision,kind:'receipt'}))))}>选择当天提交</button></div>}
      {date.records.map(row=><section className="dated-batch" key={row.id}>
        <div className="dated-batch-heading"><div><label className="record-checkbox">{selecting&&<input type="checkbox" aria-label={`选择提交记录 ${row.title} ${submissionTime(row.submittedAt)}`} disabled={working||!!error} checked={selected.has(`receipt:${row.id}`)} onChange={()=>setSelected(before=>toggleSubmissionChoice(before,{key:`receipt:${row.id}`,id:row.id,revision:row.revision,kind:'receipt'}))}/>}<strong>{submissionTime(row.submittedAt)} 提交</strong></label></div>
          <span className="submission-batch-status">{statusText[row.status||'']||row.status||'已记录'} · {row.total||0} 条视频</span></div>
        <ReceiptDetails row={row} selecting={selecting} selected={selected} disabled={working||!!error} onObserved={observeItems} onSelect={choice=>setSelected(before=>toggleSubmissionChoice(before,choice))}/>
      </section>)}
    </DateSection>)}</div>
  </section>;
}
