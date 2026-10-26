import { useCallback, useEffect, useRef, useState } from 'react';
import DateSection from './DateSection';
import RecordDelete from './RecordDelete';
import { groupSubmissionDays, submissionTime } from './submission-dates';
import { usePolling } from './usePolling';
import './submission-records.css';

type Item={id?:string;deletionKey:string;revision:number;videoName?:string;startTime?:string;endTime?:string;status?:string;camera?:string;reason?:string;message?:string;taskId?:string};
type Receipt={id:string;title:string;submittedAt:string;revision:number;status?:string;submitted?:number;total?:number;
  detail?:{message?:string;notice?:string;jobId?:string;items?:Item[];page?:number;pages?:number;total?:number}};
type Removal={id:string;name:string;scope:'records'|'items';restoreRequestId:string};
const statusText:Record<string,string>={running:'处理中',complete:'已结束',attention:'待核对',recorded:'已记录',stopped:'已停止',submitted:'已提交',pending:'待处理',submitting:'提交中',uncertain:'待核对',duplicate:'已有任务',failed:'失败'};
async function load<T>(path:string,body?:object):Promise<T>{
  let response:Response;
  try{response=await fetch(`/api/submissions${path}`,body===undefined?undefined:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});}
  catch{throw new Error('本地程序暂时无法连接，请重试。');}
  const value=await response.json().catch(()=>({}));
  if(!response.ok)throw new Error(value.message||value.detail||'提交记录暂时无法读取。');
  return value as T;
}

function ReceiptDetails({row,revision,onRemove}:{row:Receipt;revision:number;onRemove:(item:Item,requestId:string)=>Promise<void>}){
  const [page,setPage]=useState(1);
  const [value,setValue]=useState<Receipt|null>(null);
  const [error,setError]=useState('');
  const sequence=useRef(0);
  const refresh=useCallback(async()=>{
    const current=++sequence.current;
    try{const result=await load<Receipt>(`/${row.id}?page=${page}`);if(current===sequence.current){setValue(result);setError('');}}
    catch(failure){if(current===sequence.current)setError((failure as Error).message);}
  },[row.id,page]);
  useEffect(()=>{void refresh();return()=>{++sequence.current;};},[refresh,revision]);
  usePolling(refresh,8000,{immediate:false});
  const detail=value?.detail;
  return <div className="submission-detail" aria-label="采集提交详情">
    {error&&<div role="alert"><p className="error">{error}</p><button className="link" onClick={()=>void refresh()}>重新读取详情</button></div>}
    {!value&&!error&&<p className="submission-loading" role="status">正在读取提交详情…</p>}
    {detail?.notice&&<p className="notice">{detail.notice}</p>}{detail?.message&&<p className="submission-message">{detail.message}</p>}
    {!!detail?.items?.length&&<div className="submission-items">{detail.items.map(item=><article className="submission-item" key={item.deletionKey}>
      <div className="submission-item-head"><strong>{item.videoName||'未命名视频'}</strong><span className={`submission-state ${item.status||''}`}>{statusText[item.status||'']||item.status||'状态未记录'}</span></div>
      <dl><div><dt>采集开始</dt><dd>{item.startTime||'未记录'}</dd></div><div><dt>采集结束</dt><dd>{item.endTime||'未记录'}</dd></div>
        {item.camera&&<div><dt>摄像头</dt><dd>{item.camera}</dd></div>}{item.taskId&&<div><dt>平台任务</dt><dd>{item.taskId}</dd></div>}</dl>
      {(item.reason||item.message)&&<p className="submission-item-message">{item.reason||item.message}</p>}
      <RecordDelete name={`${item.videoName} · 提交明细`} version={item.deletionKey} onDelete={requestId=>onRemove(item,requestId)}/>
    </article>)}</div>}
    {value&&!detail?.items?.length&&!detail?.notice&&!error&&<p className="subtle">没有可显示的视频明细，提交凭证仍保留。</p>}
    {(detail?.pages||0)>1&&<nav className="submission-pagination" aria-label="提交详情分页"><span>共 {detail?.total} 项 · {detail?.page} / {detail?.pages} 页</span><div>
      <button className="secondary" disabled={(detail?.page||1)<=1} onClick={()=>setPage((detail?.page||1)-1)}>上一页</button>
      <button className="secondary" disabled={(detail?.page||1)>=(detail?.pages||1)} onClick={()=>setPage((detail?.page||1)+1)}>下一页</button></div></nav>}
  </div>;
}

export default function SubmissionRecords({kind,onChanged}:{kind:'collection'|'slicing';onChanged?:()=>void}){
  const [rows,setRows]=useState<Receipt[]>([]);
  const [error,setError]=useState('');
  const [working,setWorking]=useState(false);
  const [loaded,setLoaded]=useState(false);
  const [revision,setRevision]=useState(0);
  const [removed,setRemoved]=useState<Removal|null>(null);
  const sequence=useRef(0);
  const lock=useRef(false);
  const refresh=useCallback(async()=>{
    const current=++sequence.current;
    try{const result=await load<Receipt[]>(`?kind=${kind}`);if(current===sequence.current){setRows(result);setError('');setLoaded(true);}}
    catch(failure){if(current===sequence.current)setError((failure as Error).message);}
  },[kind]);
  usePolling(refresh,8000);
  useEffect(()=>()=>{++sequence.current;},[]);
  async function action(task:()=>Promise<void>){
    if(lock.current)throw new Error('正在处理，请稍候。');
    lock.current=true;++sequence.current;setWorking(true);setError('');
    try{await task();await refresh();setRevision(before=>before+1);onChanged?.();}
    catch(failure){setError((failure as Error).message);throw failure;}
    finally{lock.current=false;setWorking(false);}
  }
  async function remove(scope:'records'|'items',selections:{id:string;expectedRevision:number}[],name:string,requestId:string){
    await action(async()=>{
      const result=await load<{removalId:string}>(`/${scope}/remove`,{requestId,selections});
      setRemoved({id:result.removalId,name,scope,restoreRequestId:crypto.randomUUID()});
    });
  }
  const dates=groupSubmissionDays(rows,row=>row.submittedAt);
  return <section className="panel submission-records" aria-label="采集提交记录">
    <div className="section-title"><h2>采集提交记录</h2><span>按提交日期展开 · 北京时间</span></div>
    {error&&<p className="error" role="alert">{error}</p>}
    {removed&&<div className="record-deleted-notice" role="status"><span>已删除 {removed.name}，任务和文件保留。</span><button className="link" disabled={working} onClick={()=>void action(async()=>{
      await load(`/${removed.scope}/restore`,{requestId:removed.restoreRequestId,removalId:removed.id});setRemoved(null);
    }).catch(()=>undefined)}>撤销删除</button></div>}
    {!loaded&&!error&&<p className="submission-loading" role="status">正在读取记录…</p>}
    {loaded&&!rows.length&&<p className="subtle">暂无提交记录。</p>}
    <div className="date-list">{dates.map(date=><DateSection key={date.day} day={date.day} summary={`${date.records.length} 次提交 · ${date.records.reduce((sum,row)=>sum+(row.total||0),0)} 条视频`}>
      <div className="date-tools"><p>以下为当天提交的采集明细</p><RecordDelete name={`${date.label} · 全部提交记录`} disabled={working}
        version={date.records.map(row=>row.id).sort().join('|')} onDelete={requestId=>remove('records',date.records.map(row=>({id:row.id,expectedRevision:row.revision})),date.label,requestId)}/></div>
      {date.records.map(row=><section className="dated-batch" key={row.id}>
        <div className="dated-batch-heading"><div><h3>{submissionTime(row.submittedAt)} · {row.title}</h3><p>{statusText[row.status||'']||row.status||'已记录'} · {row.total||0} 条视频</p></div>
          <RecordDelete name={`${submissionTime(row.submittedAt)} · 本次提交`} disabled={working} version={row.id}
            onDelete={requestId=>remove('records',[{id:row.id,expectedRevision:row.revision}],row.title,requestId)}/></div>
        <ReceiptDetails row={row} revision={revision} onRemove={(item,requestId)=>remove('items',[{id:item.deletionKey,expectedRevision:item.revision}],item.videoName||'提交明细',requestId)}/>
      </section>)}
    </DateSection>)}</div>
  </section>;
}
