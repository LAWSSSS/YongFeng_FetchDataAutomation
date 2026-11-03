import {useRecordChanges} from './RecordOperations';
import {useCallback,useEffect,useRef,useState} from 'react';
import {createReadCycle,localJson,cancelledRead} from './local-api';
import {usePolling} from './usePolling';
import {requestIntents} from './request-intent';
import AdmissionSettings from './AdmissionSettings';
import {dateRangeDraft,readDraft,writeDraft} from './draft-storage';
import {readInterval} from './view-preferences';
import DateSection from './DateSection';
import CollectionPlan from './CollectionPlan';
import RecordDelete from './RecordDelete';
import RecordBatch,{toggleRecord,reconcileRecordChoices} from './RecordBatch';
import type {RecordChoice} from './RecordBatch';
import {groupSubmissionDays,submissionTime} from './submission-dates';

const defaults={timeFrom:'00:00:00',timeTo:'23:59:59',plates:''};
const advanced=()=>{try{return {...defaults,...JSON.parse(localStorage.getItem('collection-filters')||'{}')};}catch{return defaults;}};
const today=()=>new Date(Date.now()+8*3600000).toISOString().slice(0,10);
type Run={id:string;revision:number;status:string;stage:string;message:string;filters:{dateFrom:string;dateTo:string};
  scanned:number;total:number|null;counts:Record<string,number>;createdAt:string};
const terminal=(state:string)=>['complete','attention','cancelled','awaiting_review'].includes(state);
export function CollectionSettings(){
  const [filters,setFilters]=useState(advanced),[message,setMessage]=useState('');
  const noChange=useCallback(()=>{},[]);
  return <section className="panel"><h2>采集设置</h2>
    <AdmissionSettings disabled={false} onBlockedChange={noChange} onApplied={noChange}/>
    <form onSubmit={e=>{e.preventDefault();try{localStorage.setItem('collection-filters',JSON.stringify(filters));setMessage('筛选已保存，下次日期采集生效。');}catch{setMessage('浏览器无法保存设置，请检查存储权限。');}}}>
      <details className="record-advanced"><summary>高级筛选</summary><div className="filters">
        <label>每日开始<input type="time" step="1" required value={filters.timeFrom} onChange={e=>setFilters({...filters,timeFrom:e.target.value})}/></label>
        <label>每日结束<input type="time" step="1" required value={filters.timeTo} onChange={e=>setFilters({...filters,timeTo:e.target.value})}/></label>
        <label>车牌<input value={filters.plates} onChange={e=>setFilters({...filters,plates:e.target.value})} placeholder="留空采集全部车牌"/></label>
        <button>保存筛选</button><button className="secondary" type="button" onClick={()=>setFilters(defaults)}>恢复全天</button>
      </div></details>{message&&<p role="status">{message}</p>}
    </form></section>;
}

export default function DateCollection({onVideos,onSettings}:{onVideos:()=>void;onSettings:()=>void}){
  const [dates,setDates]=useState(()=>readDraft('collection-dates',{dateFrom:today(),dateTo:today()},dateRangeDraft));
  const {dateFrom,dateTo}=dates;
  const setDate=(key:'dateFrom'|'dateTo',value:string)=>{const next={...dates,[key]:value};writeDraft('collection-dates',next);setDates(next);};
  const [runs,setRuns]=useState<Run[]>([]);
  const [reviewId,setReviewId]=useState('');
  const autoOpened=useRef(new Set<string>());
  const [page,setPage]=useState(1),[paging,setPaging]=useState({page:1,pages:1,total:0});
  const [selected,setSelected]=useState<Map<string,RecordChoice>>(new Map());
  const [busy,setBusy]=useState(false),[error,setError]=useState(''),[notice,setNotice]=useState('');
  const reads=useRef(createReadCycle()),intents=useRef(requestIntents()),locked=useRef(false);
  const refresh=useCallback(async()=>{await reads.current.run(`collection-runs:${page}`,async context=>{
    try{const value=await localJson<{runs:Run[];page:number;pages:number;total:number}>(`/api/collection-runs?page=${page}`,undefined,{signal:context.signal});
      if(context.current()){setRuns(Array.isArray(value.runs)?value.runs:[]);setPaging({page:value.page||1,pages:value.pages||1,total:value.total||0});setError('');}}
    catch(failure){if(context.current()&&!cancelledRead(failure))setError((failure as Error).message);}
  });},[page]);
  useRecordChanges(refresh);
  useEffect(()=>{const ready=runs.find(run=>run.status==='awaiting_review'&&!autoOpened.current.has(run.id));
    if(ready){autoOpened.current.add(ready.id);setReviewId(ready.id);}},[runs]);
  useEffect(()=>{void refresh();return()=>reads.current.cancel();},[refresh]);
  useEffect(()=>{setSelected(previous=>reconcileRecordChoices(previous,runs.map(run=>({key:run.id,id:run.id,revision:run.revision,kind:'item'}))));},[runs]);
  const activeRuns=runs.filter(run=>!terminal(run.status));
  usePolling(refresh,readInterval(activeRuns.some(run=>!['waiting_connection','waiting'].includes(run.status)),
    activeRuns.length>0&&activeRuns.every(run=>run.status==='waiting_connection')),{immediate:false});
  async function start(){
    if(locked.current)return;locked.current=true;setBusy(true);setError('');setNotice('');reads.current.cancel();
    try{const value=await localJson<Run>('/api/collection-runs',intents.current.payload('collect',{filters:{...advanced(),dateFrom,dateTo}}));
      intents.current.complete('collect');if(page===1)setRuns(before=>[value,...before.filter(row=>row.id!==value.id)]);else setPage(1);
      setNotice('查询已登记。结果就绪后，请核对并确认采集计划。');}
    catch(failure){setError((failure as Error).message);}
    finally{locked.current=false;setBusy(false);}
  }
  async function hide(choices:RecordChoice[],requestId:string){
    if(locked.current)throw new Error('正在处理，请稍候。');
    locked.current=true;setBusy(true);reads.current.cancel();
    try{
      await localJson<{recordRemoval:{removalId:string}}>('/api/collection-runs/records/remove',{
        requestId,selections:choices.map(row=>({id:row.id,expectedRevision:row.revision}))});
      const ids=new Set(choices.map(row=>row.id));setRuns(before=>before.filter(row=>!ids.has(row.id)));setSelected(new Map());
      void refresh();
    }finally{locked.current=false;setBusy(false);}
  }
  const pick=(run:Run):RecordChoice=>({key:run.id,id:run.id,revision:run.revision,kind:'item'});
  return <section className="panel date-collection" aria-label="日期采集表单">
    <form className="filters" onSubmit={e=>{e.preventDefault();void start();}}>
      <label>开始日期<input type="date" required value={dateFrom} onChange={e=>setDate('dateFrom',e.target.value)}/></label>
      <label>结束日期<input type="date" min={dateFrom} required value={dateTo} onChange={e=>setDate('dateTo',e.target.value)}/></label>
      <button disabled={busy}>{busy?'正在登记…':'查询待采集视频'}</button>
      <button type="button" className="link" onClick={onSettings}>采集设置</button>
    </form>
    {error&&<p className="error" role="alert">{error}</p>}{notice&&<p className="success" role="status">{notice}</p>}
    {!!activeRuns.length&&<div className="collection-active" role="status">{activeRuns.map(run=><p key={run.id}><strong>{run.filters.dateFrom} 至 {run.filters.dateTo}</strong>　{run.message}</p>)}</div>}
    {reviewId&&<CollectionPlan key={reviewId} runId={reviewId} revision={runs.find(run=>run.id===reviewId)?.revision||0} onChanged={()=>void refresh()}/>}
    {(runs.length>0||paging.total>0)&&<details className="collection-history"><summary>日期任务记录 · {paging.total||runs.length} 条</summary>
    <RecordBatch selected={selected} disabled={busy} total={runs.length} scope="本页日期任务"
      onSelectAll={()=>setSelected(new Map(runs.map(run=>[run.id,pick(run)])))} onClear={()=>setSelected(new Map())} onDelete={hide}/>
    <div className="date-list collection-runs">{groupSubmissionDays(runs,run=>run.createdAt).map(day=><DateSection key={day.day} day={day.day}
      rememberKey={`collection-runs:${day.day}`} summary={`${day.records.length} 个日期任务`}>
      <div className="date-tools"><span>按提交日期管理</span><button className="secondary" disabled={busy} onClick={()=>setSelected(before=>{const next=new Map(before);day.records.forEach(run=>next.set(run.id,pick(run)));return next;})}>选择当天任务</button></div>
      {day.records.map(run=><article key={run.id} className="collection-run-record">
      <div className="dated-batch-heading"><div><label className="record-checkbox"><input type="checkbox" disabled={busy} aria-label={`选择日期任务 ${run.filters.dateFrom} 至 ${run.filters.dateTo}`}
        checked={selected.has(run.id)} onChange={()=>setSelected(before=>toggleRecord(before,pick(run)))}/><strong>{run.filters.dateFrom} 至 {run.filters.dateTo}</strong></label><p>{submissionTime(run.createdAt)}</p></div>
      <RecordDelete name={`${run.filters.dateFrom} 至 ${run.filters.dateTo} · 日期任务`} disabled={busy} version={run.revision} onDelete={id=>hide([pick(run)],id)}/></div>
      <p>{run.message}</p>
      <p>查询 {run.scanned}{run.total!==null?` / ${run.total}`:''} 条 · 已提交 {run.counts.submitted||0} 条 · 可查看 {run.counts.ready||0} 条 · 重复 {run.counts.duplicate||0} 条</p>
      {run.status==='awaiting_review'&&<button className="secondary" onClick={()=>setReviewId(run.id)}>核对采集计划</button>}
      {!!run.counts.ready&&<button className="secondary" onClick={onVideos}>查看并确认视频</button>}
      </article>)}
    </DateSection>)}</div>
    {paging.pages>1&&<nav className="submission-pagination" aria-label="日期任务分页"><span>第 {paging.page}/{paging.pages} 页</span><div>
      <button className="secondary" disabled={busy||paging.page<=1} onClick={()=>{setSelected(new Map());setPage(paging.page-1);}}>上一页</button>
      <button className="secondary" disabled={busy||paging.page>=paging.pages} onClick={()=>{setSelected(new Map());setPage(paging.page+1);}}>下一页</button>
    </div></nav>}</details>}
  </section>;
}
