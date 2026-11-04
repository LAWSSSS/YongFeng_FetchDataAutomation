import {useRecordChanges} from './RecordOperations';
import {useCallback,useEffect,useRef,useState} from 'react';
import {createReadCycle,localJson,cancelledRead} from './local-api';
import {usePolling} from './usePolling';
import {requestIntents} from './request-intent';
import AdmissionSettings from './AdmissionSettings';
import {dateRangeDraft,readDraft,writeDraft} from './draft-storage';
import {readInterval} from './view-preferences';
import CollectionPlan from './CollectionPlan';

const defaults={timeFrom:'00:00:00',timeTo:'23:59:59',plates:''};
const advanced=()=>{try{return {...defaults,...JSON.parse(localStorage.getItem('collection-filters')||'{}')};}catch{return defaults;}};
const today=()=>new Date(Date.now()+8*3600000).toISOString().slice(0,10);
type Run={id:string;revision:number;status:string;message:string;filters:{dateFrom:string;dateTo:string};
  counts:Record<string,number>;createdAt:string;jobId?:string;planRevision?:number};
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
  const [selectedRun,setSelectedRun]=useState('');
  const [busy,setBusy]=useState(false),[error,setError]=useState('');
  const reads=useRef(createReadCycle()),intents=useRef(requestIntents()),locked=useRef(false);
  const seenJobs=useRef(new Set<string>());
  const refresh=useCallback(async()=>{await reads.current.run('current-collection-runs',async context=>{
    try{const value=await localJson<{runs:Run[]}>('/api/collection-runs?currentOnly=true',undefined,{signal:context.signal});
      if(!context.current())return;
      setRuns(value.runs);
      setError('');
      const jobs=value.runs.flatMap(run=>run.jobId?[run.jobId]:[]);
      const newJob=jobs.some(id=>!seenJobs.current.has(id));
      jobs.forEach(id=>seenJobs.current.add(id));
      if(newJob)window.dispatchEvent(new Event('collection-submissions-updated'));
    }
    catch(failure){if(context.current()&&!cancelledRead(failure))setError((failure as Error).message);}
  });},[]);
  useRecordChanges(refresh);
  useEffect(()=>{void refresh();return()=>reads.current.cancel();},[refresh]);
  const activeRuns=runs.filter(run=>!terminal(run.status));
  const current=runs.find(run=>run.id===selectedRun)||runs[0];
  usePolling(refresh,activeRuns.length?readInterval(activeRuns.some(run=>!['waiting_connection','waiting'].includes(run.status)),
    activeRuns.every(run=>run.status==='waiting_connection')):null,{immediate:false});
  async function start(){
    if(locked.current)return;locked.current=true;setBusy(true);setError('');reads.current.cancel();
    try{const value=await localJson<Run>('/api/collection-runs',intents.current.payload('collect',{filters:{...advanced(),dateFrom,dateTo}}));
      intents.current.complete('collect');
      reads.current.cancel();
      setSelectedRun(value.id);
      setRuns(before=>[value,...before.filter(row=>row.id!==value.id)]);}
    catch(failure){setError((failure as Error).message);}
    finally{locked.current=false;setBusy(false);}
  }
  return <section className="panel date-collection" aria-label="日期采集表单">
    <form className="filters" onSubmit={e=>{e.preventDefault();void start();}}>
      <label>开始日期<input type="date" required value={dateFrom} onChange={e=>setDate('dateFrom',e.target.value)}/></label>
      <label>结束日期<input type="date" min={dateFrom} required value={dateTo} onChange={e=>setDate('dateTo',e.target.value)}/></label>
      <button disabled={busy}>{busy?'正在登记…':'查询待采集视频'}</button>
      <button type="button" className="link" onClick={onSettings}>采集设置</button>
    </form>
    {error&&<p className="error" role="alert">{error}</p>}
    {current&&(!terminal(current.status)||current.status==='attention')&&
      <p className="collection-status" role="status">{current.message||'正在查询质检记录…'}</p>}
    {current?.status==='complete'&&!!current.counts.ready&&
      <button className="link collection-result-link" onClick={onVideos}>查看已采集视频</button>}
    {current?.planRevision!==undefined&&(!terminal(current.status)||current.status==='awaiting_review'||current.status==='attention'||current.counts.duplicate>0)&&
      <CollectionPlan key={current.id} runId={current.id} revision={current.revision} queryDates={current.filters} onChanged={()=>{reads.current.cancel();void refresh();}}/>}
  </section>;
}
