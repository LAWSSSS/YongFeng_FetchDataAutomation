import {useCallback,useEffect,useRef,useState} from 'react';
import {cancelledRead,createReadCycle,localJson} from './local-api';
import {requestIntents} from './request-intent';
import {readDraft,writeDraft} from './draft-storage';

type Task={recordId:string;plate:string;videoName:string;checkStartTime:string;checkCompleteTime:string;duplicateSource?:string;
  startTime:string;endTime:string;durationMinutes:number;status:string;reason?:string;photoCount:number;
  existingVideoId?:string;existingTaskId?:string;canRecaptureArchived?:boolean};
type Plan={runId:string;planRevision:number;tasks:Task[];total:number;page:number;pages:number;selectableIds:string[];selectAllIds:string[];pendingSubmission?:string;platformUrl?:string};
type Draft={start:string;minutes:string};

function CaptureDraft({task,runId,revision,page,category,onSaved,onBusy}:{task:Task;runId:string;revision:number;page:number;category:string;onSaved:(plan:Plan)=>void;onBusy:(busy:boolean)=>void}){
  const key=`initial-capture:${runId}:${task.recordId}`;
  const [draft,setDraft]=useState(()=>readDraft<Draft>(key,{start:task.startTime.replace(' ','T'),minutes:String(task.durationMinutes)},
    (value):value is Draft=>!!value&&typeof value==='object'&&typeof (value as Draft).start==='string'&&typeof (value as Draft).minutes==='string'));
  const [error,setError]=useState(''),[busy,setBusy]=useState(false);
  const intent=useRef(requestIntents());
  const change=(next:Draft)=>{setDraft(next);writeDraft(key,next);setError('');};
  async function save(){
    const seconds=Math.round(Number(draft.minutes)*60);
    if(!Number.isFinite(seconds)||seconds<60||seconds>7200){setError('时长应为1–120分钟。');return;}
    const start=draft.start.length===16?draft.start+':00':draft.start;
    setBusy(true);onBusy(true);setError('');
    try{const result=await localJson<Plan>(`/api/collection-runs/${runId}/plan?category=${category}`,intent.current.payload('draft',{
      recordId:task.recordId,startTime:start.replace('T',' '),durationSeconds:seconds,expectedPlanRevision:revision,page}),{method:'PATCH'});
      intent.current.complete('draft');writeDraft(key,undefined);onSaved(result);
    }catch(failure){setError((failure as Error).message);}finally{setBusy(false);onBusy(false);}
  }
  return <form className="initial-capture-editor" onSubmit={e=>{e.preventDefault();void save();}}>
    <label>采集开始<input type="datetime-local" step="1" required value={draft.start} onChange={e=>change({...draft,start:e.target.value})}/></label>
    <label>时长（分钟）<input type="number" min="1" max="120" step="any" required value={draft.minutes} onChange={e=>change({...draft,minutes:e.target.value})}/></label>
    <button disabled={busy}>{busy?'正在保存…':'保存此条区间'}</button>{error&&<p className="error" role="alert">{error}</p>}
  </form>;
}

export default function CollectionPlan({runId,revision,queryDates,onChanged}:{runId:string;revision:number;queryDates?:{dateFrom:string;dateTo:string};onChanged:()=>void}){
  const [plan,setPlan]=useState<Plan|null>(null),[page,setPage]=useState(1),[category,setCategory]=useState('all');
  const [selected,setSelected]=useState<Set<string>>(new Set()),[editing,setEditing]=useState('');
  const [draftSaving,setDraftSaving]=useState(false);
  const [busy,setBusy]=useState(false),[error,setError]=useState(''),[notice,setNotice]=useState('');
  const reads=useRef(createReadCycle()),intents=useRef(requestIntents()),locked=useRef(false);
  const accept=useCallback((value:Plan)=>{
    const allowed=new Set(value.selectableIds);
    setPlan(value);setSelected(before=>new Set([...before].filter(id=>allowed.has(id))));
  },[]);
  const refresh=useCallback(()=>reads.current.run(`${runId}:${page}:${category}`,async context=>{
    try{const value=await localJson<Plan>(`/api/collection-runs/${runId}/plan?page=${page}&category=${category}`,undefined,{signal:context.signal});
      if(context.current()){accept(value);setError('');}}
    catch(failure){if(context.current()&&!cancelledRead(failure))setError((failure as Error).message);}
  }),[runId,page,category,accept]);
  useEffect(()=>{void refresh();return()=>reads.current.cancel();},[refresh,revision]);
  const selectable=new Set(plan?.selectableIds);
  async function submit(){
    if(locked.current||!plan||!selected.size)return;locked.current=true;reads.current.cancel();setBusy(true);setError('');setNotice('');
    try{await localJson(`/api/collection-runs/${runId}/submit`,intents.current.payload('submit',{
      recordIds:[...selected].sort(),expectedPlanRevision:plan.planRevision}));intents.current.complete('submit');
      setNotice(`已确认 ${selected.size} 条，后台核对后采集。`);setSelected(new Set());setEditing('');
      onChanged();void refresh();
    }catch(failure){setError((failure as Error).message);}finally{locked.current=false;setBusy(false);}
  }
  function openVideo(videoId:string) {
    window.dispatchEvent(new CustomEvent('simple-open-video',{detail:{videoId}}));
  }
  async function recapture(task:Task) {
    if (!plan || locked.current || !window.confirm(`按当前区间重新采集 ${task.videoName}？旧版本会保留。`)) return;
    locked.current=true;setBusy(true);setError('');reads.current.cancel();
    const key=`recapture:${task.recordId}`;
    try {
      const result=await localJson<{videoId:string}>(`/api/collection-runs/${runId}/recapture`,intents.current.payload(key,{
        recordId:task.recordId,expectedPlanRevision:plan.planRevision,
      }));
      intents.current.complete(key);
      openVideo(result.videoId);
    } catch(failure) {
      setError((failure as Error).message);
    } finally {
      locked.current=false;setBusy(false);
    }
  }
  return <section className="collection-plan panel" aria-label="采集前确认">
    <div className="section-title"><div><h2>核对采集计划</h2><p>{queryDates
      ?`质检日期：${queryDates.dateFrom}${queryDates.dateTo!==queryDates.dateFrom?` 至 ${queryDates.dateTo}`:''}`
      :'查看原始质检与拟采集区间，勾选后确认采集。'}</p></div>
      <label>显示<select value={category} disabled={busy||!!editing} onChange={e=>{setCategory(e.target.value);setPage(1);setPlan(null);setSelected(new Set());}}>
        <option value="all">全部记录</option><option value="ready">可采集</option><option value="other">未批准与已提交</option></select></label></div>
    {plan&&<><div className="plan-selection"><span>已选 {selected.size} 条 · 可选 {plan.selectableIds.length} 条</span><div className="actions">
      <button className="secondary" disabled={busy||!!plan.pendingSubmission||!plan.selectAllIds.length} onClick={()=>setSelected(new Set(plan.selectAllIds))}>全选可采集</button>
      <button className="link" disabled={!selected.size||busy} onClick={()=>setSelected(new Set())}>取消选择</button>
      <button disabled={busy||!selected.size||!!plan.pendingSubmission||!!editing} onClick={()=>void submit()}>{busy?'正在登记…':`确认采集所选${selected.size?`（${selected.size}）`:''}`}</button></div></div>
      {editing&&<p className="notice">请先保存或关闭正在编辑的区间，再确认采集。</p>}
      <div className="initial-plan-grid">{plan.tasks.map(task=><article className="initial-plan-card" key={task.recordId}>
        <div className="video-head"><label className="capture-choice"><input type="checkbox" aria-label={`采集 ${task.videoName}`} checked={selected.has(task.recordId)} disabled={busy||!selectable.has(task.recordId)||!!plan.pendingSubmission}
          onChange={()=>setSelected(before=>{const next=new Set(before);if(next.has(task.recordId))next.delete(task.recordId);else next.add(task.recordId);return next;})}/><strong>{task.videoName}</strong></label>
          <span className="badge">{task.status==='ready'?'待确认':({invalid:'未批准',duplicate:task.duplicateSource==='selection'?'相同区间':'已有采集',confirmed:'已确认',submitted:'已提交',uncertain:'待核对',failed:'未提交成功'} as Record<string,string>)[task.status]||task.status}</span></div>
        <p className="subtle">质检 {task.checkStartTime}<br/>至 {task.checkCompleteTime} · 照片 {task.photoCount} 张</p>
        <p>采集 {task.startTime}<br/>至 {task.endTime} · {task.durationMinutes} 分钟</p>
        {task.reason&&<p className="notice">{task.reason}</p>}
        {task.existingTaskId&&!task.existingVideoId&&<p className="subtle">平台任务 {task.existingTaskId}</p>}
        {task.existingTaskId&&!task.existingVideoId&&plan.platformUrl&&<a href={plan.platformUrl} target="_blank" rel="noreferrer">在采集平台核对</a>}
        {task.existingVideoId&&<button type="button" className="secondary" disabled={busy} onClick={()=>openVideo(task.existingVideoId!)}>打开已有采集</button>}
        {task.canRecaptureArchived&&<button type="button" className="secondary" disabled={busy||!!plan.pendingSubmission||!!editing} onClick={()=>void recapture(task)}>核对旧任务并重采</button>}
        {task.status==='ready'&&<button className="secondary" disabled={busy||draftSaving||!!plan.pendingSubmission} onClick={()=>setEditing(editing===task.recordId?'':task.recordId)}>{editing===task.recordId?'关闭编辑':'修改此条区间'}</button>}
        {editing===task.recordId&&<CaptureDraft task={task} runId={runId} revision={plan.planRevision} page={plan.page} category={category} onBusy={setDraftSaving} onSaved={value=>{reads.current.cancel();accept(value);setEditing('');setNotice('此条采集区间已保存，尚未提交。');onChanged();}}/>}
      </article>)}</div>
      {!plan.tasks.length&&<p className="subtle">没有符合条件的记录。</p>}
      {plan.pages>1&&<nav className="submission-pagination" aria-label="采集计划分页"><span>第 {plan.page}/{plan.pages} 页 · {plan.total} 条</span><div className="actions">
        <button className="secondary" disabled={plan.page<=1||busy||!!editing} onClick={()=>setPage(plan.page-1)}>上一页</button>
        <button className="secondary" disabled={plan.page>=plan.pages||busy||!!editing} onClick={()=>setPage(plan.page+1)}>下一页</button></div></nav>}</>}
    {error&&<p className="error" role="alert">{error}</p>}{notice&&<p className="success" role="status">{notice}</p>}
  </section>;
}
