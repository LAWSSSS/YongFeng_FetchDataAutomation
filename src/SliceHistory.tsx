import {useRecordChanges} from './RecordOperations';
import {processingFinished,processingLabels} from './processing-status';
import {readInterval} from './view-preferences';
import {useCallback,useEffect,useRef,useState} from 'react';
import DateSection,{useDateSectionOpen} from './DateSection';
import RecordBatch from './RecordBatch';
import type {RecordChoice} from './RecordBatch';
import RecordDelete from './RecordDelete';
import {cancelledRead,createReadCycle,localJson} from './local-api';
import {usePolling} from './usePolling';
import {physicalCategories,physicalFiles,physicalState,sliceTitle} from './slicing-view';
import {submissionTime} from './submission-dates';
import {viewMemory} from './view-memory';

type Day={day:string;total:number;itemCount:number;receiptCount:number;revision:string};
export type SliceHistorySummary={recordDays?:Day[];enabled?:boolean;busy?:boolean};
type Category={state:string;packageId?:string;taskId?:string;actualImages?:number;estimatedImages?:number;
  intervalSeconds?:number|null;intervals?:{start:number;end:number}[];message?:string;errorCode?:string};
type Item={id:string;name:string;mediaId?:string;mediaGeneration?:number;collectionRevision?:number;collectionVideoId?:string;
  processing?:{state:string;stageLabel:string;message:string;terminal:boolean;progress?:{done:number;total:number;unit:string}|null};
  terminal?:boolean;displayProgress?:{done:number;total:number;unit:string}|null;revision:number;sourceDatasetId:string;sourceVideoId:string;path?:string;state:string;message?:string;
  superseded?:boolean;coveredByRecovery?:string;categories:Record<string,Category>};
type Receipt={id:string;revision:number;title:string;submittedAt:string;message?:string;notice?:string};
type Package={id:string;itemId?:string;state:string;imageCount?:number;localCleanupState?:string;cleanupState?:string};
type Detail={items:Item[];submissionReceipts:Receipt[];packages?:{items:Package[]};
  recordPagination:{day:string;page:number;pages:number;pageSize:number;total:number;scopeRevision:string}};
type Query={status:string;source:string;search:string};
const api=<T,>(path='',body?:unknown,signal?:AbortSignal)=>localJson<T>(`/api/slicing${path}`,body,{signal});
const labels:Record<string,string>={...processingLabels,pending:'待处理',analyzing:'识别中',platform_processing:'照片处理中',
  attention:'需处理',review:'需确认',waiting_connection:'等待恢复',superseded:'旧版已停止',ready:'准备导出',queued:'排队上传',
  uploading:'上传中',uploaded:'等待导入',importing:'导入中',submitting:'提交核对中',waiting:'等待平台'};
const queryString=(query:Query)=>new URLSearchParams(query).toString();
const choice=(item:Item):RecordChoice=>({key:`item:${item.id}`,id:item.id,revision:item.revision,kind:'item'});
const receiptChoice=(item:Receipt):RecordChoice=>({key:`receipt:${item.id}`,id:item.id,revision:item.revision,kind:'receipt'});
const seconds=(value:number)=>{const rounded=Math.max(0,Math.round(value));return `${Math.floor(rounded/60)}:${String(rounded%60).padStart(2,'0')}`;};

function HistoryDay({day,query,epoch,selected,busy,onSelect,onSelectMany,onRemove,onRemoveDay}: {
  day:Day;query:Query;epoch:number;selected:Map<string,RecordChoice>;busy:boolean;
  onSelect:(row:RecordChoice)=>void;onSelectMany:(rows:RecordChoice[],checked:boolean)=>void;
  onRemove:(rows:RecordChoice[],requestId:string,_name:string)=>Promise<void>;
  onRemoveDay:(day:Day,query:Query,requestId:string)=>Promise<void>;
}){
  const expanded=useDateSectionOpen(),reads=useRef(createReadCycle());
  const scope=`slice-page:${day.day}:${queryString(query)}`;
  const [paging,setPaging]=useState(()=>({scope,page:viewMemory.read(scope,1)}));
  const page=paging.scope===scope?paging.page:viewMemory.read(scope,1);
  const setPage=(page:number)=>{viewMemory.save(scope,page);setPaging({scope,page});};
  const [value,setValue]=useState<Detail|null>(null),[error,setError]=useState('');
  const [loadedKey,setLoadedKey]=useState(''),[loadedParams,setLoadedParams]=useState('');
  const params=new URLSearchParams({...query,recordsDay:day.day,page:String(page),pageSize:'20'}).toString();
  const key=`${params}:${day.revision}:${epoch}`;
  const refresh=useCallback(async()=>{
    await reads.current.run(key,async context=>{
      try{const result=await api<Detail>(`?${params}`,undefined,context.signal);
        if(context.current()){setValue(result);setError('');setLoadedKey(key);setLoadedParams(params);}}
      catch(failure){if(context.current()&&!cancelledRead(failure))setError((failure as Error).message);}
    });
  },[params,key]);
  useEffect(()=>{if(expanded)void refresh();return()=>reads.current.cancel();},[expanded,refresh]);
  const unsettled=value?.items.filter(item=>['pending','analyzing','platform_processing','waiting_connection','submitting','waiting'].includes(item.state))||[];
  const onlyOffline=unsettled.length>0&&unsettled.every(item=>item.state==='waiting_connection');
  usePolling(refresh,expanded&&unsettled.length?readInterval(!onlyOffline,onlyOffline):null,{immediate:false});
  // A scope revision refresh must preserve the mounted cards, open details and drafts.
  // A different filter/page still hides the previous query, and stale revisions cannot mutate.
  const current=key===loadedKey,visible=loadedParams===params;
  const items=visible?value?.items||[]:[];
  const groups=physicalFiles(items),receipts=visible?value?.submissionReceipts||[]:[];
  const pageChoices=items.map(choice);
  const filtering=query.status!=='all'||query.source!=='all'||!!query.search;
  return <div className="slice-history-day">
    <div className="actions"><button className="secondary" disabled={busy||!current||!items.length}
      onClick={()=>onSelectMany(pageChoices,!pageChoices.every(row=>selected.has(row.key)))}>
      {pageChoices.length&&pageChoices.every(row=>selected.has(row.key))?'取消选择本页':'选择本页视频'}</button>
      <RecordDelete name={`${day.day||'日期未记录'}${filtering?' · 筛选结果':' · 全部记录'}`} version={day.revision} disabled={busy}
        description={`删除该日期${filtering?'符合当前筛选的':'的全部'} ${day.total} 个视频与 ${day.receiptCount} 次提交记录，包含其他分页；已结束项目清理已核验本地文件；进行中项目仅移除记录。云端成果保留。`}
        onDelete={id=>onRemoveDay(day,query,id)}/></div>
    {error&&<p className="error" role="alert">{error}<button className="link" onClick={()=>void refresh()}>重新读取</button></p>}
    {!current&&!error&&<p className="subtle" role="status">{visible?'正在更新记录，已展开内容保留…':'正在读取该日期的记录…'}</p>}
    <div className="batch-record-list">{groups.map(group=>{
      const effective=group.records.filter(item=>!item.superseded&&!item.coveredByRecovery);
      const first=effective[0]||group.records[0],heading=sliceTitle(first.name);
      const state=effective.length?physicalState(effective):group.records.some(item=>item.coveredByRecovery)?'complete':'superseded';
      const choices=group.records.map(choice);
      return <article className="video-card slice-video-card" key={group.key}>
        <div className="video-head slice-video-head"><div><label className="record-checkbox"><input type="checkbox" disabled={busy||!current}
          aria-label={`选择切片视频 ${first.name}`} checked={choices.every(row=>selected.has(row.key))}
          onChange={event=>onSelectMany(choices,event.target.checked)}/>选择</label><h3 title={first.name}>{heading.title}</h3>
          {heading.when&&<p className="slice-record-time">{heading.when}</p>}</div><span className={`slice-state ${first.processing?.state||state}`}>{first.processing?.stageLabel||labels[state]||state}</span></div>
        {first.processing?.message&&<p className="slice-detail">{first.processing.message}</p>}
        {!first.processing?.terminal&&(first.processing?.progress||(!first.terminal&&first.displayProgress))&&(()=>{
          const progress=first.processing?.progress||first.displayProgress!;
          return <div className="pipeline-progress"><progress aria-label={`${heading.title}处理进度`} max={progress.total} value={progress.done}/>
            <small>{progress.unit==='bytes'?`${(progress.done/1024**2).toFixed(1)} / ${(progress.total/1024**2).toFixed(1)} MB`:`${progress.done}/${progress.total} ${progress.unit}`}</small></div>;
        })()}
        <details className="video-card-details"><summary>查看结果与详情</summary>
        <div className="slice-categories">{physicalCategories(effective.length?effective:group.records,query.source).map(({item,categoryName})=>{
          const category=item.categories[categoryName];
          const pack=value?.packages?.items.find(row=>row.id===category.packageId);
          const cleaned=pack?.cleanupState==='complete'||pack?.localCleanupState==='complete';
          return <div key={categoryName} className="slice-category"><div className="slice-category-heading"><strong>{categoryName==='bed'?'车斗':'磁盘吊'}</strong>
            <span className={`slice-state ${category.state}`}>{labels[category.state]||category.state}</span></div>
            {category.actualImages!=null&&<p className="slice-detail">已核验 {category.actualImages} 张</p>}
            {category.message&&category.state!=='complete'&&<p className="slice-detail">{category.message}</p>}
            {!!category.intervals?.length&&<details className="slice-observations"><summary>有效区间 · {category.intervals.length} 段</summary>
              <p className="slice-detail">{category.intervals.map(row=>`${seconds(row.start)}–${seconds(row.end)}`).join('；')}{category.intervalSeconds?` · 每 ${category.intervalSeconds} 秒一张`:''}</p></details>}
            {pack&&<p className="slice-detail">{cleaned?'本地照片已清理':<a href={`/package-view/${encodeURIComponent(pack.id)}`} target="_blank" rel="noopener noreferrer">查看结果图片</a>}</p>}
            {!pack&&category.taskId&&<p className="slice-detail">平台任务 {category.taskId} · 无本地照片包</p>}
          </div>;
        })}</div>
        {group.records.some(item=>item.coveredByRecovery)&&<p className="slice-detail">旧尝试已由验证完成的恢复结果覆盖，历史凭证保留。</p>}
        {effective.some(item=>!processingFinished(item.state)&&!['excluded','superseded'].includes(item.state))&&<button className="secondary"
          onClick={()=>window.dispatchEvent(new CustomEvent('simple-open-video',{detail:{mediaId:first.mediaId,videoId:first.collectionVideoId}}))}>查看处理详情</button>}
        <details className="slice-file"><summary>文件位置</summary><p className="slice-detail">{first.name}<br/>{first.path||'尚未绑定'}</p></details>
        <RecordDelete name={`${heading.title}${heading.when?` · ${heading.when}`:''}`} version={choices.map(row=>`${row.id}:${row.revision}`).sort().join('|')}
          disabled={busy||!current} onDelete={id=>onRemove(choices,id,heading.title)}/>
        </details>
      </article>;
    })}</div>
    {value&&current&&value.recordPagination.pages>1&&<nav className="submission-pagination" aria-label="切片记录分页">
      <span>共 {value.recordPagination.total} 个视频 · 第 {value.recordPagination.page}/{value.recordPagination.pages} 页</span><div>
        <button className="secondary" disabled={busy||value.recordPagination.page<=1} onClick={()=>setPage(value.recordPagination.page-1)}>上一页</button>
        <button className="secondary" disabled={busy||value.recordPagination.page>=value.recordPagination.pages} onClick={()=>setPage(value.recordPagination.page+1)}>下一页</button></div></nav>}
    {!!receipts.length&&<details className="slice-receipts"><summary>提交凭证 · {receipts.length}</summary>{receipts.map(row=><article className="slice-receipt" key={row.id}>
      <div className="slice-receipt-heading"><label className="record-checkbox"><input type="checkbox" disabled={busy||!current}
        checked={selected.has(`receipt:${row.id}`)} onChange={()=>onSelect(receiptChoice(row))}/>选择</label>
        <strong>{row.title}</strong><time>{submissionTime(row.submittedAt)}</time></div>
      <p className="subtle">{row.message||row.notice||'提交凭证保留。'}</p>
      <RecordDelete name={`${row.title} · ${submissionTime(row.submittedAt)}`} version={row.revision} disabled={busy||!current}
        onDelete={id=>onRemove([receiptChoice(row)],id,row.title)}/></article>)}</details>}
  </div>;
}

export default function SliceHistory({snapshot,onChanged}:{snapshot?:SliceHistorySummary;onChanged?:()=>void}){
  const [status,setStatus]=useState(()=>viewMemory.read('slice-history:status','all'));
  const [source,setSource]=useState(()=>viewMemory.read('slice-history:source','all'));
  const [search,setSearch]=useState(()=>viewMemory.read('slice-history:search',''));
  const [appliedSearch,setAppliedSearch]=useState(()=>viewMemory.read('slice-history:search',''));
  const [value,setValue]=useState<SliceHistorySummary|null>(snapshot||null),[loadedKey,setLoadedKey]=useState('');
  const [selected,setSelected]=useState<Map<string,RecordChoice>>(new Map()),[busy,setBusy]=useState(false),[error,setError]=useState(''),[epoch,setEpoch]=useState(0);
  const selectionScopes=useRef(new Map<string,{day:string;revision:string}>());
  const reads=useRef(createReadCycle()),locked=useRef(false),payloads=useRef(new Map<string,{selections:{id:string;expectedRevision:number}[];receipts:{id:string;expectedRevision:number}[]}>());
  const query={status,source,search:appliedSearch},key=queryString(query);
  const currentScope=useRef({key,days:new Map<string,string>()});
  currentScope.current={key,days:new Map((value?.recordDays||[]).map(day=>[day.day,day.revision]))};
  const supplied=!!snapshot?.recordDays&&status==='all'&&source==='all'&&!appliedSearch;
  const refresh=useCallback(async()=>{
    if(locked.current)return;
    await reads.current.run(key,async context=>{
      try{const result=await api<SliceHistorySummary>(`?summary=true&${key}`,undefined,context.signal);
        if(context.current()){setValue(result);setLoadedKey(key);setError('');}}
      catch(failure){if(context.current()&&!cancelledRead(failure))setError((failure as Error).message);}
    });
  },[key]);
  useRecordChanges(refresh);
  useEffect(()=>{if(supplied){setValue(snapshot!);setLoadedKey(key);}},[snapshot,supplied,key]);
  useEffect(()=>{if(!supplied)void refresh();return()=>reads.current.cancel();},[supplied,key,refresh]);
  useEffect(()=>{const timer=setTimeout(()=>setAppliedSearch(search.trim()),250);return()=>clearTimeout(timer);},[search]);
  useEffect(()=>{viewMemory.save('slice-history:status',status);viewMemory.save('slice-history:source',source);viewMemory.save('slice-history:search',search);},[status,source,search]);
  useEffect(()=>{selectionScopes.current.clear();setSelected(new Map());},[key]);
  useEffect(()=>{
    if(key!==loadedKey)return;
    const revisions=new Map((value?.recordDays||[]).map(day=>[day.day,day.revision]));
    setSelected(previous=>{
      const next=new Map(previous);
      for(const [id] of previous){const scope=selectionScopes.current.get(id);
        if(!scope||revisions.get(scope.day)!==scope.revision){next.delete(id);selectionScopes.current.delete(id);}}
      return next.size===previous.size?previous:next;
    });
  },[value?.recordDays,key,loadedKey]);
  usePolling(refresh,!supplied?(value?.enabled||value?.busy?4000:15000):null,{immediate:false});
  async function remove(rows:RecordChoice[],requestId:string,_name:string){
    if(locked.current)return;
    locked.current=true;reads.current.cancel();setBusy(true);setError('');
    if(!payloads.current.has(requestId))payloads.current.set(requestId,{
      selections:rows.filter(row=>row.kind==='item').map(row=>({id:row.id,expectedRevision:row.revision})),
      receipts:rows.filter(row=>row.kind==='receipt').map(row=>({id:row.id,expectedRevision:row.revision})),
    });
    try{await api<{recordRemoval:{removalId:string}}>('/records/remove-group',{requestId,...payloads.current.get(requestId)});
      payloads.current.delete(requestId);
      setSelected(new Map());setEpoch(value=>value+1);onChanged?.();}
    finally{locked.current=false;setBusy(false);void refresh();}
  }
  async function removeDay(day:Day,query:Query,requestId:string){
    if(payloads.current.has(requestId))return remove([],requestId,day.day||'日期未记录');
    if(locked.current)return;
    if(day.itemCount+day.receiptCount>5000)throw new Error('该日期记录较多，请分批选择删除；尚未删除任何记录。');
    const rows:RecordChoice[]=[],receipts=new Map<string,Receipt>();
    locked.current=true;reads.current.cancel();setBusy(true);setError('');
    try{let pages=1;
      for(let page=1;page<=pages;page++){
        const detail=await api<Detail>(`?${new URLSearchParams({...query,recordsDay:day.day,page:String(page),pageSize:'20'})}`);
        if(detail.recordPagination.scopeRevision!==day.revision)throw new Error('该日期已有新进度，请刷新后重新确认；记录尚未删除。');
        rows.push(...detail.items.map(choice));detail.submissionReceipts.forEach(row=>receipts.set(row.id,row));pages=detail.recordPagination.pages;
      }
    }finally{locked.current=false;setBusy(false);}
    await remove([...rows,...[...receipts.values()].map(receiptChoice)],requestId,day.day||'日期未记录');
  }
  function selectRows(rows:RecordChoice[],checked:boolean,day:Day){
    rows.forEach(row=>checked?selectionScopes.current.set(row.key,{day:day.day,revision:day.revision}):selectionScopes.current.delete(row.key));
    setSelected(previous=>{const next=new Map(previous);rows.forEach(row=>checked?next.set(row.key,row):next.delete(row.key));return next;});
  }
  async function selectAll(){
    if(locked.current)return;
    if(days.reduce((sum,day)=>sum+day.itemCount+day.receiptCount,0)>5000){setError('记录较多，请按日期分批选择。');return;}
    locked.current=true;reads.current.cancel();setBusy(true);setError('');
    const choices=new Map<string,RecordChoice>(),scopes=new Map<string,{day:string;revision:string}>();
    try{
      for(const day of days){let pages=1;
        for(let page=1;page<=pages;page++){
          const detail=await api<Detail>(`?${new URLSearchParams({...query,recordsDay:day.day,page:String(page),pageSize:'20'})}`);
          if(detail.recordPagination.scopeRevision!==day.revision)throw new Error('筛选结果已有新进度，请刷新后重新选择。');
          for(const row of [...detail.items.map(choice),...detail.submissionReceipts.map(receiptChoice)]){
            choices.set(row.key,row);scopes.set(row.key,{day:day.day,revision:day.revision});
          }
          pages=detail.recordPagination.pages;
        }
      }
      if(currentScope.current.key!==key||days.some(day=>currentScope.current.days.get(day.day)!==day.revision))throw new Error('筛选结果已有变化，请重新选择。');
      selectionScopes.current=scopes;setSelected(choices);
    }catch(failure){setError((failure as Error).message);}
    finally{locked.current=false;setBusy(false);}
  }
  const current=key===loadedKey,days=current?value?.recordDays||[]:[];
  const disabled=busy||search.trim()!==appliedSearch||!current;
  return <section className="panel slice-history" data-selection-scope="slicing" aria-labelledby="slice-history-title">
    <div className="section-title"><h2 id="slice-history-title">视频记录</h2></div>
    <div className="slice-filter-bar"><label>状态<select value={status} disabled={busy} onChange={event=>setStatus(event.target.value)}>
      {[['all','全部'],['unsliced','未完成'],['attention','需处理'],['pending','待识别'],['analyzing','识别中'],['waiting_connection','等待恢复'],['platform_processing','照片处理中'],['complete','完整处理完成'],['partial_complete','可用部分完成'],['no_target','未检出目标'],['uncuttable','无可用画面']].map(([id,label])=><option value={id} key={id}>{label}</option>)}</select></label>
      <label>类别<select value={source} disabled={busy} onChange={event=>setSource(event.target.value)}><option value="all">全部类别</option><option value="27912">车斗</option><option value="27944">磁盘吊</option></select></label>
      <label>查找视频<input type="search" value={search} disabled={busy} onChange={event=>setSearch(event.target.value)} placeholder="输入车牌、名称或编号"/></label></div>
    {error&&<p className="error" role="alert">{error}<button className="link" onClick={()=>void refresh()}>重试读取</button></p>}
    <RecordBatch selected={selected} disabled={disabled} total={days.reduce((sum,day)=>sum+day.itemCount+day.receiptCount,0)} scope="筛选结果"
      onSelectAll={()=>void selectAll()} onClear={()=>{selectionScopes.current.clear();setSelected(new Map());}}
      onDelete={(rows,id)=>remove(rows,id,`所选 ${rows.length} 条记录`)}/>
    {!current?<p className="subtle" role="status">正在读取日期记录…</p>:!days.length?<p className="subtle">当前没有符合条件的切片记录。</p>:
      days.map(day=><DateSection key={day.day} day={day.day} rememberKey={`slice-history:${day.day}`} summary={`${day.total} 个视频 · ${day.receiptCount} 次提交`}>
        <HistoryDay day={day} query={query} epoch={epoch} selected={selected} busy={disabled}
          onSelect={row=>selectRows([row],!selected.has(row.key),day)}
          onSelectMany={(rows,checked)=>selectRows(rows,checked,day)}
          onRemove={remove} onRemoveDay={removeDay}/>
      </DateSection>)}
  </section>;
}
