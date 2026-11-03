import { useRef, useState } from 'react';
import './video-processing.css';
import {localJson} from './local-api';
import {editProcessingDraft,currentProcessingDraft,validProcessingDrafts} from './processing-drafts';
import type {ProcessingDraft} from './processing-drafts';
import {usePolling} from './usePolling';
import {pipelineBusy,pipelineSelection} from './pipeline-view';
import type {NextAction,PipelineAction,PipelineVideo} from './pipeline-view';
import {viewMemory} from './view-memory';
import {useInputDraft} from './useInputDraft';
import {processingFinished,processingLabels} from './processing-status';
import {useDateSectionOpen} from './DateSection';

type Category={state:string;message?:string;errorCode?:string;intervals?:{start:number;end:number}[];packageId?:string;actualImages?:number};
type Item={id:string;name:string;path:string;revision:number;state:string;errorCode?:string;message?:string;blockedReason?:string;coveredByRecovery?:string;sourceDatasetId:string;categories:Record<string,Category>};
type Pack={id:string;itemId:string;revision:number;category:string;state:string;imageCount?:number;cleanupState?:string;localCleanupState?:string};
export type ProcessingGroup={id:string;videoId?:string;name:string;generation:number;owner:string;blockedReason?:string;createdAt?:string;nextAction?:NextAction;items:Item[];packages:Pack[];files?:{id:string;actionItemId?:string|null;archiveState:string;archiveMessage?:string;cleanupEligible?:boolean;cleanupReason?:string;cleanup?:{state:string;message?:string}}[]};
type Candidate={id:string;name:string;qualityStart:string;revision:number;activeRevision:number};
const labels:Record<string,string>={...processingLabels,pending:'待识别',analyzing:'识别中',platform_processing:'照片处理中',attention:'需处理',review:'待确认',queued:'排队上传',uploaded:'等待导入',uploading:'上传中',importing:'导入中',waiting:'等待平台',ready:'准备导出',superseded:'旧版已停止'};
const text=(category:Category)=>category.intervals?.map(row=>`${row.start}-${row.end}`).join(', ')||'';
function intervals(value:string){return value.split(/[,，]/).map(x=>x.trim()).filter(Boolean).map(x=>{
  const m=x.match(/^(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)$/);
  if(!m||Number(m[2])<Number(m[1]))throw new Error('区间请按“开始-结束秒”填写，多个区间用逗号分开。');
  return {start:Number(m[1]),end:Number(m[2])};
});}

function ProcessingItem({group,item,onRefresh,candidates}:{group:ProcessingGroup;item:Item;onRefresh:()=>Promise<void>;candidates:Candidate[]}) {
  const [busy,setBusy]=useState(false),[error,setError]=useState('');
  const draftKey=`processing:${group.id}:${item.id}`;
  const [notice,setNotice]=useState('');
  const [drafts,setDrafts]=useInputDraft<Record<string,ProcessingDraft>>(`${draftKey}:intervals`,{},validProcessingDrafts);
  const [link,setLink]=useState('');
  const locked=useRef(false),intent=useRef<{signature:string;requestId:string}|null>(null);
  async function act(action:string,extra:object={},revision=item.revision){
    if(locked.current)return;
    const body={action,generation:group.generation,itemId:item.id,expectedRevision:revision,...extra};
    const signature=JSON.stringify(body);
    if(intent.current?.signature!==signature)intent.current={signature,requestId:crypto.randomUUID()};
    locked.current=true;setBusy(true);setError('');setNotice('');
    try{
      await localJson(`/api/video-processing/${encodeURIComponent(group.id)}/actions`,{...body,requestId:intent.current.requestId});
      if(action==='review')setDrafts(before=>{const next={...before};delete next[(extra as {category:string}).category];return next;});
      intent.current=null;setNotice(action==='retry-archive'?'归档已排队，文件使用结束后自动处理。':'操作已登记，后台继续处理。');void onRefresh().catch(()=>setNotice('操作已登记，等待状态更新。'));
    }catch(failure){setError((failure as Error).message);}finally{locked.current=false;setBusy(false);}
  }
  const blocked=!!item.blockedReason;
  const primaryItemId=group.items.find(row=>!row.coveredByRecovery)?.id;
  // A physical video's old and recovered files can belong to different items.
  // File commands must use the backend's exact eligible binding, never the first card.
  const files=group.files?.filter(file=>file.actionItemId===item.id||(!file.actionItemId&&item.id===primaryItemId));
  return <div className="video-processing-item"><div className="processing-status"><strong>{item.sourceDatasetId==='27912'?'车斗来源':'磁盘吊来源'}</strong><span>{labels[item.state]||item.state}</span></div>
    {item.blockedReason&&<p className="notice">{item.blockedReason}</p>}
    {item.message&&item.state!=='complete'&&<p className="subtle">{item.message}</p>}
    <details className="processing-more"><summary>更多操作</summary><div className="actions">
      {item.state==='attention'&&<button className="secondary" disabled={busy||blocked} onClick={()=>void act('retry')}>重试切片</button>}
      {item.state==='attention'&&item.errorCode==='media_incomplete'&&<button disabled={busy||blocked} onClick={()=>void act('recover')}>处理可解码片段</button>}
      {item.state==='pending'&&<button className="secondary" disabled={busy||blocked} onClick={()=>void act('start')}>开始本条</button>}
    </div></details>
    {!group.videoId&&item.id===primaryItemId&&<details><summary>关联已有采集记录</summary><label className="processing-link">采集视频<select value={link} onChange={event=>setLink(event.target.value)}><option value="">选择需要关联的当前版本</option>{candidates.map(v=><option key={v.id} value={v.id}>{v.name} · {v.qualityStart}</option>)}</select></label>
      <button className="secondary" disabled={busy||!link} onClick={()=>{const target=candidates.find(v=>v.id===link);if(target)void act('link',{videoId:target.id,collectionRevision:target.activeRevision,collectionExpectedRevision:target.revision});}}>核对原片并关联</button></details>}
    {files?.map(file=><div className="processing-archive" key={file.id}>
      <p className="subtle">{file.cleanup?.state==='complete'?'本地文件已清理，成果和记录保留。':file.archiveState==='preserved'?'按保留规则保持原位。':file.archiveState==='complete'?'已归档。':file.archiveMessage||'等待分类归档。'}</p>
      {['waiting','attention'].includes(file.archiveState)&&file.cleanup?.state!=='complete'&&<button className="secondary" disabled={busy||blocked||file.actionItemId!==item.id} onClick={()=>void act('retry-archive',{fileId:file.id})}>{busy?'正在登记…':'重试归档'}</button>}
      {!file.actionItemId&&['waiting','attention'].includes(file.archiveState)&&<p className="subtle">文件处理绑定尚未就绪，原片保留。</p>}
    </div>)}
    {files?.filter(f=>f.archiveState==='complete'&&f.cleanup?.state!=='complete').map(file=><div key={file.id}><button className="secondary" disabled={busy||blocked||file.actionItemId!==item.id||file.cleanupEligible===false} onClick={()=>{
      if(window.confirm('删除本视频已核验的本地视频、照片和压缩包？平台成果与历史记录保留。'))void act('cleanup',{fileId:file.id});
    }}>清理已核验的本地媒体</button>{file.cleanupEligible===false&&<p className="subtle">{file.cleanupReason}</p>}</div>)}
    {!group.videoId&&!['complete','superseded'].includes(item.state)&&<details><summary>本地原片</summary><div className="processing-editor">
      {item.path&&<video controls preload="none" src={`/api/slicing/${item.id}/stream`}/>}
    </div></details>}
    {Object.entries(item.categories).filter(([,category])=>category.state!=='not_applicable').map(([key,category])=>{
      const pack=group.packages.find(p=>p.id===category.packageId);
      const draft=drafts[key]||editProcessingDraft(undefined,text(category),item.revision,group.generation);
      const current=currentProcessingDraft(draft,item.revision,group.generation);
      return <div className="processing-category" key={key}><strong>{key==='bed'?'车斗':'磁盘吊'} · {labels[category.state]||category.state}{category.actualImages!=null?` · ${category.actualImages} 张`:''}</strong>
        {pack&&<div className="actions">{pack.cleanupState==='complete'||pack.localCleanupState==='complete'?<span className="subtle">本地照片已清理</span>:<a href={`/package-view/${pack.id}`} target="_blank" rel="noopener noreferrer">查看结果图片</a>}
          {pack.state==='review'&&<button disabled={busy||blocked} onClick={()=>void act('confirm-package',{packageId:pack.id},pack.revision)}>确认并上传照片包</button>}
          {pack.state==='attention'&&<button className="secondary" disabled={busy||blocked} onClick={()=>void act('retry-package',{packageId:pack.id},pack.revision)}>核对照片包</button>}</div>}
        {!pack&&['attention','ready','no_target','pending'].includes(category.state)&&<details>
          <summary>调整区间</summary><div className="processing-editor">
            <label>有效区间（秒）<input value={draft.value} onChange={event=>setDrafts(before=>({...before,[key]:editProcessingDraft(before[key],event.target.value,item.revision,group.generation)}))} placeholder="例如 12-385, 520-880"/></label>
            {!current&&<p className="notice">记录已变化，草稿保留。<button className="link" onClick={()=>setDrafts(before=>({...before,[key]:{...draft,revision:item.revision,generation:group.generation}}))}>已核对当前记录，沿用草稿</button></p>}
            <div className="actions"><button disabled={busy||blocked||!current} onClick={()=>{try{void act('review',{category:key,intervals:intervals(draft.value)},draft.revision);}catch(failure){setError((failure as Error).message);}}}>保存区间</button>
              <button className="secondary" disabled={busy||blocked||!current} onClick={()=>void act('review',{category:key,confirmedNoTarget:true},draft.revision)}>确认未检出</button></div>
          </div></details>}
      </div>;
    })}{notice&&<p className="subtle" role="status">{notice}</p>}{error&&<p className="error" role="alert">{error}</p>}
  </div>;
}

function ProcessingPrimary({group,onRefresh}:{group:ProcessingGroup;onRefresh:()=>Promise<void>}){
  const expanded=useDateSectionOpen();
  const [busy,setBusy]=useState(false),[operation,setOperation]=useState(''),[error,setError]=useState(''),[message,setMessage]=useState('');
  const locked=useRef(false),intent=useRef<{signature:string;requestId:string}|null>(null);
  const next=group.nextAction;
  async function enqueue(action:PipelineAction){
    if(!next||locked.current||operation)return;
    const row:PipelineVideo={id:group.id,mediaId:group.id,generation:group.generation,videoId:group.videoId,
      name:group.name,state:next.kind,stage:next.stage,message:next.reason,nextAction:next};
    const selections=[pipelineSelection(row,action)],signature=JSON.stringify(selections);
    if(intent.current?.signature!==signature)intent.current={signature,requestId:crypto.randomUUID()};
    locked.current=true;setBusy(true);setError('');
    try{const result=await localJson<{operationId:string}>('/api/pipeline/continue-batch',{requestId:intent.current.requestId,selections});
      intent.current=null;setOperation(result.operationId);setMessage('操作已登记，后台继续处理。');}
    catch(failure){setError((failure as Error).message);}finally{locked.current=false;setBusy(false);}
  }
  usePolling(async()=>{
    if(!operation)return;
    const result=await localJson<{state:string;outcomes:{ok:boolean;message?:string}[]}>(`/api/pipeline/operations/${operation}`);
    if(!pipelineBusy(result.state)){setOperation('');setMessage(result.outcomes.every(row=>row.ok)?'操作已执行。':'部分操作需要核对。');
      setError(result.outcomes.filter(row=>!row.ok).map(row=>row.message).join('；'));void onRefresh().catch(()=>{});}
  },operation&&expanded?4000:null,{onError:failure=>setError((failure as Error).message)});
  if(!next)return null;
  return <div className="processing-primary"><p className="subtle">{next.reason}</p>
    {next.canContinue?<button disabled={busy||!!operation} onClick={()=>void enqueue(next.action)}>{busy?'正在登记…':next.action==='archive'?'重试整理':'继续处理'}</button>:
      next.action==='resume'?<button disabled={busy||!!operation} onClick={()=>void enqueue('resume')}>继续此视频</button>:
      next.kind==='automatic'?<button className="secondary" disabled={busy||!!operation} onClick={()=>void enqueue('pause')}>暂停此视频</button>:null}
    {message&&<p className="subtle" role="status">{message}</p>}{error&&<p className="error" role="alert">{error}</p>}
  </div>;
}

export default function VideoProcessing({groups,onRefresh,candidates=[]}:{groups:ProcessingGroup[];onRefresh:()=>Promise<void>;candidates?:Candidate[]}) {
  if(!groups.length)return null;
  return <div className="video-processing">{groups.map(group=>{
    const current=group.items.filter(item=>!item.coveredByRecovery);
    const old=group.items.filter(item=>!!item.coveredByRecovery);
    const viewKey=`processing:${group.id}:${group.generation}`;
    return <details key={group.id} id={`processing-${group.id}`} className="processing-group"
      open={viewMemory.read(viewKey,false)} onToggle={event=>{
        if(event.target===event.currentTarget)viewMemory.save(viewKey,event.currentTarget.open);
      }}>
      <summary>切片处理 · {current.some(i=>i.state==='attention')?'需处理':!current.length||current.every(i=>processingFinished(i.state))?'处理已结束':'查看进度与操作'}</summary>
      <div className="processing-group-body">
        <ProcessingPrimary group={group} onRefresh={onRefresh}/>
        {current.map(item=><ProcessingItem key={item.id} group={group} item={item} onRefresh={onRefresh} candidates={candidates}/>)}
        {!!old.length&&<details className="processing-history"><summary>已处理的旧尝试 · {old.length}</summary>
          <p className="subtle">同一视频版本的可解码部分已通过照片导入核验，旧尝试仅保留记录。</p>
        </details>}
      </div>
    </details>;
  })}</div>;
}
