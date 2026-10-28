import { useRef, useState } from 'react';
import './video-processing.css';
import {localJson} from './local-api';
import {editProcessingDraft,currentProcessingDraft} from './processing-drafts';
import type {ProcessingDraft} from './processing-drafts';

type Category={state:string;message?:string;errorCode?:string;intervals?:{start:number;end:number}[];packageId?:string;actualImages?:number};
type Item={id:string;name:string;path:string;revision:number;state:string;errorCode?:string;message?:string;blockedReason?:string;sourceDatasetId:string;categories:Record<string,Category>};
type Pack={id:string;itemId:string;revision:number;category:string;state:string;imageCount?:number;cleanupState?:string;localCleanupState?:string};
export type ProcessingGroup={id:string;videoId?:string;name:string;generation:number;owner:string;blockedReason?:string;createdAt?:string;items:Item[];packages:Pack[];files?:{id:string;archiveState:string;archiveMessage?:string;cleanupEligible?:boolean;cleanupReason?:string;cleanup?:{state:string;message?:string}}[]};
type Candidate={id:string;name:string;qualityStart:string;revision:number;activeRevision:number};
const labels:Record<string,string>={pending:'待识别',analyzing:'识别中',platform_processing:'照片处理中',attention:'需处理',complete:'完成',no_target:'未检出',review:'待确认',queued:'排队上传',uploaded:'等待导入',uploading:'上传中',importing:'导入中',waiting:'等待平台',ready:'准备导出',superseded:'旧版已停止'};
const text=(category:Category)=>category.intervals?.map(row=>`${row.start}-${row.end}`).join(', ')||'';
function intervals(value:string){return value.split(/[,，]/).map(x=>x.trim()).filter(Boolean).map(x=>{
  const m=x.match(/^(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)$/);
  if(!m||Number(m[2])<Number(m[1]))throw new Error('区间请按“开始-结束秒”填写，多个区间用逗号分开。');
  return {start:Number(m[1]),end:Number(m[2])};
});}

function ProcessingItem({group,item,onRefresh,candidates}:{group:ProcessingGroup;item:Item;onRefresh:()=>Promise<void>;candidates:Candidate[]}) {
  const [busy,setBusy]=useState(false),[error,setError]=useState('');
  const [pathDraft,setPathDraft]=useState<ProcessingDraft>();
  const [notice,setNotice]=useState('');
  const [drafts,setDrafts]=useState<Record<string,ProcessingDraft>>({});
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
      if(action==='bind')setPathDraft(undefined);
      if(action==='review')setDrafts(before=>{const next={...before};delete next[(extra as {category:string}).category];return next;});
      intent.current=null;setNotice(action==='retry-archive'?'归档已排队，文件使用结束后自动处理。':'操作已登记，后台继续处理。');await onRefresh();
    }catch(failure){setError((failure as Error).message);}finally{locked.current=false;setBusy(false);}
  }
  const blocked=!!item.blockedReason;
  return <div className="video-processing-item"><div className="processing-status"><strong>{item.sourceDatasetId==='27912'?'车斗来源':'磁盘吊来源'}</strong><span>{labels[item.state]||item.state}</span></div>
    {item.blockedReason&&<p className="notice">{item.blockedReason}</p>}
    {item.message&&item.state!=='complete'&&<p className="subtle">{item.message}</p>}
    <div className="actions">
      {item.state==='attention'&&<button className="secondary" disabled={busy||blocked} onClick={()=>void act('retry')}>重试切片</button>}
      {item.state==='attention'&&item.errorCode==='media_incomplete'&&<button disabled={busy||blocked} onClick={()=>void act('recover')}>处理可解码片段</button>}
      {item.state==='pending'&&<button className="secondary" disabled={busy||blocked} onClick={()=>void act('start')}>开始本条</button>}
    </div>
    {!group.videoId&&item.id===group.items[0].id&&<details><summary>关联已有采集记录</summary><label className="processing-link">采集视频<select value={link} onChange={event=>setLink(event.target.value)}><option value="">选择需要关联的当前版本</option>{candidates.map(v=><option key={v.id} value={v.id}>{v.name} · {v.qualityStart}</option>)}</select></label>
      <button className="secondary" disabled={busy||!link} onClick={()=>{const target=candidates.find(v=>v.id===link);if(target)void act('link',{videoId:target.id,collectionRevision:target.activeRevision,collectionExpectedRevision:target.revision});}}>核对原片并关联</button></details>}
    {item.id===group.items[0].id&&group.files?.map(file=><div className="processing-archive" key={file.id}>
      <p className="subtle">{file.cleanup?.state==='complete'?'本地文件已清理，成果和记录保留。':file.archiveState==='preserved'?'按保留规则保持原位。':file.archiveState==='complete'?'已归档。':file.archiveMessage||'等待分类归档。'}</p>
      {['waiting','attention'].includes(file.archiveState)&&file.cleanup?.state!=='complete'&&<button className="secondary" disabled={busy||blocked} onClick={()=>void act('retry-archive',{fileId:file.id})}>{busy?'正在登记…':'重试归档'}</button>}
    </div>)}
    {item.id===group.items[0].id&&group.files?.filter(f=>f.archiveState==='complete'&&f.cleanup?.state!=='complete').map(file=><div key={file.id}><button className="secondary" disabled={busy||blocked||file.cleanupEligible===false} onClick={()=>{
      if(window.confirm('删除本视频已核验的本地视频、照片和压缩包？平台成果与历史记录保留。'))void act('cleanup',{fileId:file.id});
    }}>清理已核验的本地媒体</button>{file.cleanupEligible===false&&<p className="subtle">{file.cleanupReason}</p>}</div>)}
    {!['complete','superseded'].includes(item.state)&&<details><summary>本地原片</summary><div className="processing-editor">
      {item.path&&<video controls preload="none" src={`/api/slicing/${item.id}/stream`}/>}
      <label>原片路径<input value={pathDraft?.value??item.path??''} onChange={e=>setPathDraft(before=>editProcessingDraft(before,e.target.value,item.revision,group.generation))}/></label>
      {pathDraft&&!currentProcessingDraft(pathDraft,item.revision,group.generation)&&<p className="notice">记录已变化，请重新核对路径。<button className="link" onClick={()=>setPathDraft(undefined)}>载入当前路径</button></p>}
      <button className="secondary" disabled={busy||blocked||!pathDraft?.value||pathDraft.value===item.path||!currentProcessingDraft(pathDraft,item.revision,group.generation)} onClick={()=>void act('bind',{path:pathDraft?.value})}>重新绑定原片</button>
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

export default function VideoProcessing({groups,onRefresh,candidates=[]}:{groups:ProcessingGroup[];onRefresh:()=>Promise<void>;candidates?:Candidate[]}) {
  if(!groups.length)return null;
  return <div className="video-processing">{groups.map(group=><details key={group.id} id={`processing-${group.id}`} className="processing-group">
    <summary>切片处理 · {group.items.some(i=>i.state==='attention')?'需处理':group.items.every(i=>['complete','no_target'].includes(i.state))?'已完成':'查看进度与操作'}</summary>
    <div className="processing-group-body">
      {group.items.map(item=><ProcessingItem key={item.id} group={group} item={item} onRefresh={onRefresh} candidates={candidates}/>)}</div>
  </details>)}</div>;
}
