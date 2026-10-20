import {useCallback, useEffect, useState} from 'react';
import {usePolling} from './usePolling';
import {effectiveUploadFilter,platformDatasetUrl,queueComplete} from './upload-queue';
import './upload-queue.css';

type Item = {id:string;name:string;path:string;size:number;state:string;phase:string;bytes:number;speed:number;
  retries:number;message:string;errorCode:string;jobId:string;nextAttempt:number;removable:boolean};
type Target = {id:string;name:string;items:Item[];counts:Record<string,number>;activeUploads:number;activeImport:string;
  removedItems:{id:string;name:string;path:string;removedAt:string;message:string}[];
  blocked:boolean;message:string;batches:{id:string;state:string;jobId?:string;message:string}[]};
type State = {enabled:boolean;connected:boolean;directory:string;validationMode:boolean;parallelFilesPerTarget:number;
  scan:null|{id:string;state:string;message:string;added:number;review:number};targets:Target[]};
const labels:Record<string,string> = {queued:'排队中',checking:'重新核对',uploading:'上传中',uploaded:'等待导入',
  importing:'平台导入中',complete:'已导入平台',attention:'待核对',cleanup:'清理本条分片',retry_wait:'等待单条重试',
  waiting_connection:'等待网络',waiting_auth:'等待授权',waiting_disk:'等待外置盘'};
const phases:Record<string,string> = {queued:'等待上传',review:'待核对',paused:'断点已保留',reconciling:'核对已有对象及分片',
  creating:'建立分片上传',uploading:'上传分片',completing:'核验完整对象',uploaded:'上传核验完成',submitting:'提交导入',
  importing:'等待导入结果',complete:'导入完成',cleanup:'只清理本条失败分片'};

function bytes(value:number) {
  if (!value) return '0 B';
  const unit = Math.min(3,Math.floor(Math.log(value)/Math.log(1024)));
  return `${(value/1024**unit).toFixed(unit ? 1 : 0)} ${['B','KiB','MiB','GiB'][unit]}`;
}
async function api(path='', body?:object):Promise<State> {
  let response:Response;
  try {response=await fetch(`/api/uploads${path}`,body === undefined ? undefined : {
    method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),
  });} catch {throw new Error('本地程序暂时无法连接；队列已保存，恢复连接后刷新即可。');}
  const data=await response.json();
  if (!response.ok) throw new Error(data.message || '操作未完成，请检查连接后重试。');
  return data;
}

function TargetQueue({target,busy,onRetry,onRemove,onRestore}:{target:Target;busy:boolean;onRetry:(id:string)=>void;
  onRemove:(id:string)=>void;onRestore:(id:string)=>void}) {
  const [filter,setFilter]=useState('auto');
  const [expanded,setExpanded]=useState(false);
  const counts=target.counts;
  const selectedFilter=effectiveUploadFilter(filter,target.items);
  const rank:Record<string,number>={uploading:0,cleanup:1,importing:2,uploaded:3,attention:4};
  const rows=target.items.filter(item=>selectedFilter==='all' || (selectedFilter==='attention' ? item.state==='attention' : item.state!=='complete'))
    .sort((a,b)=>(rank[a.state] ?? 5)-(rank[b.state] ?? 5));
  const visible=expanded ? rows : rows.slice(0,8);
  return <div className="upload-target">
    <div className="upload-target-head"><h3>{target.name} <span>{target.id}</span></h3>
      <label>显示<select aria-label={`${target.name}上传筛选`} value={selectedFilter} onChange={e=>{setFilter(e.target.value);setExpanded(false);}}>
        <option value="active">未完成</option><option value="attention">待核对</option><option value="all">全部</option></select></label></div>
    <p className="upload-totals">上传 {target.activeUploads} · 等待导入 {counts.uploaded || 0} · 已完成 {counts.complete || 0} · 待核对 {counts.attention || 0}</p>
    <div className="upload-platform-actions">
      <a className="upload-platform-link" href={platformDatasetUrl(target.id)} target="_blank" rel="noreferrer">查看平台视频 ↗</a>
      <a className="upload-platform-link" href={platformDatasetUrl(target.id,'base')} target="_blank" rel="noreferrer">任务与日志 ↗</a>
    </div>
    {target.activeImport && <p className="upload-import">导入任务 {target.activeImport} · 等待平台结果</p>}
    {target.message && <p className={target.blocked ? 'upload-warning' : 'upload-explanation'}>{target.message}</p>}
    {!visible.length ? <p className="upload-empty">{target.items.length ? '此筛选范围没有记录；切换“全部”可查看已导入的视频。' : '尚无上传队列。扫描后仅添加此目标缺少的视频。'}</p> :
      <ul className="upload-files">{visible.map(item=><li key={item.id}>
        <div className="upload-file-title"><strong>{item.name}</strong><span className={`upload-state ${item.state}`}>{labels[item.state] || item.state}</span></div>
        <p className="upload-stage">{item.state==='queued' ? phases.queued : phases[item.phase] || item.phase}{item.jobId ? ` · 导入任务 ${item.jobId}` : ''} · 重试 {Math.min(item.retries,3)}/3</p>
        {item.state!=='complete' && <><progress aria-label={`${item.name}上传进度`} value={Math.min(item.bytes,item.size)} max={Math.max(item.size,1)} />
          <p className="upload-bytes">{bytes(item.bytes)} / {bytes(item.size)}{item.speed>0 ? ` · ${bytes(item.speed)}/s` : ''}</p></>}
        <p className={item.state==='attention' ? 'upload-warning' : 'upload-explanation'}>{item.state==='queued' ? '已进入队列，等待本目标的上传批次。' : item.message}</p>
        {['attention','waiting_disk','waiting_connection','waiting_auth','retry_wait'].includes(item.state) &&
          <button className="secondary" disabled={busy} onClick={()=>onRetry(item.id)}>{item.errorCode==='import_uncertain' ? '重新对账' : '重试此视频'}</button>}
        {item.removable && <button className="secondary" disabled={busy} onClick={()=>{
          if (window.confirm(`将“${item.name}”从待上传队列移除？这只移除程序里的队列记录，不会删除本地视频或平台已有数据；后续扫描会继续忽略这个文件版本。`)) onRemove(item.id);
        }}>移除重复项</button>}
        <details className="upload-location"><summary>本地文件位置</summary><p>{item.path}</p></details>
      </li>)}</ul>}
    {rows.length>8 && <button className="link" onClick={()=>setExpanded(value=>!value)}>{expanded ? '收起清单' : `展开全部 ${rows.length} 条`}</button>}
    {!!target.removedItems?.length && <details className="upload-removed">
      <summary>已移除的队列记录 · {target.removedItems.length} 条（可恢复）</summary>
      <ul>{target.removedItems.map(item=><li key={item.id}><strong>{item.name}</strong><p>{item.path}</p>
        <button className="secondary" disabled={busy} onClick={()=>onRestore(item.id)}>恢复到队列</button></li>)}</ul>
    </details>}
  </div>;
}

export default function UploadQueue({onSessionChange}:{onSessionChange:()=>Promise<void>}) {
  const [state,setState]=useState<State|null>(null);
  const [directory,setDirectory]=useState('/Volumes/out/视频');
  const [busy,setBusy]=useState(false);
  const [error,setError]=useState('');
  const refresh=useCallback(async()=>{const value=await api();setState(value);return value;},[]);
  useEffect(()=>{
    let active=true;
    void api().then(value=>{if(active){setState(value);setDirectory(value.directory);}}).catch(e=>{if(active)setError((e as Error).message);});
    return()=>{active=false;};
  },[]);
  usePolling(refresh, state?.enabled ? 4000 : 15000, {immediate:false,onError:e=>setError((e as Error).message)});
  useEffect(()=>{if(state)void onSessionChange().catch(()=>{});},[state?.connected,onSessionChange]);
  async function perform(path:string,body:object={}) {
    setBusy(true);setError('');
    try {setState(await api(path,body));}
    catch(e){setError((e as Error).message);}
    finally{setBusy(false);}
  }
  async function scan() {
    const key='simple-upload-scan-request';
    let saved:{directory:string;requestId:string}|null=null;
    try{saved=JSON.parse(sessionStorage.getItem(key)||'null');}catch{/* Invalid local draft creates a new request. */}
    const request=saved?.directory===directory.trim() ? saved : {directory:directory.trim(),requestId:crypto.randomUUID()};
    sessionStorage.setItem(key,JSON.stringify(request));
    setBusy(true);setError('');
    try {setState(await api('/scan',request));sessionStorage.removeItem(key);}
    catch(e){setError((e as Error).message);}
    finally{setBusy(false);}
  }
  const scanning=state?.scan?.state==='scanning';
  const complete=Boolean(state && !scanning && queueComplete(state.targets));
  const completedCount=state?.targets.reduce((sum,target)=>sum+(target.counts.complete||0),0)||0;
  return <section className="panel automatic-uploads" aria-label="自动上传与导入">
    <div className="section-title"><h2>自动上传与导入</h2><span>{complete ? '当前队列已完成' : state?.enabled ? '后台队列已启用' : '队列已暂停'}</span></div>
    <p className="upload-intro">只补传两个目标分别缺少的视频。点一次扫描添加本次文件，已入队任务会在关闭网页后继续处理。</p>
    <form className="upload-controls" onSubmit={e=>{e.preventDefault();void scan();}}>
      <label>补传目录（包含分类子目录）<input value={directory} onChange={e=>setDirectory(e.target.value)} required disabled={busy||scanning} /></label>
      <button disabled={busy||scanning||!state?.connected||!directory.trim()}>{scanning ? '正在扫描并核对…' : '扫描并补传'}</button>
      {state?.enabled ? <button className="secondary" type="button" disabled={busy} onClick={()=>void perform('/pause')}>暂停上传</button> :
        <button className="secondary" type="button" disabled={busy||!state?.connected} onClick={()=>void perform('/resume')}>继续队列</button>}
      <button className="link" type="button" disabled={busy} onClick={()=>void refresh().then(()=>setError('')).catch(e=>setError((e as Error).message))}>刷新状态</button>
    </form>
    {!state?.connected && <p className="upload-warning">请先在上方连接视觉AI平台。已有断点和导入凭证保留。</p>}
    {state?.validationMode ? <p className="upload-policy">单条验收：两个目标各核验成功一个真实视频后，自动开启每目标4个并行上传。</p> :
      <p className="upload-policy">每目标最多4个并行上传、1个导入，可预上传下一批。每30秒检查进展；卡住时只恢复本条，原视频保留。</p>}
    {state?.scan && <p className="upload-scan" role="status">{state.scan.message}</p>}
    {complete && <p className="upload-completion" role="status">{completedCount} 项目标任务已导入，当前没有等待上传的视频。下方已显示完成记录，可点击“查看平台视频”进入“数据内容”；新增视频后再扫描即可。</p>}
    {error && <p className="error" role="alert">{error}</p>}
    {state && <div className="upload-targets">{state.targets.map(target=><TargetQueue key={target.id} target={target} busy={busy}
      onRetry={id=>void perform(`/${id}/retry`)} onRemove={id=>void perform(`/${id}/remove`)}
      onRestore={id=>void perform(`/${id}/restore`)} />)}</div>}
  </section>;
}
