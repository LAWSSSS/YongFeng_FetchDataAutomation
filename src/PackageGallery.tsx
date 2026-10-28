import { useEffect, useState } from 'react';
import './package-gallery.css';
import {localJson} from './local-api';

type View = {available:boolean;message?:string;sessionId?:string;name?:string;category?:string;count?:number;expectedCount?:number;canDownload?:boolean};
type Contents = {names:string[];page:number;pages:number;count:number};
async function read<T>(url:string, body?:object):Promise<T> {
  return localJson<T>(url,body);
}

export default function PackageGallery({id}:{id:string}) {
  const base=`/api/slicing/packages/${encodeURIComponent(id)}`;
  const [view,setView]=useState<View|null>(null);
  const [contents,setContents]=useState<Contents|null>(null);
  const [page,setPage]=useState(1);
  const [selected,setSelected]=useState<string|null>(null);
  const [error,setError]=useState('');
  useEffect(()=>{
    let disposed=false,key='';let timer:ReturnType<typeof setInterval>|undefined;
    const close=()=>{if(key)navigator.sendBeacon(`${base}/view/close`,new Blob([JSON.stringify({sessionId:key})],{type:'application/json'}));};
    void read<View>(`${base}/view`,{}).then(result=>{
      key=result.sessionId||'';
      if(disposed){close();return;}
      setView(result);
      if(key)timer=setInterval(()=>{void read<View>(`${base}/view`,{sessionId:key}).catch(failure=>setError((failure as Error).message));},30000);
    }).catch(failure=>{if(!disposed)setError((failure as Error).message);});
    window.addEventListener('pagehide',close);
    return()=>{disposed=true;if(timer)clearInterval(timer);close();window.removeEventListener('pagehide',close);};
  },[base]);
  useEffect(()=>{
    if(!view?.sessionId)return;
    let disposed=false;
    setContents(null);
    void read<Contents>(`${base}/contents?session=${view.sessionId}&page=${page}`).then(result=>{
      if(!disposed)setContents(result);
    }).catch(failure=>{if(!disposed)setError((failure as Error).message);});
    return()=>{disposed=true;};
  },[base,view?.sessionId,page]);
  const image=(name:string,thumbnail=false)=>`${base}/images/${encodeURIComponent(name)}?session=${view?.sessionId}&thumbnail=${thumbnail}`;
  return <div className="gallery-shell"><header className="gallery-header"><a href="/">返回采集助手</a>
    <h1>{view?.name||'照片包'}</h1><p>{view?.available?`${view.category==='bed'?'车斗':'磁盘吊'} · 本地可查看 ${view.count} / ${view.expectedCount} 张`:'仅查看本地保留的照片'}</p>
    {view?.canDownload&&<a className="gallery-download" href={`${base}/download?session=${view.sessionId}`} download>下载这个 ZIP</a>}
  </header><main>
    {error&&<p className="error" role="alert">{error}</p>}
    {!view&&!error&&<p role="status">正在打开照片包…</p>}
    {view&&!view.available&&<section className="panel empty"><h2>本地照片不可查看</h2><p>{view.message}</p><p>平台成果和历史记录保留。</p></section>}
    {selected&&<section className="gallery-preview" aria-label="照片大图"><div className="actions"><strong>{selected}</strong><button className="secondary" onClick={()=>setSelected(null)}>关闭大图</button></div>
      <img src={image(selected)} alt={selected} onError={()=>setError('这张本地照片暂不可用，请重新打开照片包。')}/></section>}
    {view?.available&&<><div className="gallery-toolbar"><span>第 {contents?.page||page} / {contents?.pages||1} 页</span><div className="actions">
      <button className="secondary" disabled={page<=1||!contents} onClick={()=>setPage(page-1)}>上一页</button>
      <button className="secondary" disabled={!contents||page>=contents.pages} onClick={()=>setPage(page+1)}>下一页</button></div></div>
      {!contents?<p role="status">正在读取照片…</p>:<div className="package-photo-grid">{contents.names.map(name=><button className="package-photo" key={name} onClick={()=>{setSelected(name);window.scrollTo({top:0,behavior:'smooth'});}}>
        <img loading="lazy" src={image(name,true)} alt={name}/><span>{name}</span></button>)}</div>}</>}
  </main></div>;
}
