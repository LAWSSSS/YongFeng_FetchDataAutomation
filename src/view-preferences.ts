export type Page = 'collect' | 'videos' | 'slicing' | 'settings';

export function initialVideoTarget(search:string):{videoId?:string;mediaId?:string}|null {
  const query=new URLSearchParams(search);
  const videoId=query.get('videoId')||undefined,mediaId=query.get('mediaId')||undefined;
  return query.get('tab')==='videos'&&(videoId||mediaId)?{videoId,mediaId}:null;
}

export function navigationSearch(search:string,page:Page):string {
  const query=new URLSearchParams(search);
  query.set('tab',page);
  query.delete('videoId');query.delete('mediaId');
  return `?${query}`;
}

/** URL links take precedence; older bookmarks keep their existing destination. */
export function initialPage(search:string,saved:string|null):Page {
  const requested=new URLSearchParams(search).get('tab');
  if(requested==='collect'||requested==='videos'||requested==='slicing'||requested==='settings')return requested;
  if(saved==='audit')return 'videos';
  return saved==='videos'||saved==='slicing'||saved==='settings'?saved:'collect';
}

/** Only visible progress needs the faster read cadence; scheduling is server owned. */
export const readInterval=(active:boolean,offlineWaiting=false)=>active?4000:offlineWaiting?30000:15000;
export function pipelineReadInterval(counts?:{processing:number},offlineWaiting=false):number {
  return readInterval((counts?.processing||0)>0,offlineWaiting);
}

export function slicingReadInterval(state:{busy?:boolean;phase?:string;localWorkers?:{active:number};packages?:{busy?:boolean}}):number {
  const processing=state.busy||(state.localWorkers?.active||0)>0||state.packages?.busy
    ||['scanning','analyzing','exporting','archiving'].includes(state.phase||'');
  return readInterval(!!processing,state.phase==='waiting_connection');
}

export function transferProgress(done:number,total:number):string {
  const bytes=(value:number)=>{
    if(value>=1024**3)return `${(value/1024**3).toFixed(2)} GB`;
    if(value>=1024**2)return `${(value/1024**2).toFixed(1)} MB`;
    if(value>=1024)return `${(value/1024).toFixed(1)} KB`;
    return `${Math.floor(value)} B`;
  };
  return `${bytes(Math.max(0,Math.min(done,total)))} / ${bytes(Math.max(0,total))}`;
}
