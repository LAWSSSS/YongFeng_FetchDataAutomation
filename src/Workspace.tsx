import {useCallback,useEffect,useRef,useState} from 'react';
import {FolderOpen} from '@phosphor-icons/react';
import {usePolling} from './usePolling';
import {createReadCycle,localJson} from './local-api';
import './workspace.css';

type State={online:boolean;maintenance:boolean;totalBytes:number;freeBytes?:number;fixedLocation?:boolean;
  availability?:{message?:string};counts:Record<string,number>};
const size=(bytes:number)=>bytes>=1024**3?`${(bytes/1024**3).toFixed(1)} GB`:`${(bytes/1024**2).toFixed(1)} MB`;
export default function Workspace({onChanged:_onChanged}:{onChanged:()=>Promise<void>}){
  const [state,setState]=useState<State|null>(null),[error,setError]=useState(''),[busy,setBusy]=useState(false);
  const reads=useRef(createReadCycle());
  const refresh=useCallback(async()=>{await reads.current.run('storage',async context=>{
    try{const value=await localJson<State>('/api/workspace',undefined,{signal:context.signal});
      if(context.current()){setState(value);setError('');}}
    catch(failure){if(context.current())setError((failure as Error).message);}
  });},[]);
  useEffect(()=>{void refresh();return()=>reads.current.cancel();},[refresh]);
  usePolling(refresh,30000,{immediate:false});
  async function open(){setBusy(true);setError('');try{await localJson('/api/workspace/open',{});}catch(failure){setError((failure as Error).message);}finally{setBusy(false);}}
  return <section className="panel workspace-panel" aria-label="本地数据">
    <div className="section-title"><div><h2><FolderOpen size={22} aria-hidden="true"/>本地数据</h2>
      <p>视频和照片自动保存在程序旁的“数据”文件夹。</p></div>
      <button className="secondary" disabled={busy||!state?.online} onClick={()=>void open()}>{busy?'正在打开…':'打开数据文件夹'}</button></div>
    {state&&<p className="storage-summary" role="status"><span className={`status-dot ${state.online?'online':'offline'}`}/>
      {state.online?'可用':'等待磁盘恢复'} · 已登记 {size(state.totalBytes)}{typeof state.freeBytes==='number'&&` · 可用空间 ${size(state.freeBytes)}`}</p>}
    {!state?.online&&state?.availability?.message&&<p className="notice">{state.availability.message}</p>}
    {error&&<p className="error" role="alert">{error}</p>}
  </section>;
}
