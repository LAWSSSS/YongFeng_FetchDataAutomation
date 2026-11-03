import {announceMediaChange} from './media-updates.ts';
type Options = {timeoutMs?:number;fetcher?:typeof fetch;signal?:AbortSignal;method?:'POST'|'PATCH'};

export type ReadContext={signal:AbortSignal;current:()=>boolean};

/** Coalesce the same view and cancel superseded reads. Never owns mutations. */
export function createReadCycle(){
  let generation=0;
  let active:{key:string;controller:AbortController;promise:Promise<unknown>}|undefined;
  function cancel(){++generation;active?.controller.abort();active=undefined;}
  return {cancel,run<T>(key:string,task:(context:ReadContext)=>Promise<T>):Promise<T>{
    if(active?.key===key)return active.promise as Promise<T>;
    cancel();
    const version=generation,controller=new AbortController();
    const context={signal:controller.signal,current:()=>version===generation&&!controller.signal.aborted};
    const promise=Promise.resolve().then(()=>task(context)).finally(()=>{
      if(active?.controller===controller)active=undefined;
    });
    active={key,controller,promise};
    return promise;
  }};
}

export const cancelledRead=(error:unknown)=>error instanceof Error&&error.name==='AbortError';

/** JSON control calls only. Media streams/downloads keep their own lifetimes. */
export async function localJson<T>(path:string,body?:unknown,options:Options={}):Promise<T> {
  const abort=new AbortController();
  const cancel=()=>abort.abort();
  // A cancelled page must not cancel a submitted business operation.
  const parent=body===undefined?options.signal:undefined;
  if(parent?.aborted)abort.abort();
  else parent?.addEventListener('abort',cancel,{once:true});
  const timer=setTimeout(()=>abort.abort(),options.timeoutMs??(body===undefined?15000:30000));
  try {
    if(parent?.aborted)throw new DOMException('读取已取消','AbortError');
    const response=await (options.fetcher||fetch)(path,{signal:abort.signal,...(body===undefined?{}:{
      method:options.method||'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),
    })});
    let result:unknown;
    try {result=await response.json();} catch {throw new Error('本地程序返回异常，请刷新状态后重试。');}
    if(!response.ok){
      const error=result as {message?:unknown;detail?:unknown};
      throw Object.assign(new Error(typeof error?.message==='string'?error.message:typeof error?.detail==='string'?error.detail:`请求未完成（${response.status}），请刷新状态后重试。`),{status:response.status});
    }
    if(parent?.aborted)throw new DOMException('读取已取消','AbortError');
    if(body!==undefined)announceMediaChange(path);
    if(body!==undefined&&typeof window!=='undefined'&&(result as {operationKind?:string})?.operationKind==='record-delete'){
      window.dispatchEvent(new CustomEvent('record-operation',{detail:result}));
    }
    return result as T;
  } catch(error) {
    if(parent?.aborted)throw new DOMException('读取已取消','AbortError');
    if(abort.signal.aborted)throw new Error(body===undefined?'读取超时，后台任务仍在继续，请重试。':'请求超时，结果尚未确认；请重试原操作，程序会核对请求编号。');
    if(error instanceof TypeError)throw new Error('本地程序暂时无法连接，已保存的任务和进度保留。');
    throw error;
  } finally {clearTimeout(timer);parent?.removeEventListener('abort',cancel);}
}
