type Options = {timeoutMs?:number;fetcher?:typeof fetch};

/** JSON control calls only. Media streams/downloads keep their own lifetimes. */
export async function localJson<T>(path:string,body?:unknown,options:Options={}):Promise<T> {
  const abort=new AbortController();
  const timer=setTimeout(()=>abort.abort(),options.timeoutMs??(body===undefined?15000:30000));
  try {
    const response=await (options.fetcher||fetch)(path,{signal:abort.signal,...(body===undefined?{}:{
      method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body),
    })});
    let result:unknown;
    try {result=await response.json();} catch {throw new Error('本地程序返回异常，请刷新状态后重试。');}
    if(!response.ok){
      const error=result as {message?:unknown;detail?:unknown};
      throw new Error(typeof error?.message==='string'?error.message:typeof error?.detail==='string'?error.detail:`请求未完成（${response.status}），请刷新状态后重试。`);
    }
    return result as T;
  } catch(error) {
    if(abort.signal.aborted)throw new Error(body===undefined?'读取超时，后台任务仍在继续，请重试。':'请求超时，结果尚未确认；请重试原操作，程序会核对请求编号。');
    if(error instanceof TypeError)throw new Error('本地程序暂时无法连接，已保存的任务和进度保留。');
    throw error;
  } finally {clearTimeout(timer);}
}
