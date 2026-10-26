export type CapturePlan = {id:string;tasks:{status:string;reason?:string}[]};
type Call = <T>(path:string,body:unknown)=>Promise<T>;

/** Reuse the same prepared intent after a lost submit response. */
export function collectionAction(call:Call) {
  let prepared:{key:string;plan:CapturePlan}|undefined;
  let running=false;
  return async <T>(searchId:string,recordIds:string[],admissionRevision:number,downloadAfterCollection:boolean) => {
    if(running)throw new Error('正在提交，请稍候。');
    running=true;
    try {
      const key=JSON.stringify([searchId,[...recordIds].sort(),admissionRevision,downloadAfterCollection]);
      if(prepared?.key!==key) {
        const plan=await call<CapturePlan>('/preview',{searchId,recordIds});
        prepared={key,plan};
      }
      const plan=prepared.plan;
      if(plan.tasks.some(task=>task.status==='invalid'))throw new Error('所选实绩包含无效采集时段，请检查后重新选择。');
      if(!plan.tasks.some(task=>task.status==='ready'))throw new Error('所选实绩已有任务，或未通过准入；没有需要新建的采集。');
      return await call<T>('/jobs',{planId:plan.id,downloadAfterCollection});
    } finally {running=false;}
  };
}

export function initialCapture(start:string) {
  const stamp=new Date(`${start.replace(' ','T')}+08:00`).getTime();
  if(!Number.isFinite(stamp))return null;
  const format=(ms:number)=>new Date(ms+8*3600_000).toISOString().slice(0,19).replace('T',' ');
  return {start:format(stamp+5*60_000),end:format(stamp+65*60_000)};
}
