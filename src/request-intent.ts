/** Retain an operation identity across errors; only a confirmed reply clears it. */
export function requestIntents(){
  const values=new Map<string,{signature:string;requestId:string}>();
  return {
    payload<T extends object>(name:string,body:T):T&{requestId:string}{
      const signature=JSON.stringify(body);
      if(values.get(name)?.signature!==signature)values.set(name,{signature,requestId:crypto.randomUUID()});
      return {...body,requestId:values.get(name)!.requestId};
    },
    complete(name:string){values.delete(name);},
  };
}
