/** Drafts are browser-local input, never submitted task data or credentials. */
const memory=new Map<string,unknown>();
export function readDraft<T>(key:string,fallback:T,valid:(value:unknown)=>value is T):T {
  try {
    const value=JSON.parse(sessionStorage.getItem(`input-draft:${key}`)||'null');
    if(valid(value)){memory.set(key,value);return value;}
  }catch{/* Private browsing can disable session storage. */}
  const value=memory.get(key);
  return valid(value)?value:fallback;
}
export function writeDraft<T>(key:string,value:T|undefined) {
  if(value===undefined)memory.delete(key);else memory.set(key,value);
  try {
    if(value===undefined)sessionStorage.removeItem(`input-draft:${key}`);
    else sessionStorage.setItem(`input-draft:${key}`,JSON.stringify(value));
  }catch{/* Keep the draft across tab navigation in memory. */}
}
export const dateRangeDraft=(value:unknown):value is {dateFrom:string;dateTo:string}=>{
  if(!value||typeof value!=='object')return false;
  const row=value as Record<string,unknown>;
  return typeof row.dateFrom==='string'&&typeof row.dateTo==='string';
};
