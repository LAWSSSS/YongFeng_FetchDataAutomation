export type RecordChoice={key:string;id:string;revision:number;kind:'item'|'receipt'};

export function toggleRecord(previous:Map<string,RecordChoice>,choice:RecordChoice) {
  const next=new Map(previous);
  if(next.has(choice.key))next.delete(choice.key);else next.set(choice.key,choice);
  return next;
}

export function reconcileRecordChoices(previous:Map<string,RecordChoice>,current:RecordChoice[],knownKeys?:Set<string>) {
  const valid=new Map(current.map(row=>[row.key,row]));let changed=false;
  const next=new Map(previous);
  for(const [key,row] of next){
    if(knownKeys&&!knownKeys.has(key))continue;
    if(valid.get(key)?.revision!==row.revision){next.delete(key);changed=true;}
  }
  return changed?next:previous;
}
