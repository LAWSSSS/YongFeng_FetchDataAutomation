export type RecordChoice={key:string;id:string;revision:number;kind:'item'|'receipt';parentId?:string;parentRevision?:number};

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

export function selectSubmissionReceipts(previous:Map<string,RecordChoice>,receipts:RecordChoice[]) {
  const parents=new Set(receipts.map(row=>row.id));
  const next=new Map([...previous].filter(([,row])=>row.kind!=='item'||!parents.has(row.parentId||row.id.split(':')[0])));
  for(const row of receipts)next.set(row.key,row);
  return next;
}

export function toggleSubmissionChoice(previous:Map<string,RecordChoice>,choice:RecordChoice) {
  if(previous.has(choice.key))return toggleRecord(previous,choice);
  if(choice.kind==='receipt')return selectSubmissionReceipts(previous,[choice]);
  if(previous.has(`receipt:${choice.parentId||choice.id.split(':')[0]}`))return previous;
  return toggleRecord(previous,choice);
}

export function reconcileSubmissionChoices(previous:Map<string,RecordChoice>,receipts:{id:string;revision:number}[]) {
  const parents=new Map(receipts.map(row=>[row.id,row.revision]));
  const next=new Map([...previous].filter(([,row])=>row.kind==='receipt'
    ?parents.get(row.id)===row.revision
    :parents.get(row.parentId||row.id.split(':')[0])===row.parentRevision));
  return next.size===previous.size?previous:next;
}
