export type ProcessingDraft={value:string;revision:number;generation:number};
export function validProcessingDraft(value:unknown):value is ProcessingDraft {
  if(!value||typeof value!=='object')return false;
  const row=value as Record<string,unknown>;
  return typeof row.value==='string'&&Number.isInteger(row.revision)&&Number.isInteger(row.generation);
}
export function validProcessingDrafts(value:unknown):value is Record<string,ProcessingDraft> {
  return !!value&&typeof value==='object'&&!Array.isArray(value)&&Object.values(value).every(validProcessingDraft);
}
export function editProcessingDraft(previous:ProcessingDraft|undefined,value:string,revision:number,generation:number):ProcessingDraft {
  return previous?{...previous,value}:{value,revision,generation};
}
export function currentProcessingDraft(draft:ProcessingDraft,revision:number,generation:number){
  return draft.revision===revision&&draft.generation===generation;
}
