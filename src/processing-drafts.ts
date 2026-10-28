export type ProcessingDraft={value:string;revision:number;generation:number};
export function editProcessingDraft(previous:ProcessingDraft|undefined,value:string,revision:number,generation:number):ProcessingDraft {
  return previous?{...previous,value}:{value,revision,generation};
}
export function currentProcessingDraft(draft:ProcessingDraft,revision:number,generation:number){
  return draft.revision===revision&&draft.generation===generation;
}
