export type PipelineChoice={id:string;expectedRevision:number};
export type NextAction={kind:'automatic'|'waiting'|'manual'|'complete';action:'continue'|'archive'|'open'|'none'|'resume';
  stage:string;reason:string;canContinue:boolean;canPause?:boolean;autoEligible?:boolean;fileId?:string;actionItemId?:string;
  selections:PipelineChoice[];packages:PipelineChoice[];versionSelections?:PipelineChoice[];
  issue?:{code:string;stage?:string;message:string}|null;environmentBlocker?:{code:string;message:string}|null;
  progress?:{done:number;total:number;unit:string}|null};
export type PipelineVideo={id:string;mediaId:string;generation:number;videoId?:string;name:string;processingEntry?:string|null;
  version?:number;updatedAt?:string;confirmedAt?:string;hasResultRecord?:boolean;
  packages?:{id:string;itemId:string;category:string;state:string;imageCount?:number}[];
  state:string;stage:string;message:string;nextAction:NextAction};
export type PipelineAction=NextAction['action']|'pause';

export function pipelineSelection(row:PipelineVideo,action:PipelineAction=row.nextAction.action){
  const next=row.nextAction;
  if(action==='archive'){
    // The file being archived can belong to the second model record. A name,
    // group ordering or the first displayed row is never an operation binding.
    const item=next.actionItemId===undefined?next.versionSelections?.[0]:
      next.versionSelections?.find(choice=>choice.id===next.actionItemId);
    if(!item||!next.fileId)throw new Error('整理记录已变化，请刷新后再试；尚未执行整理。');
    return {mediaId:row.mediaId,generation:row.generation,action:'archive',fileId:next.fileId,
      itemId:item.id,expectedRevision:item.expectedRevision};
  }
  return {mediaId:row.mediaId,generation:row.generation,...(['pause','resume'].includes(action)?{action}:{}),
    selections:['pause','resume'].includes(action)?next.versionSelections||next.selections:next.selections,
    packages:['pause','resume'].includes(action)?[]:next.packages};
}

export const pipelineBusy=(state?:string)=>!!state&&['queued','running','waiting_connection'].includes(state);
