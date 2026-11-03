/** Honest terminal outcomes shared by history, filters and manual controls. */
const terminal=new Set(['complete','partial_complete','no_target','uncuttable']);
export function processingFinished(state:string){return terminal.has(state);}
export const processingLabels:Record<string,string>={complete:'已完成',partial_complete:'可用部分已完成',
  no_target:'未检出目标',uncuttable:'未找到可用画面'};
export function terminalGroupState(states:string[]):string {
  if(states.every(state=>state==='uncuttable'))return 'uncuttable';
  if(states.every(state=>state==='no_target'||state==='uncuttable'))return 'no_target';
  return states.includes('partial_complete')||states.includes('uncuttable')?'partial_complete':'complete';
}
