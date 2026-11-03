import test from 'node:test';
import assert from 'node:assert/strict';
import {pipelineBusy,pipelineSelection} from '../src/pipeline-view.ts';

const row={id:'media',mediaId:'media',generation:4,name:'video',state:'processing',stage:'sampling',message:'',
  nextAction:{kind:'automatic',action:'continue',stage:'sampling',reason:'',canContinue:true,
    selections:[{id:'retry',expectedRevision:3}],packages:[{id:'package',expectedRevision:8}],
    versionSelections:[{id:'first',expectedRevision:7},{id:'second',expectedRevision:9}]}};

test('normal continuation carries only the backend-approved nodes and revisions',()=>{
  assert.deepEqual(pipelineSelection(row),{mediaId:'media',generation:4,selections:row.nextAction.selections,packages:row.nextAction.packages});
});
test('single-video pause uses all version revisions without restarting photo packages',()=>{
  assert.deepEqual(pipelineSelection(row,'pause'),{mediaId:'media',generation:4,action:'pause',
    selections:row.nextAction.versionSelections,packages:[]});
});
test('archive retry carries the exact registered file and video node',()=>{
  const value=pipelineSelection({...row,nextAction:{...row.nextAction,action:'archive',fileId:'registered-file'}});
  assert.equal(value.fileId,'registered-file');assert.equal(value.itemId,'first');assert.equal(value.expectedRevision,7);
});
test('archive retry selects the file-bound node instead of the first displayed model',()=>{
  const value=pipelineSelection({...row,nextAction:{...row.nextAction,action:'archive',fileId:'magnet-file',actionItemId:'second'}});
  assert.equal(value.fileId,'magnet-file');assert.equal(value.itemId,'second');assert.equal(value.expectedRevision,9);
});
test('missing file or mismatched binding never falls back to another model record',()=>{
  for(const nextAction of [
    {...row.nextAction,action:'archive',fileId:'file',actionItemId:'absent'},
    {...row.nextAction,action:'archive',fileId:'file',actionItemId:''},
    {...row.nextAction,action:'archive',actionItemId:'second'},
    {...row.nextAction,action:'archive',fileId:'file',versionSelections:[]},
  ])assert.throws(()=>pipelineSelection({...row,nextAction}),/尚未执行整理/);
});
test('operation failure is terminal so that the page can present a next action',()=>{
  assert.equal(pipelineBusy('queued'),true);assert.equal(pipelineBusy('running'),true);
  assert.equal(pipelineBusy('attention'),false);assert.equal(pipelineBusy('complete'),false);
});
