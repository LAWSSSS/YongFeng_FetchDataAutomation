import test from 'node:test';
import assert from 'node:assert/strict';
import {readDraft,writeDraft,dateRangeDraft} from '../src/draft-storage.ts';
import {validProcessingDraft,currentProcessingDraft} from '../src/processing-drafts.ts';
import {processingFinished,processingLabels,terminalGroupState} from '../src/processing-status.ts';
import {physicalState,slicePage} from '../src/slicing-view.ts';
import {platformStatus} from '../src/platform-availability.ts';

test('dates and manual drafts survive remount/storage failure without adopting changed versions',()=>{
  const storage=new Map();
  const previous=globalThis.sessionStorage;
  globalThis.sessionStorage={getItem:key=>storage.get(key)||null,setItem:(key,value)=>storage.set(key,value),removeItem:key=>storage.delete(key)};
  try{
    const dates={dateFrom:'2026-09-29',dateTo:'2026-10-08'};
    writeDraft('test-dates',dates);
    assert.deepEqual(readDraft('test-dates',{dateFrom:'today',dateTo:'today'},dateRangeDraft),dates);
    const draft={value:'30-120',revision:4,generation:2};
    writeDraft('test-interval',draft);
    globalThis.sessionStorage={getItem(){throw Error('blocked');},setItem(){throw Error('blocked');},removeItem(){throw Error('blocked');}};
    const restored=readDraft('test-interval',undefined,validProcessingDraft);
    assert.deepEqual(restored,draft);
    assert.equal(currentProcessingDraft(restored,5,3),false);
    writeDraft('test-interval',undefined);
    assert.equal(readDraft('test-interval',undefined,validProcessingDraft),undefined);
  }finally{globalThis.sessionStorage=previous;}
});

test('zero-usable-frame and partial outcomes end work without becoming false successful slicing',()=>{
  const row=state=>({id:state,name:state,state,sourceDatasetId:'27912',sourceVideoId:'fixture',categories:{bed:{state}}});
  assert.equal(physicalState([row('uncuttable')]),'uncuttable');
  assert.equal(physicalState([row('partial_complete')]),'partial_complete');
  assert.equal(terminalGroupState(['complete','uncuttable']),'partial_complete');
  for(const state of ['complete','partial_complete','no_target','uncuttable']){
    assert.equal(processingFinished(state),true);
    assert.equal(slicePage([row(state)],{status:'unsliced',source:'all',search:'',page:1,pageSize:20}).total,0);
    assert.ok(processingLabels[state]);
  }
  assert.equal(processingFinished('attention'),false);
});

test('cached credentials are not presented as live platform connectivity',()=>{
  assert.equal(platformStatus(undefined,true).connected,false);
  assert.equal(platformStatus(undefined,true).label,'已登录 · 连接待核实');
  const offline=platformStatus({authenticated:true,reachability:'offline'});
  assert.equal(offline.authenticated,true);assert.equal(offline.connected,false);
  assert.match(offline.label,/登录已保留/);
  assert.equal(platformStatus({authenticated:true,reachability:'online'}).connected,true);
  assert.equal(platformStatus({authenticated:false,reachability:'online'}).authenticated,false);
});
