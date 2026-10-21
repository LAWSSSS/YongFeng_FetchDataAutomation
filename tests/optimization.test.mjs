import test from 'node:test';
import assert from 'node:assert/strict';
import {createPoller} from '../src/usePolling.ts';
import {physicalCategories,slicePage,sliceTitle,slicingControls} from '../src/slicing-view.ts';

test('slicing controls allow another video after a failed single run without a dead resume button',()=>{
  const state={enabled:false,busy:false,run:{state:'attention',singleMode:true,singleItemId:'failed'},
    items:[{id:'failed',state:'attention'},{id:'other',state:'pending'}]};
  assert.equal(slicingControls(state).canStart,true);
  assert.equal(slicingControls(state).canResume,false);
  assert.equal(slicingControls({...state,busy:true}).canStart,false);
  assert.equal(slicingControls({...state,run:{...state.run,state:'scanning'}}).canStart,false);
  assert.equal(slicingControls({...state,items:[{id:'failed',state:'pending'}]}).canResume,true);
});

test('video cards show plate and date without losing VIRTUAL identity or restored version',()=>{
  assert.deepEqual(sliceTitle('VIRTUAL_016_20261005050441_38bcd1239ac8_恢复.mp4'),{
    title:'VIRTUAL_016',when:'2026-10-05 05:04:41',restored:true});
  assert.equal(sliceTitle('鲁Q550GY_20260930122000_bab10f7a5061.mp4').title,'鲁Q550GY');
  assert.equal(sliceTitle('my-source.mp4').title,'my-source');
});

function timers() {
  const scheduled = new Map();let next=0;
  return {scheduled,setTimer(callback,delay){const id=++next;scheduled.set(id,{callback,delay});return id;},
    clearTimer(id){scheduled.delete(id);}};
}

test('slow polling never overlaps and stops after the page is disposed',async()=>{
  const timer=timers();let calls=0,release;
  const poller=createPoller(async()=>{calls++;await new Promise(resolve=>{release=resolve;});},{delay:()=>4000,...timer});
  const first=poller.refresh();await poller.refresh();
  assert.equal(calls,1);assert.equal(timer.scheduled.size,0);
  release();await first;
  assert.equal([...timer.scheduled.values()][0].delay,0);
  poller.stop();assert.equal(timer.scheduled.size,0);await poller.refresh();assert.equal(calls,1);
});

test('hidden pages do not poll and returning to the page resumes immediately',async()=>{
  const timer=timers();let visible=false,calls=0;
  const poller=createPoller(async()=>{calls++;},{delay:()=>15000,visible:()=>visible,...timer});
  await poller.refresh();assert.equal(calls,0);
  visible=true;await poller.refresh();assert.equal(calls,1);
  const task=[...timer.scheduled.values()][0];visible=false;await task.callback();
  assert.equal(calls,1);assert.equal(timer.scheduled.size,0);
  visible=true;await poller.refresh();assert.equal(calls,2);poller.stop();
});

test('unsliced videos stay listed until the cut is finished',()=>{
  const rows=[{name:'open',sourceVideoId:'1',sourceDatasetId:'27912',state:'platform_processing',categories:{bed:{state:'waiting'}}},
    {name:'done',sourceVideoId:'2',sourceDatasetId:'27912',state:'complete',categories:{bed:{state:'complete'}}},
    {name:'empty',sourceVideoId:'3',sourceDatasetId:'27944',state:'no_target',categories:{magnet:{state:'no_target'}}}];
  const view=slicePage(rows,{status:'unsliced',source:'all',search:'',page:1,pageSize:12});
  assert.deepEqual(view.rows.map(row=>row.sourceVideoId),['1']);
});

test('slicing filters compose before pagination and shrinking results clamp the page',()=>{
  const rows=Array.from({length:117},(_,index)=>({name:'视频 '+index,sourceVideoId:String(index),
    sourceDatasetId:index%2 ? '27912':'27944',state:index<100 ? 'pending':'complete',categories:{[index%2 ? 'bed':'magnet']:{state:index<100 ? 'pending':'complete'}}}));
  const options={status:'all',source:'all',search:'',page:2,pageSize:12};
  assert.equal(slicePage(rows,options).rows[0].sourceVideoId,'12');
  assert.equal(slicePage(rows,options).pages,10);
  const view=slicePage(rows,{...options,source:'27912',status:'complete',search:'视频 11',page:999});
  assert.equal(view.page,1);assert.deepEqual(view.rows.map(item=>item.sourceVideoId),['111','113','115']);
  assert.equal(slicePage(rows,{...options,search:'not present'}).page,1);
});

test('physical cards merge two categories by bound file, never merge unbound names', async()=>{
  const {physicalFiles}=await import('../src/slicing-view.ts');
  const record=(id,path,sourceDatasetId)=>({id,path,sourceDatasetId,name:'same.mp4',sourceVideoId:id,state:'pending',categories:{}});
  const cards=physicalFiles([record('a','/video/a.mp4','27912'),record('b','/video/a.mp4','27944'),record('c','','27912'),record('d','','27944')]);
  assert.equal(cards.length,3);
  assert.deepEqual(cards[0].records.map(record=>record.id),['a','b']);
});

test('one completed category does not label the whole file complete',async()=>{
  const {physicalState}=await import('../src/slicing-view.ts');
  assert.equal(physicalState([{state:'complete'},{state:'platform_processing'}]),'platform_processing');
  assert.equal(physicalState([{state:'complete'},{state:'attention'}]),'attention');
  assert.equal(physicalState([{state:'complete'},{state:'no_target'}]),'complete');
  assert.equal(physicalState([{state:'no_target'},{state:'no_target'}]),'no_target');
});

test('both models on one source are visible and filters follow category progress',()=>{
  const item={id:'one',name:'video',sourceDatasetId:'27912',sourceVideoId:'42',state:'attention',
    categories:{bed:{state:'complete',packageId:'bed-package'},magnet:{state:'attention',packageId:'magnet-package'}}};
  assert.deepEqual(physicalCategories([item]).map(row=>row.categoryName),['bed','magnet']);
  const options={status:'complete',source:'27912',search:'',page:1,pageSize:12};
  assert.equal(slicePage([item],options).total,1);
  assert.equal(slicePage([item],{...options,source:'27944'}).total,0);
  assert.equal(slicePage([item],{...options,source:'27944',status:'attention'}).total,1);
  assert.equal(physicalCategories([item],'27944')[0].categoryName,'magnet');
});

test('physical category selection prefers existing receipts over a pending duplicate',()=>{
  const base={name:'video',sourceDatasetId:'27912',sourceVideoId:'42',state:'pending'};
  const pending={...base,id:'a',categories:{bed:{state:'pending'},magnet:{state:'not_applicable'}}};
  const complete={...base,id:'b',categories:{bed:{state:'complete',packageId:'existing'},magnet:{state:'not_applicable'}}};
  assert.deepEqual(physicalCategories([pending,complete]).map(row=>row.item.id),['b']);
});
