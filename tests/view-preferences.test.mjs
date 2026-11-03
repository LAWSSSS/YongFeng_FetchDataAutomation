import test from 'node:test';
import assert from 'node:assert/strict';
import {initialPage,initialVideoTarget,navigationSearch,pipelineReadInterval,slicingReadInterval,transferProgress} from '../src/view-preferences.ts';

test('opening the videos tab is not an empty single-video focus request',()=>{
  assert.equal(initialVideoTarget('?tab=videos'),null);
  assert.equal(initialVideoTarget('?tab=videos&videoId='),null);
  assert.equal(initialVideoTarget('?tab=collect&videoId=one'),null);
  assert.deepEqual(initialVideoTarget('?tab=videos&videoId=one'),{videoId:'one',mediaId:undefined});
});

test('navigation updates the reload destination and clears consumed detail links',()=>{
  const search=navigationSearch('?tab=slicing&videoId=old&mediaId=stale&extra=keep','videos');
  assert.equal(initialPage(search,'slicing'),'videos');
  assert.equal(new URLSearchParams(search).has('videoId'),false);
  assert.equal(new URLSearchParams(search).has('mediaId'),false);
  assert.equal(new URLSearchParams(search).get('extra'),'keep');
});

test('existing page links bypass an unrelated saved page; unsupported links cannot open a new surface',()=>{
  for(const page of ['collect','videos','slicing','settings'])assert.equal(initialPage(`?tab=${page}`,'videos'),page);
  assert.equal(initialPage('?tab=unknown','settings'),'settings');
  assert.equal(initialPage('', 'audit'),'videos');
  assert.equal(initialPage('', null),'collect');
});
test('visible active work refreshes quickly even with continuous mode off, while idle waiting reads slow down',()=>{
  assert.equal(pipelineReadInterval({processing:1}),4000);
  assert.equal(pipelineReadInterval({processing:0}),15000);
  assert.equal(pipelineReadInterval(),15000);
  assert.equal(pipelineReadInterval({processing:0},true),30000);
  assert.equal(pipelineReadInterval({processing:1},true),4000);
  assert.equal(slicingReadInterval({phase:'waiting_connection'}),30000);
  for(const value of [{busy:true},{localWorkers:{active:1}},{packages:{busy:true}},
    {phase:'scanning'},{phase:'analyzing'},{phase:'exporting'},{phase:'archiving'}])assert.equal(slicingReadInterval(value),4000);
  for(const value of [{},{busy:false,phase:'complete'},{phase:'idle'}])assert.equal(slicingReadInterval(value),15000);
});
test('upload progress uses only known byte counts and clamps display to the bound size',()=>{
  assert.equal(transferProgress(512*1024,2*1024**2),'512.0 KB / 2.0 MB');
  assert.equal(transferProgress(2*1024**3,3*1024**3),'2.00 GB / 3.00 GB');
  assert.equal(transferProgress(-1,100),'0 B / 100 B');
  assert.equal(transferProgress(101,100),'100 B / 100 B');
});
