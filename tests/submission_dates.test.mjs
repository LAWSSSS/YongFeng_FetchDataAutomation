import test from 'node:test';
import assert from 'node:assert/strict';
import { groupSubmissionDays,submissionDay,submissionTime } from '../src/submission-dates.ts';
import { collectionAction,initialCapture } from '../src/collection-actions.ts';
import { groupVideos } from '../src/record-groups.ts';
import { physicalFiles } from '../src/slicing-view.ts';

test('submission days use Beijing midnight and explicit unknowns, including naive server times',()=>{
  assert.equal(submissionDay('2026-10-25T16:00:00Z'),'2026-10-26');
  assert.equal(submissionDay('2026-10-25T15:59:59Z'),'2026-10-25');
  assert.equal(submissionDay('2026-10-26 00:00:00'),'2026-10-26');
  assert.equal(submissionTime('2026-10-25T16:00:00Z'),'00:00:00');
  assert.equal(submissionDay('bad date'),'');
  assert.equal(submissionDay(), '');
});

test('same-day submissions share a date but retain exact batch identities and unknown records',()=>{
  const jobs=[{id:'a',createdAt:'2026-10-26T00:00:00Z'},{id:'b',createdAt:'2026-10-25T23:59:59Z'}];
  const video=(id,jobId)=>({id,jobId,versions:[],revision:0,activeRevision:0});
  const groups=groupVideos([video('a1','a'),video('b1','b'),video('old','missing')],jobs);
  const dates=groupSubmissionDays(groups,g=>g.job?.createdAt);
  assert.deepEqual(dates.map(row=>row.day),['2026-10-26','']);
  assert.deepEqual(dates[0].records.map(row=>row.id),['a','b']);
  assert.equal(dates[1].records[0].records[0].id,'old');
  const file=physicalFiles([{id:'bed',path:'/one.mp4',name:'capture_20260101.mp4',submittedAt:'2026-10-26T00:00:00Z'},
    {id:'magnet',path:'/one.mp4',name:'capture_20260101.mp4',submittedAt:'2026-10-26T01:00:00Z'}]);
  assert.equal(groupSubmissionDays(file,g=>g.records[0].submittedAt)[0].records.length,1);
});

test('one-click collect retains its prepared intent after lost response, with an independent next selection',async()=>{
  const calls=[];let attempts=0;
  const collect=collectionAction(async(path,body)=>{
    calls.push([path,body]);
    if(path==='/preview')return {id:`plan-${calls.length}`,tasks:[{status:'ready'}]};
    if(++attempts===1)throw new Error('response lost');
    return {id:'job'};
  });
  await assert.rejects(collect('search',['a','b'],2,false),/response lost/);
  assert.deepEqual(await collect('search',['b','a'],2,false),{id:'job'});
  assert.equal(calls.filter(([path])=>path==='/preview').length,1);
  assert.deepEqual(calls[1][1],calls[2][1]);
  await collect('search',['c'],2,false);
  assert.equal(calls.filter(([path])=>path==='/preview').length,2);
});

test('direct collect prevents duplicate clicks and refuses invalid or duplicate-only plans',async()=>{
  for(const status of ['invalid','duplicate']){
    const paths=[];const collect=collectionAction(async path=>{paths.push(path);return {id:'p',tasks:[{status}]};});
    await assert.rejects(collect('s',['a'],0,false));assert.deepEqual(paths,['/preview']);
  }
  let release;const collect=collectionAction(async path=>path==='/preview'?await new Promise(resolve=>{release=resolve;}):{id:'job'});
  const first=collect('s',['a'],0,false);
  await assert.rejects(collect('s',['a'],0,false),/正在提交/);
  release({id:'plan',tasks:[{status:'ready'}]});await first;
  assert.deepEqual(initialCapture('2026-10-26 23:58:12'),{start:'2026-10-27 00:03:12',end:'2026-10-27 01:03:12'});
});
