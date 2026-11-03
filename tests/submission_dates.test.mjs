import test from 'node:test';
import assert from 'node:assert/strict';
import { groupSubmissionDays,submissionDay,submissionTime } from '../src/submission-dates.ts';
import { groupVideos } from '../src/record-groups.ts';
import { physicalFiles } from '../src/slicing-view.ts';

test('stable identity groups moved aliases but keeps correction generations separate',()=>{
  const item=(id,path,generation)=>({id,path,name:'same.mp4',mediaId:'collection:one',mediaGeneration:generation,collectionRevision:generation,sourceDatasetId:'27912',sourceVideoId:id,state:'pending',categories:{}});
  assert.equal(physicalFiles([item('a','/old.mp4',0),item('b','/moved.mp4',0)]).length,1);
  assert.equal(physicalFiles([item('a','/old.mp4',0),item('new','/old.mp4',1)]).length,2);
});

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
