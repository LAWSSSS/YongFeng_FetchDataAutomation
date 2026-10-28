import test from 'node:test';
import assert from 'node:assert/strict';
import { batchDate, canDownload, completedPartialVideo, groupVideos, splitInspections, syncVideoSelection, videoCategory, videoStatusOptions, selectVideoScope } from '../src/record-groups.ts';

const video = (id, jobId, state, extra = {}) => ({id, jobId, activeRevision: 0, revision: 0,
  versions: [{revision: 0, state}], ...extra});

test('bulk selection never silently adopts a newer revision or version', () => {
  const selection = new Map([['keep', {id:'keep', version:1, expectedRevision:2}],
    ['stale', {id:'stale', version:1, expectedRevision:2}],
    ['hidden', {id:'hidden', version:0, expectedRevision:1}]]);
  const next = syncVideoSelection(selection, [
    {id:'keep', revision:5, activeRevision:1},
    {id:'stale', revision:4, activeRevision:2},
  ]);
  assert.equal(next.has('keep'),false);
  assert.equal(next.has('stale'), false);
  assert.equal(next.get('hidden').expectedRevision, 1);
  assert.equal(syncVideoSelection(selection,[{id:'keep',revision:2,activeRevision:1}]),selection);
});

test('one selection control keeps the requested scope and excludes deleting records',()=>{
  const rows=[video('ready','j','ready'),video('review','j','failed'),video('done','j','failed',{
    versions:[{revision:0,state:'failed',recovery:{state:'partial_complete',resultVerified:true}}]}),
    video('deleting','j','ready',{deletion:{state:'pending'}})];
  assert.deepEqual(selectVideoScope(rows,'all').map(v=>v.id),['ready','review','done']);
  assert.deepEqual(selectVideoScope(rows,'downloadable').map(v=>v.id),['ready']);
  assert.deepEqual(selectVideoScope(rows,'partial').map(v=>v.id),['done']);
  assert.deepEqual(selectVideoScope(rows,'invalid'),[]);
});

test('approved and unapproved results are separate; rejection reasons and stable IDs remain', () => {
  const rows = [{id:'valid-plate',issue:''}, {id:'no-photo',issue:'无真实车牌且没有有效照片'},
    {id:'valid-virtual'}, {id:'short',issue:'质检时长未严格超过10分钟'}];
  const groups = splitInspections(rows);
  assert.deepEqual(groups.approved.map(r => r.id), ['valid-plate', 'valid-virtual']);
  assert.deepEqual(groups.unapproved, [rows[1], rows[3]]);
});

test('one group per immutable job ID; filtering never merges batches with identical dates', () => {
  const rows = [video('a','first','saved'),video('b','second','ready'),video('c','first','waiting'),
    video('d','old-missing-job','attention')];
  const jobs = [{id:'second'},{id:'first'}];
  const before = JSON.stringify(rows);
  const groups = groupVideos(rows,jobs);
  assert.deepEqual(groups.map(g => g.id), ['second','first','old-missing-job']);
  assert.deepEqual(groups[1].records.map(r => r.id), ['a','c']);
  assert.deepEqual(groups[1].counts, {saved:1,waiting:1});
  assert.equal(groupVideos(rows.filter(r => r.id === 'c'),jobs)[0].id, 'first');
  assert.equal(before, JSON.stringify(rows));
});

test('batch download uses the current revision and excludes review, saved, queued and deleting records', () => {
  const records = [video('current','j','saved',{activeRevision:2,revision:3,versions:[
    {revision:0,state:'saved'}, {revision:2,state:'ready'}]}),
    video('saved','j','saved'), video('review','j','attention'), video('unbound','j','pending'),
    video('uncertain','j','uncertain'), video('failed','j','failed'), video('waiting','j','waiting'),
    video('submitted','j','submitted'),video('deleting','j','ready',{deletion:{state:'pending'}}),
    video('queued','j','ready',{versions:[{revision:0,state:'ready',downloadRequested:true}]})];
  assert.deepEqual(records.filter(canDownload).map(r => r.id), ['current','waiting','submitted']);
  const counts = groupVideos(records,[])[0].counts;
  assert.equal(counts.review,3);
  assert.equal(counts.download,1);
});

test('batch dates display in Beijing time and missing dates have an explicit fallback', () => {
  assert.equal(batchDate('2026-10-14T18:30:00Z'), '2026/10/15 02:30:00');
  assert.equal(batchDate(), '创建时间未记录');
});

test('verified partial results belong to saved and batch selection never includes pending or deleting media',()=>{
  const row=video('partial','j','failed',{versions:[{revision:0,state:'failed',recovery:{state:'partial_complete',resultVerified:true}}]});
  assert.equal(videoCategory(row),'saved');
  assert.equal(completedPartialVideo(row),true);
  assert.equal(canDownload(row),false);
  assert.equal(completedPartialVideo({...row,deletion:{state:'pending'}}),false);
  assert.equal(completedPartialVideo({...row,versions:[{revision:0,state:'failed',recovery:{state:'slicing',resultVerified:true}}]}),false);
  assert.deepEqual(groupVideos([row,video('normal','j','saved')],[])[0].counts,{saved:2});
  assert.ok(videoStatusOptions.some(o=>o.value==='saved'));
  assert.ok(!videoStatusOptions.some(o=>o.value==='complete'));
});
