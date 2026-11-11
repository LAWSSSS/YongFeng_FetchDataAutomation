import test from 'node:test';
import assert from 'node:assert/strict';
import { batchDate, canDownload, captureStatusLabel, completedPartialVideo, groupVideos, groupVideoDays, mergeOpenVideoRows, splitInspections, syncVideoSelection, videoCategory, videoStatusOptions, selectVideoScope, visibleVideoSelections } from '../src/record-groups.ts';

const video = (id, jobId, state, extra = {}) => ({id, jobId, activeRevision: 0, revision: 0,
  versions: [{revision: 0, state}], capabilities:{version:0,expectedRevision:0,canConfirmFinal:state==='ready'}, ...extra});

test('an open confirmation stays in its slot after leaving the ready filter',()=>{
  const first=video('first','j','ready'), current=video('current','j','downloading'), last=video('last','j','ready');
  const order=['first','current','last'];
  const remaining=[first,last];
  assert.deepEqual(mergeOpenVideoRows(remaining,[current],order),[first,current,last]);
  assert.deepEqual(mergeOpenVideoRows(remaining,[first,current],order),[first,current,last]);
  assert.strictEqual(mergeOpenVideoRows(remaining,[],order),remaining);
});

test('withdrawal never appears as a waiting platform download', () => {
  const row = video('withdrawing', 'j', 'withdrawing', {operation:{kind:'withdraw',state:'reconciling'}});
  assert.equal(captureStatusLabel(row.versions[0]), '撤回处理中');
  assert.equal(videoCategory(row), 'deleting');
  assert.equal(canDownload(row), false);
  row.operation.state = 'attention';
  assert.equal(videoCategory(row), 'review');
});

test('terminating a capture belongs to cleanup, without a download action', () => {
  const row = video('stopping', 'j', 'terminating');
  assert.equal(videoCategory(row), 'deleting');
  assert.equal(canDownload(row), false);
});

test('independent same-day submissions share one grid; cross-midnight items keep their real day',()=>{
  const records=[video('a','one','ready',{submittedAt:'2026-11-03T02:00:00Z'}),
    video('b','two','ready'),video('c','three','ready'),
    video('d','one','ready',{submittedAt:'2026-11-03T16:00:00Z'}),video('unknown','missing','ready')];
  const jobs=['one','two','three'].map(id=>({id,createdAt:'2026-11-03T03:00:00Z',status:'complete',items:[]}));
  const days=groupVideoDays(records,jobs);
  assert.deepEqual(days.map(day=>[day.day,day.records.map(row=>row.id),day.submissionCount]),[
    ['2026-11-04',['d'],1],['2026-11-03',['a','b','c'],3],['',['unknown'],1]]);
  assert.strictEqual(days[1].records[0],records[0]);
});

test('pending submission and a bound platform queue have distinct labels', () => {
  assert.equal(captureStatusLabel({state:'pending'}), '尚未提交采集');
  assert.equal(captureStatusLabel({state:'waiting', taskId:'bound', platformStatus:'PENDING'}), '平台排队中');
  assert.equal(captureStatusLabel({state:'waiting', taskId:'bound', platformStatus:'FIXED'}), '等待平台结果');
  assert.equal(captureStatusLabel({state:'ready', taskId:'bound', platformStatus:'FIXED'}), '待确认');
});

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
  assert.equal(next.has('hidden'),false);
  const visible=syncVideoSelection(selection,[{id:'keep',revision:2,activeRevision:1}]);
  assert.deepEqual([...visible.keys()],['keep']);
});

test('one selection control keeps the requested scope and excludes deleting records',()=>{
  const rows=[video('ready','j','ready'),video('review','j','failed'),video('done','j','failed',{
    versions:[{revision:0,state:'failed',recovery:{state:'partial_complete',resultVerified:true}}]}),
    video('deleting','j','ready',{deletion:{state:'pending'},capabilities:{version:0,expectedRevision:0,canConfirmFinal:false}})];
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

test('final confirmation only uses a current backend capability, never a ready or waiting guess', () => {
  const records = [video('current','j','saved',{activeRevision:2,revision:3,versions:[
    {revision:0,state:'saved'}, {revision:2,state:'ready'}],capabilities:{version:2,expectedRevision:3,canConfirmFinal:true}}),
    video('saved','j','saved'), video('review','j','attention'), video('unbound','j','pending'),
    video('uncertain','j','uncertain'), video('failed','j','failed'), video('waiting','j','waiting'),
    video('submitted','j','submitted'),video('deleting','j','ready',{deletion:{state:'pending'},capabilities:{version:0,expectedRevision:0,canConfirmFinal:false}}),
    video('queued','j','ready',{capabilities:{version:0,expectedRevision:0,canConfirmFinal:false},versions:[{revision:0,state:'ready',downloadRequested:true}]})];
  assert.deepEqual(records.filter(canDownload).map(r => r.id), ['current']);
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


test('mutation selections intersect visible current rows and authoritative confirmation capabilities',()=>{
  const current=video('ready','j','ready');
  const denied=video('denied','j','ready',{capabilities:{version:0,expectedRevision:0,canConfirmFinal:false}});
  const choices=['ready','denied','hidden'].map(id=>({id,version:0,expectedRevision:0}));
  assert.deepEqual(visibleVideoSelections(choices,[current,denied]).map(row=>row.id),['ready','denied']);
  assert.deepEqual(visibleVideoSelections(choices,[current,denied],true).map(row=>row.id),['ready']);
  assert.deepEqual(visibleVideoSelections(choices,[{...current,revision:1}],true),[]);
  assert.equal(canDownload({...current,capabilities:undefined}),false);
  assert.equal(canDownload({...current,capabilities:{...current.capabilities,version:1}}),false);
});
