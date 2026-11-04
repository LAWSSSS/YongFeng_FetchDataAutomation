import test from 'node:test';
import assert from 'node:assert/strict';
import {reconcileRecordChoices,toggleRecord,reconcileSubmissionChoices,selectSubmissionReceipts,toggleSubmissionChoice} from '../src/record-choices.ts';

const row=(id,revision=1)=>({key:`item:${id}`,id,revision,kind:'item'});
test('selection drops records outside the current filter and stale revisions',()=>{
  const previous=new Map([row('a'),row('b'),row('c')].map(r=>[r.key,r]));
  const next=reconcileRecordChoices(previous,[row('a'),row('b',2)]);
  assert.deepEqual([...next.keys()],['item:a']);
  assert.equal(previous.size,3);
});
test('refreshing one page preserves selections from other explicitly selected pages',()=>{
  const previous=new Map([row('a'),row('b')].map(r=>[r.key,r]));
  const next=reconcileRecordChoices(previous,[row('a',2)],new Set(['item:a']));
  assert.deepEqual([...next.keys()],['item:b']);
  assert.strictEqual(reconcileRecordChoices(next,[row('b')]),next);
});
test('checkbox toggling never mutates the current snapshot',()=>{
  const old=new Map();const chosen=toggleRecord(old,row('a'));
  assert.equal(old.size,0);assert.equal(chosen.size,1);
  assert.equal(toggleRecord(chosen,row('a')).size,0);assert.equal(chosen.size,1);
});

const receipt=(id,revision=1)=>({key:`receipt:${id}`,id,revision,kind:'receipt'});
const detail=(parent,id,revision=1,parentRevision=1)=>({...row(`${parent}:${id}`,revision),parentId:parent,parentRevision});
test('selecting a receipt removes child selections instead of deleting the same scope twice',()=>{
  const child=detail('a','1'),other=detail('b','1');
  const previous=new Map([child,other].map(r=>[r.key,r]));
  const next=toggleSubmissionChoice(previous,receipt('a'));
  assert.deepEqual([...next.keys()],['item:b:1','receipt:a']);
  assert.strictEqual(toggleSubmissionChoice(next,child),next);
  assert.equal(previous.size,2);
  const day=selectSubmissionReceipts(next,[receipt('a'),receipt('b')]);
  assert.deepEqual([...day.keys()],['receipt:a','receipt:b']);
});
test('updated or hidden parent invalidates selected details even while history is collapsed',()=>{
  const choices=[detail('a','1'),detail('b','1'),detail('c','1'),receipt('d')];
  const previous=new Map(choices.map(r=>[r.key,r]));
  const next=reconcileSubmissionChoices(previous,[{id:'a',revision:2},{id:'b',revision:1},{id:'d',revision:2}]);
  assert.deepEqual([...next.keys()],['item:b:1']);
  assert.strictEqual(reconcileSubmissionChoices(next,[{id:'b',revision:1}]),next);
});
