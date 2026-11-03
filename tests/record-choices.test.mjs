import test from 'node:test';
import assert from 'node:assert/strict';
import {reconcileRecordChoices,toggleRecord} from '../src/record-choices.ts';

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
