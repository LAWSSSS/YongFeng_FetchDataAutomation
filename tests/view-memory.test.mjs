import test from 'node:test';
import assert from 'node:assert/strict';
import {createViewMemory} from '../src/view-memory.ts';

test('view preferences preserve explicit collapse and isolate sections and queries',()=>{
  const memory=createViewMemory();
  memory.save('date:slicing:2026-11-02',true);
  memory.save('page:all',3);memory.save('page:attention',1);
  assert.equal(memory.read('date:collection:2026-11-02',false),false);
  assert.equal(memory.read('date:slicing:2026-11-02',false),true);
  memory.save('date:slicing:2026-11-02',false);
  assert.equal(memory.read('date:slicing:2026-11-02',true),false);
  assert.equal(memory.read('page:all',1),3);
  assert.equal(memory.read('page:attention',1),1);
});

test('view memory stays bounded and saving an existing key makes it recent',()=>{
  const memory=createViewMemory(2);
  memory.save('first',1);memory.save('second',2);memory.save('first',3);memory.save('third',4);
  assert.equal(memory.read('second',null),null);
  assert.equal(memory.read('first',null),3);
  assert.equal(memory.read('third',null),4);
});
