import test from 'node:test';
import assert from 'node:assert/strict';
import {refreshSections,stateSection} from '../src/refresh-sections.ts';

test('a slow progress panel cannot hold up usable video records',async()=>{
  let release;const updates=[],errors=[];
  const slow=new Promise(resolve=>{release=resolve;});
  const refresh=refreshSections([
    stateSection('视频记录',async()=>['video'],value=>updates.push(value)),
    stateSection('进度',()=>slow,()=>updates.push('progress')),
  ],()=>true,value=>errors.push(value));
  await new Promise(resolve=>setImmediate(resolve));
  assert.deepEqual(updates,[['video']]);
  release();await refresh;assert.equal(updates.length,2);assert.deepEqual(errors.at(-1),[]);
});

test('navigation discards late success and failure without losing the new page',async()=>{
  let current=true,release;const updates=[];
  const slow=new Promise((_,reject)=>{release=reject;});
  const refresh=refreshSections([stateSection('旧栏目',()=>slow,value=>updates.push(value))],()=>current,value=>updates.push(value));
  current=false;release(new Error('late error'));await refresh;assert.deepEqual(updates,[]);
});

test('a panel failure reports its own name while other panels remain usable',async()=>{
  let records,errors;
  await refreshSections([
    stateSection('视频记录',async()=>['kept'],value=>{records=value;}),
    stateSection('平台连接',async()=>{throw new Error('暂时离线');},()=>{}),
  ],()=>true,value=>{errors=value;});
  assert.deepEqual(records,['kept']);assert.deepEqual(errors,['平台连接：暂时离线']);
});
