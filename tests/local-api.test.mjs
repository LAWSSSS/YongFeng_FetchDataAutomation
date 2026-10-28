import test from 'node:test';
import assert from 'node:assert/strict';
import {localJson} from '../src/local-api.ts';
import {requestIntents} from '../src/request-intent.ts';

test('lost execute reply reuses its operation identity; a later new operation gets another',()=>{
  const intent=requestIntents(),external=new Map();
  const accept=body=>{external.set(body.requestId,(external.get(body.requestId)||0)+1);};
  const first=intent.payload('execute',{expectedRevision:2});accept(first);
  const retry=intent.payload('execute',{expectedRevision:2});accept(retry);
  assert.equal(first.requestId,retry.requestId);assert.equal(external.size,1);
  intent.complete('execute');assert.notEqual(intent.payload('execute',{expectedRevision:2}).requestId,first.requestId);
});

test('control calls preserve the exact request ID and backend error',async()=>{
  const body={requestId:'stable-id',expectedRevision:3};let sent;
  const fetcher=async(_url,init)=>{sent=init;return new Response(JSON.stringify({message:'版本已变化'}),{status:409});};
  await assert.rejects(localJson('/api/test',body,{fetcher}),/版本已变化/);
  assert.equal(sent.body,JSON.stringify(body));assert.equal(sent.method,'POST');
});

test('a stalled request releases the button with an unknown-outcome message',async()=>{
  const fetcher=(_url,{signal})=>new Promise((_resolve,reject)=>signal.addEventListener('abort',()=>reject(new DOMException('aborted','AbortError'))));
  await assert.rejects(localJson('/api/test',{requestId:'same-on-retry'},{fetcher,timeoutMs:10}),/结果尚未确认/);
  await assert.rejects(localJson('/api/test',undefined,{fetcher,timeoutMs:10}),/后台任务仍在继续/);
});

test('HTML/error responses and disconnects never leave silent loading states',async()=>{
  await assert.rejects(localJson('/api/test',undefined,{fetcher:async()=>new Response('<html>bad proxy</html>')}),/返回异常/);
  await assert.rejects(localJson('/api/test',undefined,{fetcher:async()=>{throw new TypeError('network');}}),/暂时无法连接/);
  assert.deepEqual(await localJson('/api/test',undefined,{fetcher:async()=>Response.json({ok:true})}),{ok:true});
});
