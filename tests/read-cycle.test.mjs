import test from 'node:test';
import assert from 'node:assert/strict';
import {createReadCycle,localJson} from '../src/local-api.ts';
import {historyRefreshMs} from '../src/history-polling.ts';
import {createPoller} from '../src/usePolling.ts';

test('same-view refreshes share one request and navigating aborts the superseded view',async()=>{
  const reads=createReadCycle();let release,signal,calls=0;const shown=[];
  const first=reads.run('videos',async context=>{calls++;signal=context.signal;await new Promise(resolve=>{release=resolve;});if(context.current())shown.push('old');});
  const second=reads.run('videos',async()=>{throw new Error('duplicate request');});
  assert.equal(first,second);await new Promise(resolve=>setImmediate(resolve));
  await reads.run('settings',async context=>{if(context.current())shown.push('new');});
  assert.equal(signal.aborted,true);release();await first;
  assert.equal(calls,1);assert.deepEqual(shown,['new']);
});

test('operation ownership discards late reads even when a transport ignores cancellation',async()=>{
  const reads=createReadCycle();let reply;let plan;
  const old=reads.run('workspace',async context=>{await new Promise(resolve=>{reply=resolve;});if(context.current())plan='old';});
  await new Promise(resolve=>setImmediate(resolve));
  reads.cancel();plan='confirmed-preview';reply();await old;
  assert.equal(plan,'confirmed-preview');
});

test('page cancellation cancels only reads and preserves submitted operation identity',async()=>{
  const abort=new AbortController();let getSignal,postSignal;
  const fetcher=async(_path,init)=>{if(init.method==='POST')postSignal=init.signal;else getSignal=init.signal;
    return new Promise((resolve,reject)=>{if(init.method==='POST')resolve(Response.json({requestId:JSON.parse(init.body).requestId}));
      else init.signal.addEventListener('abort',()=>reject(new DOMException('cancelled','AbortError')));});};
  const read=localJson('/api/state',undefined,{signal:abort.signal,fetcher});
  const write=localJson('/api/operation',{requestId:'stable'},{signal:abort.signal,fetcher});
  abort.abort();await assert.rejects(read,{name:'AbortError'});
  assert.equal(getSignal.aborted,true);assert.equal(postSignal.aborted,false);assert.deepEqual(await write,{requestId:'stable'});
});

test('collapsed and completed submission history does not schedule polling',()=>{
  assert.equal(historyRefreshMs(false,['running']),null);
  assert.equal(historyRefreshMs(true,['complete','attention','failed']),null);
  assert.equal(historyRefreshMs(true,['complete','running']),4000);
});

test('stopping or hiding a polling surface aborts its read without reporting an error',async()=>{
  let signal;const failures=[];
  const poller=createPoller(async value=>{signal=value;await new Promise((_,reject)=>value.addEventListener('abort',()=>reject(new DOMException('cancelled','AbortError'))));},
    {delay:()=>8000,onError:error=>failures.push(error)});
  const pending=poller.refresh();poller.stop();await pending;
  assert.equal(signal.aborted,true);assert.deepEqual(failures,[]);
});
