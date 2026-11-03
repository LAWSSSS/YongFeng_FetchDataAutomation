import test from 'node:test';
import assert from 'node:assert/strict';
import {announceMediaChange,mediaRevision,onMediaChange} from '../src/media-updates.ts';
import {localJson} from '../src/local-api.ts';

test('accepted media commands invalidate once without manufacturing progress',async()=>{
  const target=new EventTarget(),events=[];
  const stop=onMediaChange(target,()=>events.push(mediaRevision()));
  const before=mediaRevision();
  announceMediaChange('/api/videos/video/withdraw',target);
  announceMediaChange('/api/slicing/packages/pause',target);
  announceMediaChange('/api/unrelated',target);
  assert.deepEqual(events,[before+1,before+2]);
  stop();announceMediaChange('/api/pipeline/pause',target);
  assert.equal(events.length,2);
});

test('reads and rejected requests cannot invalidate an accepted version',async()=>{
  const before=mediaRevision();
  await localJson('/api/videos',undefined,{fetcher:async()=>Response.json({videos:[]})});
  await assert.rejects(localJson('/api/videos/v/withdraw',{requestId:'conflict'},
    {fetcher:async()=>Response.json({message:'version conflict'},{status:409})}),/version conflict/);
  assert.equal(mediaRevision(),before);
  const value=await localJson('/api/videos/v/confirm-and-process',{requestId:'accepted'},
    {fetcher:async()=>Response.json({operationId:'operation',state:'queued'},{status:202})});
  assert.equal(mediaRevision(),before+1);assert.equal(value.state,'queued');
});
