import test from 'node:test';
import assert from 'node:assert/strict';
import { videoRecovery } from '../src/video-recovery.ts';

test('a bound incomplete video offers recapture without approving the damaged file',()=>{
  assert.deepEqual(videoRecovery({state:'failed',taskId:'task',resultId:'result',errorCode:'invalid_video'}),
    {failedMedia:true,canRecapture:true});
  assert.equal(videoRecovery({state:'failed',taskId:'task',resultId:'result',errorCode:'media_incomplete'}).canRecapture,true);
});

test('uncertain identity and unrelated disk errors cannot create replacement captures',()=>{
  for (const value of [
    {state:'uncertain',errorCode:'invalid_video'}, {state:'failed',errorCode:'invalid_video'},
    {state:'failed',taskId:'task',resultId:'result',errorCode:'disk_full'},
    {state:'attention',review:{code:'multiple_results'}}, {state:'attention',review:{code:'submission_ambiguous'}},
  ]) assert.equal(videoRecovery(value).canRecapture,false);
  assert.equal(videoRecovery({state:'attention',review:{code:'result_missing'}}).canRecapture,true);
});
