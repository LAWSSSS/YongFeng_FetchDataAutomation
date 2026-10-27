import test from 'node:test';
import assert from 'node:assert/strict';
import { videoRecovery, versionDownloadable } from '../src/video-recovery.ts';

test('a bound incomplete video offers recapture without approving the damaged file',()=>{
  assert.deepEqual(videoRecovery({state:'failed',taskId:'task',resultId:'result',errorCode:'invalid_video'}),
    {failedMedia:true,checkpointProblem:false,canRestartDownload:true,canRecapture:true});
  assert.equal(videoRecovery({state:'failed',taskId:'task',resultId:'result',errorCode:'media_incomplete'}).canRecapture,true);
});

test('checkpoint errors keep explicit restart and recapture actions after a retry changes the reason',()=>{
  for (const errorCode of ['range_invalid','object_changed','invalid_length']) {
    const result=videoRecovery({state:'failed',taskId:'task',resultId:'result',errorCode});
    assert.equal(result.checkpointProblem,true);
    assert.equal(result.canRestartDownload,true);
    assert.equal(result.canRecapture,true);
    assert.equal(result.failedMedia,false);
  }
});

test('only available historical results or verified local files can be downloaded',()=>{
  assert.equal(versionDownloadable({state:'ready',taskId:'old-task',resultId:'old-result'}),true);
  assert.equal(versionDownloadable({state:'failed',taskId:'old-task',resultId:'old-result',errorCode:'range_invalid'}),true);
  for (const value of [{state:'cloud_deleted',taskId:'task',resultId:'result'},
    {state:'uncertain',taskId:'task',resultId:'result'},
    {state:'attention',taskId:'task',resultId:'result',review:{code:'result_identity_changed'}}]) {
    assert.equal(versionDownloadable(value),false);
  }
  assert.equal(versionDownloadable({state:'saved',filePath:'/managed/video.mp4',sha256:'verified-hash'}),true);
});

test('uncertain identity and unrelated disk errors cannot create replacement captures',()=>{
  for (const value of [
    {state:'uncertain',errorCode:'invalid_video'}, {state:'failed',errorCode:'invalid_video'},
    {state:'failed',taskId:'task',resultId:'result',errorCode:'disk_full'},
    {state:'attention',review:{code:'multiple_results'}}, {state:'attention',review:{code:'submission_ambiguous'}},
  ]) assert.equal(videoRecovery(value).canRecapture,false);
  assert.equal(videoRecovery({state:'attention',review:{code:'result_missing'}}).canRecapture,true);
});
