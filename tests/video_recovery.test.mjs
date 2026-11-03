import test from 'node:test';
import assert from 'node:assert/strict';
import { recoveryInProgress, videoRecovery, versionDownloadable } from '../src/video-recovery.ts';
import {videoCategory} from '../src/record-groups.ts';

test('only the current bound recovery is processing, while pause and conflicts remain actionable',()=>{
  const row={id:'one',jobId:'job',activeRevision:2,revision:3,versions:[{revision:2,state:'failed'}],
    operation:{kind:'recover-decodable',state:'running',version:2,expectedRevision:3}};
  assert.equal(recoveryInProgress(row),true);
  assert.equal(videoCategory(row),'processing');
  for(const change of [{state:'paused'},{version:1},{expectedRevision:2},{state:'attention'}]){
    const changed={...row,operation:{...row.operation,...change}};
    assert.equal(recoveryInProgress(changed),false);
    assert.equal(videoCategory(changed),'review');
  }
});

test('verified partial media finishes without pretending the original is complete',()=>{
  const row={id:'one',jobId:'job',activeRevision:0,revision:0,versions:[{revision:0,state:'failed',recovery:{state:'partial_complete',resultVerified:true}}]};
  assert.equal(videoCategory(row),'saved');
  row.versions[0].recovery.resultVerified=false;assert.equal(videoCategory(row),'review');
});

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
