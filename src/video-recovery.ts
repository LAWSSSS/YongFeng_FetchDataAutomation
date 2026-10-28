type RecoverableVersion = {state:string;taskId?:string;resultId?:string;errorCode?:string;
  filePath?:string;sha256?:string;localCleanupState?:string;review?:{code:string};recovery?:{state:string;resultVerified?:boolean}};

export function partialComplete(version:RecoverableVersion) {
  return version.recovery?.state==='partial_complete'&&version.recovery.resultVerified===true;
}

export function videoRecovery(version: RecoverableVersion) {
  const failedMedia = version.state === 'failed' && !!version.taskId && !!version.resultId
    && ['invalid_video','media_incomplete'].includes(version.errorCode || '');
  const missingCapture = version.state === 'attention'
    && ['task_missing','task_identity_changed','result_missing','platform_failed'].includes(version.review?.code || '');
  const checkpointProblem = version.state === 'failed' && !!version.taskId && !!version.resultId
    && ['range_invalid','object_changed','invalid_length'].includes(version.errorCode || '');
  const canRestartDownload = failedMedia || checkpointProblem;
  return {failedMedia, checkpointProblem, canRestartDownload, canRecapture:failedMedia || checkpointProblem || missingCapture};
}

export function versionDownloadable(version: RecoverableVersion) {
  if (version.filePath && version.sha256 && version.localCleanupState !== 'complete') return true;
  return !!version.taskId && !!version.resultId && ['ready','saved','failed','publishing','attention'].includes(version.state)
    && !['task_missing','result_missing','result_identity_changed','multiple_results','task_identity_changed','submission_ambiguous'].includes(version.review?.code || '');
}
