type RecoverableVersion = {state:string;taskId?:string;resultId?:string;errorCode?:string;review?:{code:string}};

export function videoRecovery(version: RecoverableVersion) {
  const failedMedia = version.state === 'failed' && !!version.taskId && !!version.resultId
    && ['invalid_video','media_incomplete'].includes(version.errorCode || '');
  const missingCapture = version.state === 'attention'
    && ['task_missing','task_identity_changed','result_missing','platform_failed'].includes(version.review?.code || '');
  return {failedMedia, canRecapture:failedMedia || missingCapture};
}
