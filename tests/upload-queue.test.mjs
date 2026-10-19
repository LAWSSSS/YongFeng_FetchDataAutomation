import test from 'node:test';
import assert from 'node:assert/strict';
import {effectiveUploadFilter,platformDatasetUrl,queueComplete} from '../src/upload-queue.ts';

test('completed uploads remain visible automatically; an explicit user filter remains selected',()=>{
  assert.equal(effectiveUploadFilter('auto',[]),'active');
  assert.equal(effectiveUploadFilter('auto',[{state:'uploading'},{state:'complete'}]),'active');
  assert.equal(effectiveUploadFilter('auto',[{state:'complete'}]),'all');
  assert.equal(effectiveUploadFilter('attention',[{state:'complete'}]),'attention');
  assert.equal(effectiveUploadFilter('active',[{state:'complete'}]),'active');
});

test('empty, exceptional and platform-waiting queues are not called complete',()=>{
  const target=(states,activeImport='')=>({items:states.map(state=>({state})),activeImport});
  assert.equal(queueComplete([target([]),target([])]),false);
  assert.equal(queueComplete([target(['complete']),target(['attention'])]),false);
  assert.equal(queueComplete([target(['complete']),target(['complete'],'manual-job')]),false);
  assert.equal(queueComplete([target(['complete']),target(['complete'])]),true);
});

test('video links open data contents in the exact dataset and namespace, separately from job logs',()=>{
  for (const id of ['27912','27944']) {
    const href=platformDatasetUrl(id);
    assert.equal(href,`http://pre-athena-mv.cisdigital.cn/#/dataset/${id}?tab=content&namespace=1`);
    assert.match(platformDatasetUrl(id,'base'),/tab=base&namespace=1$/);
  }
  assert.throws(()=>platformDatasetUrl('another-target'));
});
