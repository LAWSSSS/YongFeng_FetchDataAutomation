import test from 'node:test';
import assert from 'node:assert/strict';
import { auditBucket } from '../src/upload-audit.ts';

test('classification uses confirmed statuses and fixed dataset IDs for both move folders and filters', () => {
  const cases = [
    ['uploaded','uploaded','uploaded'],
    ['uploaded','missing','missing_magnet'],
    ['missing','uploaded','missing_bed'],
    ['missing','missing','both_missing'],
    ['uploaded','review','held'],
    ['review','missing','held'],
    ['missing','in_progress','held'],
  ];
  for (const [bed,magnet,expected] of cases) {
    assert.equal(auditBucket({targets:{'27944':{status:magnet},'27912':{status:bed}}}),expected);
  }
  assert.equal(auditBucket({targets:{'27912':{status:'uploaded'}}}),'held');
  assert.equal(auditBucket({issue:'empty video',targets:{'27912':{status:'uploaded'},'27944':{status:'uploaded'}}}),'held');
});
