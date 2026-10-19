import test from 'node:test';
import assert from 'node:assert/strict';
import { correctionInterval, correctionReviewCurrent, prepareCorrection } from '../src/corrections.ts';

test('reviewing a preserved old draft binds the latest displayed record without changing the input', () => {
  const draft = {date:'2026-09-30',clock:'01:55:00',minutes:'59',revision:0,dirty:true};
  const before = {...draft};
  const first = prepareCorrection(draft,0,'review-first');
  assert.equal(correctionReviewCurrent(first,0),true);
  assert.equal(correctionReviewCurrent(first,1),false);
  const reviewedAgain = prepareCorrection(draft,1,'review-current');
  assert.equal(correctionReviewCurrent(reviewedAgain,1),true);
  assert.equal(reviewedAgain.expectedRevision,1);
  assert.equal(reviewedAgain.startTime,'2026-09-30 01:55:00');
  assert.equal(reviewedAgain.endTime,'2026-09-30 02:54:00');
  assert.equal(reviewedAgain.durationSeconds,3540);
  assert.deepEqual(draft,before);
  assert.equal(correctionReviewCurrent(reviewedAgain,2),false);
});

test('zero seconds omitted by the time control are normalized before preview and submission', () => {
  const short = correctionInterval({date:'2026-09-30',clock:'12:30',minutes:'60'});
  assert.equal(short.startTime,'2026-09-30 12:30:00');
  assert.deepEqual(short,correctionInterval({date:'2026-09-30',clock:'12:30:00',minutes:'60'}));
});

test('cross-day and fractional-minute intervals retain second precision', () => {
  assert.equal(correctionInterval({date:'2026-09-30',clock:'23:55',minutes:'60'}).endTime,'2026-10-01 00:55:00');
  const partial = correctionInterval({date:'2026-09-30',clock:'23:59:59',minutes:String(61/60)});
  assert.equal(partial.durationSeconds,61);
  assert.equal(partial.endTime,'2026-10-01 00:01:00');
});

test('invalid dates, times and durations never produce an approvable review', () => {
  const draft = {date:'2026-09-30',clock:'12:00',minutes:'60'};
  for (const patch of [{date:''},{date:'2026-02-29'},{date:'2026-09-31'},{date:'1999-12-31'},
    {clock:''},{clock:'24:00'},{clock:'12:60'},{clock:'12:00:60'},
    {minutes:''},{minutes:'NaN'},{minutes:'Infinity'},{minutes:'0.9999'},{minutes:'120.1'}]) {
    assert.throws(()=>prepareCorrection({...draft,...patch},1,'request'));
  }
  assert.throws(()=>prepareCorrection(draft,-1,'request'));
  assert.throws(()=>prepareCorrection(draft,1,''));
  assert.equal(correctionReviewCurrent(null,1),false);
});

test('retries use the exact reviewed interval and request ID until another review', () => {
  const input = {date:'2026-09-30',clock:'08:00',minutes:'45'};
  const review = prepareCorrection(input,3,'same-request');
  input.clock='09:00';
  assert.equal(review.requestId,'same-request');
  assert.equal(review.startTime,'2026-09-30 08:00:00');
  assert.equal(review.endTime,'2026-09-30 08:45:00');
  assert.equal(review.expectedRevision,3);
});
