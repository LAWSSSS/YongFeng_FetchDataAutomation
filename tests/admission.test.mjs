import test from 'node:test';
import assert from 'node:assert/strict';
import { admissionDraft, admissionSummary, defaultAdmission, parseAdmissionDraft } from '../src/admission.ts';

test('admission defaults preserve plate-or-photo and strictly greater than ten minutes', () => {
  assert.deepEqual(parseAdmissionDraft(admissionDraft(defaultAdmission)),defaultAdmission);
  assert.match(admissionSummary(defaultAdmission),/有真实车牌 或 智能判级有效照片至少 1 张/);
  assert.match(admissionSummary(defaultAdmission),/严格超过 10 分钟/);
  assert.match(admissionSummary({...defaultAdmission,matchMode:'all',minPhotoCount:3}),/有真实车牌 且 智能判级有效照片至少 3 张/);
});

test('disabled requirements do not filter out records that happen to have plates or photos', () => {
  assert.equal(admissionSummary({...defaultAdmission,requireRealPlate:false}),
    '智能判级有效照片至少 1 张，且质检时长严格超过 10 分钟');
  assert.equal(admissionSummary({...defaultAdmission,requireRealPlate:false,requireIntelligentPhotos:false}),
    '不要求车牌和照片；质检时长严格超过 10 分钟');
});

test('empty and invalid input remains a draft, including decimal duration and hidden photo count', () => {
  const draft = admissionDraft(defaultAdmission);
  for (const value of ['', ' ', '-1', 'Infinity', 'NaN']) {
    assert.throws(()=>parseAdmissionDraft({...draft,minInspectionMinutes:value}));
  }
  assert.equal(parseAdmissionDraft({...draft,minInspectionMinutes:'11.5'}).minInspectionMinutes,11.5);
  for (const value of ['', '0', '1.5', 'Infinity']) {
    assert.throws(()=>parseAdmissionDraft({...draft,minPhotoCount:value}));
  }
  const disabled = parseAdmissionDraft({...draft,requireIntelligentPhotos:false,minPhotoCount:''});
  assert.equal(disabled.minPhotoCount,1);
  assert.equal(parseAdmissionDraft({...draft,minPhotoCount:'3'}).minPhotoCount,3);
});
