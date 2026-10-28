import test from 'node:test';
import assert from 'node:assert/strict';
import {uiSequence} from '../src/ui-sequence.ts';
import {editProcessingDraft,currentProcessingDraft} from '../src/processing-drafts.ts';

test('a delayed poll cannot replace an operation result or start a second command',()=>{
  const ui=uiSequence(),oldPoll=ui.read();
  assert.equal(ui.begin(),true);assert.equal(ui.begin(),false);
  assert.equal(ui.read(),null);assert.equal(ui.accepts(oldPoll),false);
  ui.invalidate();ui.finish();assert.equal(ui.accepts(oldPoll),false);
  const next=ui.read();assert.equal(ui.accepts(next),true);
});

test('only the newest read can update the screen, including after login changes',()=>{
  const ui=uiSequence(),older=ui.read(),latest=ui.read();
  assert.equal(ui.accepts(older),false);assert.equal(ui.accepts(latest),true);
  ui.invalidate();assert.equal(ui.accepts(latest),false);
});

test('category drafts keep independent revisions and never auto-adopt a new media generation',()=>{
  const first=editProcessingDraft(undefined,'10-200',4,1);
  const second=editProcessingDraft(undefined,'30-300',5,1);
  const edited=editProcessingDraft(first,'12-200',5,2);
  assert.equal(edited.value,'12-200');
  assert.equal(currentProcessingDraft(edited,5,2),false);
  assert.equal(currentProcessingDraft(first,4,2),false);
  assert.equal(currentProcessingDraft(second,5,1),true);
  assert.deepEqual(first,{value:'10-200',revision:4,generation:1});
});
