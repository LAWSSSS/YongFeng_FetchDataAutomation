"""Physical-file analysis: one decoder, independent model outcomes."""
import asyncio
import copy
from contextlib import ExitStack

from .slicing_core import build_category_result, category_image_target, sample_points


def category_signature(category):
    """Identity of a category outcome. Progress text and poll timestamps are excluded."""
    if not isinstance(category, dict):
        return ()
    intervals = category.get("intervals")
    if isinstance(intervals, list):
        encoded = tuple((row.get("start"), row.get("end")) if isinstance(row, dict) else (row,) for row in intervals)
    else:
        encoded = ()
    return (
        category.get("state"), category.get("errorCode") or "",
        str(category.get("taskId") or ""), str(category.get("operationId") or ""),
        str(category.get("targetDatasetId") or ""), category.get("intervalSeconds"),
        str(category.get("modelFingerprint") or ""), encoded,
    )


def outcome_changed(previous, current):
    if not previous:
        return True
    if previous.get("state") != current.get("state"):
        return True
    if (previous.get("errorCode") or "") != (current.get("errorCode") or ""):
        return True
    if previous.get("path") != current.get("path") or previous.get("fileVersion") != current.get("fileVersion"):
        return True
    old, new = previous.get("categories") or {}, current.get("categories") or {}
    if set(old) != set(new):
        return True
    return any(category_signature(old.get(name)) != category_signature(category) for name, category in new.items())


def revision_after_outcome(stored, current, changed):
    if not changed:
        if stored:
            current["revision"] = int(stored.get("revision", 0))
        return
    base = int(stored.get("revision", 0)) if stored else int(current.get("revision", 0))
    current["revision"] = base + 1


class SharedSlicingAnalysis:
    def _save_analysis_item(self, work):
        peers = work.get('_analysisPeers')
        if not peers:
            stored = self.store.get('items', work['id']) if work.get('id') else None
            if stored and stored.get('superseded'):
                return
            if stored and 'collectionRecoveryPaused' in stored:
                work['collectionRecoveryPaused'] = stored['collectionRecoveryPaused']
            revision_after_outcome(stored, work, outcome_changed(stored, work))
            self.store.save('items', work)
            return
        for category, identity in peers.items():
            item = self.store.get('items', identity)
            before = copy.deepcopy(item)
            for field in ('metadata', 'progress', 'analysisStage', 'analysisTotal', 'analysisDone', 'analysisPosition'):
                if field in work:
                    item[field] = work[field]
            item['categories'][category] = copy.deepcopy(work['categories'][category])
            revision_after_outcome(before, item, outcome_changed(before, item))
            self.store.save('items', item)

    async def _process_shared(self, first, candidates):
        from .slicing import applicable_category, source_version, ERROR_TEXT, ffprobe_file
        peers = {applicable_category(first['sourceDatasetId']): first}
        for candidate in candidates:
            category = applicable_category(candidate['sourceDatasetId'])
            same_source = (candidate.get('mediaId') == first.get('mediaId') and first.get('mediaId')
                           and candidate.get('mediaGeneration') == first.get('mediaGeneration')) or candidate.get('path') == first['path']
            if (category not in peers and same_source
                    and candidate.get('fileVersion') == first['fileVersion']
                    and candidate.get('state') == 'pending'):
                peers[category] = candidate
        if len(peers) == 1:
            return await self.process_with_recovery(first)
        work = copy.deepcopy(first)
        work['_analysisPeers'] = {name: item['id'] for name, item in peers.items()}
        work['categories'] = {name: copy.deepcopy(item['categories'][name]) for name, item in peers.items()}
        for item in peers.values():
            item.update(state='analyzing', message='共用抽帧识别中。')
            self.store.save('items', item)
        affected = tuple(peers)
        guard = ExitStack()
        try:
            if getattr(self, 'control', None):
                self.control.claim(work)
            guard.enter_context(self._read_lease(work["path"]))
            if await asyncio.to_thread(source_version, work['path']) != work['fileVersion']:
                raise ValueError('source_changed')
            metadata = work.get('metadata') or await asyncio.to_thread(ffprobe_file, work['path'], self.store.settings['frameToolPath'])
            work['metadata'] = metadata
            for phase in ('initial', 'refine'):
                keys, missing = {}, {}
                active = tuple(name for name in peers if work['categories'][name].get('state') != 'attention') if phase == 'initial' else ('magnet',)
                affected = active
                interval = self.store.settings['sampleIntervalSeconds'] if phase == 'initial' else 10
                points = sample_points(metadata['duration'], interval, metadata.get('frameRate', 25))
                fingerprints = {}
                for name in active:
                    try:
                        fingerprints[name] = self._model_fingerprint(name)
                    except (OSError, ValueError) as error:
                        code = str(error) if str(error) in ERROR_TEXT else 'model_load_failed'
                        work['categories'][name].update(state='attention', errorCode=code, message=ERROR_TEXT[code])
                active = tuple(fingerprints)
                for name in active:
                    keys[name] = {point: self._observation_key(work, name, point, fingerprints[name]) for point in points}
                    missing[name] = [point for point in points if not self._get_observation(keys[name][point])]
                if any(missing.values()):
                    await self._run_worker(work, metadata, missing, keys, phase)
                for name in active:
                    observations = self._category_observations(work, name, points, keys[name], fingerprints[name])
                    category = work['categories'][name]
                    partial = bool(work.get('partialRecovery'))
                    unavailable = [row for row in observations if row.get('code') == 'decode_unavailable']
                    valid = [row for row in observations if row.get('code') != 'decode_unavailable']
                    result = build_category_result(valid, interval=interval, duration=metadata['duration'],
                        positive_labels={'target'}, target=category_image_target(self.store.settings, name),
                        undecodable_points=[row['time'] for row in unavailable] if partial else ())
                    category.update(intervals=result['intervals'], intervalSeconds=result['intervalSeconds'],
                        estimatedImages=result['estimatedImages'], observations=len(valid),
                        samplingIntervalSeconds=interval, modelFingerprint=fingerprints[name],
                        targetDatasetId=self.store.settings['targetDatasetIds'][peers[name]['sourceDatasetId']],
                        partialRecovery=partial, skippedSamples=len(unavailable))
                    uncertain = next((row for row in valid if row.get('code') or row['label'] == 'uncertain'), None)
                    if category.get('state') == 'attention':
                        continue
                    if partial and not valid:
                        code = 'no_decodable_samples'
                    elif len(valid) + len(unavailable) != len(points):
                        code = 'observations_incomplete'
                    elif uncertain:
                        code = uncertain.get('code') or 'bed_alignment_uncertain'
                    elif partial and any(row['label'] == 'target' for row in valid) and not result['intervals']:
                        code = 'no_cuttable_fragments'
                    elif result['tooMany']:
                        code = 'slice_limit'
                    else:
                        code = ''
                    if code:
                        category.update(state='attention', errorCode=code, message=ERROR_TEXT.get(code, ERROR_TEXT['inference_failed']))
                    else:
                        category.update(state='ready' if result['intervals'] else 'no_target', errorCode='',
                                        message=('仅处理可解码片段；损坏位置已跳过。' if partial and result['intervals'] else
                                                 '可解码片段未检出目标，损坏位置已跳过。' if partial else
                                                 '等待平台空位。' if result['intervals'] else '未检出目标'))
                self._save_analysis_item(work)
                magnet = work['categories'].get('magnet', {})
                if phase == 'refine' or 'magnet' not in fingerprints or magnet.get('state') == 'attention' or 100 <= (magnet.get('estimatedImages') or 0) <= 200:
                    break
        except asyncio.CancelledError:
            for identity in work['_analysisPeers'].values():
                item = self.store.get('items', identity)
                item.update(state='pending', message='已暂停，观察保留。')
                self.store.save('items', item)
            raise
        except Exception as error:
            code = str(error) if str(error) in ERROR_TEXT else 'worker_failed'
            for name in affected:
                category = work['categories'][name]
                if category.get('state') != 'attention':
                    category.update(state='attention', errorCode=code, message=ERROR_TEXT.get(code, '本条未完成，原视频保留。'))
            self._save_analysis_item(work)
        finally:
            guard.close()
        for name, identity in work['_analysisPeers'].items():
            item = self.store.get('items', identity)
            category = item['categories'][name]
            state = category.get('state')
            item.update(state='platform_processing' if state in {'ready', 'waiting_slot'} else state,
                        errorCode=category.get('errorCode', ''), message=category.get('message', ''))
            self._recover_analysis_outcome(item)
            self.store.save('items', item)
