"""Recovery keeps the full cache and never overwrites unknown partial copies."""
import asyncio
import errno
from pathlib import Path
from types import SimpleNamespace

import pytest

import app.videos as module
from app.main import create_app
from app.media import DownloadFailure
from app.media_validation import MediaValidationError
from tests.test_slicing_performance import close_app
from tests.test_simple_flow import app_factory, platform, new_video
import httpx


def cache(tmp_path):
    root=tmp_path/'cache';root.mkdir()
    source=root/'v3.part';source.write_bytes(b'\0\0\0\x18ftypmp42'+b'valid-cache'*100)
    destination=tmp_path/'managed'/'truck.mp4';destination.parent.mkdir()
    identity,digest=module.inspect_recovery_cache(source,root,source.stat().st_size,source.stat().st_size)
    return source,destination,identity,digest,{'token':'a'*32}


def test_partial_destination_is_preserved_and_verified_cache_moves_without_copy(tmp_path,monkeypatch):
    source,destination,identity,digest,reservation=cache(tmp_path)
    expected=source.read_bytes();destination.write_bytes(expected[:30])
    chosen,conflict=module.recovery_destination(destination,len(expected),digest,reservation)
    assert chosen!=destination and conflict['size']==30
    monkeypatch.setattr(module,'publish_file',lambda *_:pytest.fail('Same-volume recovery must not copy gigabytes.'))
    module.publish_recovery_cache(source,chosen,identity,digest,reservation)
    assert destination.read_bytes()==expected[:30]
    assert chosen.read_bytes()==expected and not source.exists()


def test_interrupted_atomic_publication_reuses_its_durable_marker(tmp_path,monkeypatch):
    source,destination,identity,digest,reservation=cache(tmp_path)
    original=module.publish_with_reservation
    def interrupted(src,dst,**kwargs):
        marker=b'simple-video-transfer-reservation:'+reservation['token'].encode()
        dst.write_bytes(marker)
        raise OSError(errno.EIO,'controlled interruption')
    monkeypatch.setattr(module,'publish_with_reservation',interrupted)
    with pytest.raises(OSError):module.publish_recovery_cache(source,destination,identity,digest,reservation)
    assert source.exists()
    chosen,conflict=module.recovery_destination(destination,source.stat().st_size,digest,reservation)
    assert chosen==destination and conflict is None
    monkeypatch.setattr(module,'publish_with_reservation',original)
    module.publish_recovery_cache(source,chosen,identity,digest,reservation)
    assert destination.stat().st_size==identity[2] and not source.exists()


def test_unknown_full_file_and_changed_source_are_not_overwritten(tmp_path):
    source,destination,identity,digest,reservation=cache(tmp_path)
    destination.write_bytes(b'x'*identity[2])
    with pytest.raises(FileExistsError):module.publish_recovery_cache(source,destination,identity,digest,reservation)
    assert source.exists() and destination.read_bytes()==b'x'*identity[2]
    source.write_bytes(b'changed')
    with pytest.raises(module.Conflict):module.publish_recovery_cache(source,destination,identity,digest,reservation)
    assert destination.read_bytes()==b'x'*identity[2]


def test_symlink_destination_is_never_replaced(tmp_path):
    source,destination,identity,digest,reservation=cache(tmp_path)
    other=tmp_path/'user-file';other.write_bytes(b'keep')
    destination.symlink_to(other)
    with pytest.raises(module.Conflict):module.recovery_destination(destination,identity[2],digest,reservation)
    assert other.read_bytes()==b'keep' and source.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('failure',[None,'missing','timeout','damaged'])
async def test_configured_tools_work_without_shell_path_and_failures_stay_distinct(tmp_path,monkeypatch,failure):
    app=create_app(data_dir=tmp_path/'db')
    folder=tmp_path/'tools';folder.mkdir()
    for name in ['ffmpeg','ffprobe']:
        path=folder/name;path.write_text('test executable');path.chmod(0o755)
    app.state.slicing.store.settings['frameToolPath']=str(folder/'ffmpeg')
    monkeypatch.setenv('PATH','/usr/bin:/bin')
    calls=[]
    async def communicate():return b'60.0\n',b''
    async def launch(*command,**_):
        calls.append(command)
        return SimpleNamespace(returncode=0,communicate=communicate)
    monkeypatch.setattr(module.asyncio,'create_subprocess_exec',launch)
    def tail(path,tool,duration):
        assert tool==str(folder/'ffmpeg') and duration==60
        if failure=='timeout':raise MediaValidationError('media_check_timeout')
        if failure=='damaged':raise MediaValidationError('media_incomplete')
    monkeypatch.setattr(module,'verify_video_tail',tail)
    if failure=='missing':(folder/'ffprobe').unlink()
    try:
        if failure:
            with pytest.raises(DownloadFailure) as result:await app.state.videos.ffprobe(tmp_path/'video.mp4')
            assert result.value.code=={'missing':'media_tool_missing','timeout':'media_check_timeout','damaged':'invalid_video'}[failure]
        else:
            assert await app.state.videos.ffprobe(tmp_path/'video.mp4')==60
        if calls:assert calls[0][0]==str(folder/'ffprobe')
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_recovery_error_records_sanitized_stage_and_errno(tmp_path,monkeypatch):
    app=create_app(data_dir=tmp_path/'db');manager=app.state.videos
    manager.store.save_operation(dict(id='op',videoId='video',kind='recover-decodable',state='queued',
        version=0,expectedRevision=0,requestId='one'))
    async def failed(operation):
        operation['stage']='moving_cache';manager.store.save_operation(operation)
        raise OSError(errno.ENOSPC,'must-not-leak-secret-value')
    monkeypatch.setattr(manager,'_execute_recovery_operation',failed)
    async def ignore(*_):pass
    monkeypatch.setattr(manager,'_update_recovery_version',ignore)
    try:
        await manager._run_operation('op')
        result=manager.store.get_operation('op')
        assert result['errorCode']=='disk_full' and result['failedStage']=='moving_cache'
        assert result['failureErrno']==errno.ENOSPC
        assert 'must-not-leak' not in str(result)
    finally:await close_app(app)


@pytest.mark.asyncio
async def test_recovery_api_skips_old_partial_and_queues_exact_current_version(app_factory,platform,tmp_path,monkeypatch):
    app=app_factory()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1') as client:
            row=await new_video(client,app)
            manager=app.state.videos;slicing=app.state.slicing
            slicing.auth.access='fixture-only'
            slicing.store.settings['targetDatasetIds']={'27912':'101','27944':'102'}
            async def idle(*_,**__):pass
            monkeypatch.setattr(slicing,'_tick',idle)
            monkeypatch.setattr(slicing,'_wake_scheduler',lambda:None)
            monkeypatch.setattr(manager,'_monitor_recovery',idle)
            monkeypatch.setattr('app.slicing.ffprobe_file',lambda *_:{'duration':60,'size':len(platform.media),'width':3840,'height':2160})
            row=manager.store.get(row['id']);version=row['versions'][0]
            version.update(state='failed',errorCode='invalid_video',downloadRequested=True,
                etag='"v1"',totalBytes=len(platform.media),receivedBytes=len(platform.media))
            old_destination=tmp_path/'managed'/'original.mp4';old_destination.parent.mkdir()
            old_destination.write_bytes(b'partial old copy')
            version['recovery']={'destination':str(old_destination)}
            manager.store.save(row)
            source=manager.work/row['id']/'v0.part';source.parent.mkdir(parents=True);source.write_bytes(platform.media)
            body={'requestId':'fix-partial-copy','expectedRevision':row['revision'],'version':0}
            result=await client.post(f'/api/videos/{row["id"]}/recover-decodable',json=body)
            assert result.status_code==202,result.text
            identity=result.json()['operationId']
            task=manager.operation_tasks.get(identity)
            if task:await task
            operation=manager.store.get_operation(identity)
            assert operation['stage']=='partial_slicing',operation
            current=manager.store.get(row['id'])
            assert current['activeRevision']==row['activeRevision'] and current['revision']==row['revision']
            recovery=current['versions'][0]['recovery']
            assert Path(recovery['filePath']).read_bytes()==platform.media
            assert old_destination.read_bytes()==b'partial old copy' and not source.exists()
            item=slicing.store.get('items',recovery['slicingItemId'])
            assert item['collectionVideoId']==row['id'] and item['collectionRevision']==0 and item['partialRecovery']
            assert not app.state.uploads.store.list('items')
            replay=await client.post(f'/api/videos/{row["id"]}/recover-decodable',json=body)
            assert replay.json()['operationId']==identity and len(slicing.store.list('items'))==1
            again=await client.post(f'/api/videos/{row["id"]}/recover-decodable',json={**body,'requestId':'another-click'})
            assert again.json()['operationId']==identity
            assert len([o for o in manager.store.list_operations(row['id']) if o['kind']=='recover-decodable'])==1
