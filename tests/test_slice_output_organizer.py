import asyncio

import pytest

from app.main import create_app
from app.audit_moves import source_fingerprint


def package_fixture(app, tmp_path, *, extra_file=False):
    app.state.slicing.store.settings["directories"] = [str(tmp_path / "videos")]
    video = tmp_path / "videos" / "鲁A12345_20261020_090000.mp4"
    video.parent.mkdir(parents=True)
    video.write_bytes(b"source video")
    folder = tmp_path / "videos" / "已上传" / video.stem / "车斗"
    image_dir = folder / "image"
    image_dir.mkdir(parents=True)
    image = image_dir / "frame_001.jpg"
    image.write_bytes(b"jpeg fixture")
    archive = folder / "鲁A12345_20261020_090000.zip"
    archive.write_bytes(b"zip fixture")
    if extra_file:
        (folder / "unrelated.txt").write_text("keep me")
    package = {"id": "package-1", "dataset": "27892", "category": "bed", "itemId": "item-1",
               "sourceDatasetId": "27912", "sourceVideoId": "42", "videoPath": str(video),
               "name": archive.name, "state": "complete", "path": str(archive), "imageDir": str(image_dir),
               "images": [image.name], "imageCount": 1, "fileVersion": source_fingerprint(archive),
               "imageVersions": {image.name: source_fingerprint(image)}, "actualImages": 1,
               "verifiedJobId": "verified-job", "jobId": "verified-job", "outputImageIds": ["verified-image"],
               "videoFileVersion": source_fingerprint(video)}
    app.state.slicing.store.save('items', {'id':'item-1','path':str(video), 'fileVersion':source_fingerprint(video),
        'name':video.name,'sourceDatasetId':'27912','sourceVideoId':'42','state':'complete', 'revision':0,
        'categories':{'bed':{'state':'complete','packageId':'package-1','actualImages':1,'targetDatasetId':'27892'},
                      'magnet':{'state':'no_target'}}})
    app.state.slicing.packages.store.save("items", package)
    return video, folder, archive, image


@pytest.mark.asyncio
async def test_output_organizer_moves_only_verified_program_owned_folder_and_rebinds_paths(tmp_path):
    app = create_app(data_dir=tmp_path / "data")
    video, old, archive, image = package_fixture(app, tmp_path)
    organizer = app.state.slice_output_organizer

    plan = organizer.preview({"expectedRevision": 0, "requestId": "organize-preview-1"})
    entry = next(row for row in plan["items"] if row["packageId"] == "package-1")
    assert entry["state"] == "planned" and entry["fileCount"] == 2
    assert entry["destination"].endswith("/车斗/鲁A12345_20261020_090000_42")

    result = await organizer.confirm(plan["id"], {"expectedRevision": plan["revision"],
                                                    "requestId": "organize-confirm-1"})
    assert result["state"] == "complete"
    assert not old.exists()
    updated = app.state.slicing.packages.store.get("items", "package-1")
    assert updated["path"] == entry["destination"] + "/" + archive.name
    assert updated["imageDir"] == entry["destination"] + "/image"
    assert (tmp_path / "data" / "切片结果" / "车斗" / "鲁A12345_20261020_090000_42" / "image" / image.name).is_file()
    assert video.is_file()
    await asyncio.gather(app.state.slicing.shutdown(), app.state.uploads.shutdown(),
                         app.state.videos.shutdown(), app.state.jobs.shutdown(),
                         app.state.upload_audit.shutdown())
    app.state.store.close()


@pytest.mark.asyncio
async def test_output_organizer_holds_unowned_files_and_never_moves_while_queue_runs(tmp_path):
    app = create_app(data_dir=tmp_path / "data")
    _, old, _, _ = package_fixture(app, tmp_path, extra_file=True)
    organizer = app.state.slice_output_organizer
    plan = organizer.preview({"expectedRevision": 0, "requestId": "organize-preview-extra"})
    entry = next(row for row in plan["items"] if row["packageId"] == "package-1")
    assert entry["state"] == "held"
    assert "任务之外" in entry["message"]
    assert old.is_dir()

    # Clean fixture and create a plan that would be movable, then verify the
    # live queue guard leaves it untouched.
    (old / "unrelated.txt").unlink()
    plan = organizer.preview({"expectedRevision": 0, "requestId": "organize-preview-running"})
    app.state.slicing.store.settings["enabled"] = True
    with pytest.raises(Exception, match="队列仍在运行"):
        await organizer.confirm(plan["id"], {"expectedRevision": plan["revision"],
                                               "requestId": "organize-confirm-running"})
    assert old.is_dir()
    await asyncio.gather(app.state.slicing.shutdown(), app.state.uploads.shutdown(),
                         app.state.videos.shutdown(), app.state.jobs.shutdown(),
                         app.state.upload_audit.shutdown())
    app.state.store.close()


@pytest.mark.asyncio
async def test_output_organizer_rejects_preview_after_directory_settings_change(tmp_path):
    app = create_app(data_dir=tmp_path / "data")
    _, old, _, _ = package_fixture(app, tmp_path)
    organizer = app.state.slice_output_organizer
    plan = organizer.preview({"expectedRevision": 0, "requestId": "organize-preview-stale"})
    app.state.slicing.store.settings["revision"] += 1
    app.state.slicing.store.save_settings()
    with pytest.raises(Exception, match="目录设置已变化"):
        await organizer.confirm(plan["id"], {"expectedRevision": plan["revision"],
                                                 "requestId": "organize-confirm-stale"})
    assert old.is_dir()
    await asyncio.gather(app.state.slicing.shutdown(), app.state.uploads.shutdown(),
                         app.state.videos.shutdown(), app.state.jobs.shutdown(),
                         app.state.upload_audit.shutdown())
    app.state.store.close()


@pytest.mark.asyncio
async def test_output_organizer_holds_every_photo_until_both_categories_finish(tmp_path):
    app=create_app(data_dir=tmp_path/'data')
    video,old,archive,image=package_fixture(app,tmp_path)
    item=app.state.slicing.store.get('items','item-1')
    item['categories']['magnet']={'state':'uploading'}
    app.state.slicing.store.save('items',item)
    organizer=app.state.slice_output_organizer
    held=organizer.preview({'expectedRevision':0,'requestId':'waiting-for-photo'})
    assert held['items'][0]['state']=='held' and '尚未全部' in held['items'][0]['message']
    assert old.is_dir() and archive.is_file() and image.is_file() and video.is_file()
    item['categories']['magnet']={'state':'no_target'}
    app.state.slicing.store.save('items',item)
    plan=organizer.preview({'expectedRevision':0,'requestId':'before-proof-changes'})
    assert plan['items'][0]['state']=='planned'
    package=app.state.slicing.packages.store.get('items','package-1');package['outputImageIds']=[]
    app.state.slicing.packages.store.save('items',package)
    with pytest.raises(Exception,match='尚未全部'):
        await organizer.confirm(plan['id'],{'expectedRevision':plan['revision'],'requestId':'do-not-move'})
    assert old.is_dir() and archive.is_file() and image.is_file() and video.is_file()
    from tests.test_slicing_performance import close_app
    await close_app(app)
