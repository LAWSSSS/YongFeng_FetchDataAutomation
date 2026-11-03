"""Wire-level checks for the isolated disk-backed real-video simulator."""
import hashlib
import io
import zipfile

import pytest
from PIL import Image

from app.audit_moves import source_fingerprint
from app.upload_audit import AuditError
from app.uploads import ProcessWorker
from tests.real_video_platform import PhotoPlatform, exercise_transfer_faults


def photo_zip(tmp_path):
    path = tmp_path / "real-photos.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as archive:
        for number in range(3):
            data = io.BytesIO()
            Image.new("RGB", (32, 24), (number * 80, 80, 20)).save(data, "JPEG")
            archive.writestr(f"images/real_truck_bed_{number:06d}.jpg", data.getvalue())
    return path


@pytest.mark.asyncio
async def test_disk_store_real_boto3_imports_actual_images_and_keeps_targets_independent(tmp_path):
    path = photo_zip(tmp_path)
    with PhotoPlatform(tmp_path / "platform") as platform:
        item = {"id": "wire-package", "name": path.name, "path": str(path), "size": path.stat().st_size,
                "fileVersion": source_fingerprint(path),
                "object": {"bucket": platform.bucket, "key": "exercise/simple-wire-package/actual.zip"}}
        events = []
        async def callback(event):
            events.append(event)
        await ProcessWorker().run(item, await platform.signature("27892"), callback)
        assert events[-1]["event"] == "complete"
        uploaded = platform.objects[item["object"]["key"]]
        assert isinstance(uploaded["path"], str)
        assert "body" not in uploaded
        assert hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(__import__("pathlib").Path(uploaded["path"]).read_bytes()).digest()
        job = await platform.submit("27892", [item["object"]["key"]])
        rows = await platform.images("27892", prefix="real_truck")
        assert len(rows) == 3
        assert len({row["id"] for row in rows}) == 3
        assert all(row["taskId"] == job and row["width"] == 32 for row in rows)
        assert not await platform.images("27893", prefix="real_truck")
        assert not await platform.jobs("27893")
        assert platform.metrics["logicalBytes"] == path.stat().st_size
        assert platform.metrics["importBytesRead"] > 0
        assert not list((platform.root / "parts").iterdir())


@pytest.mark.asyncio
async def test_nonexistent_object_cannot_create_import_receipt(tmp_path):
    with PhotoPlatform(tmp_path / "platform") as platform:
        with pytest.raises(AuditError):
            await platform.submit("27892", ["never-uploaded.zip"])
        assert not await platform.jobs("27892")
        assert platform.metrics["imports"] == 0


@pytest.mark.asyncio
async def test_fault_exercises_cover_actual_wire_and_reject_bad_receipts(tmp_path):
    path = photo_zip(tmp_path)
    with PhotoPlatform(tmp_path / "platform") as platform:
        rows = await exercise_transfer_faults(platform, path, tmp_path / "results")
    by_case = {row["case"]: row for row in rows}
    for case in ("positive", "part-disconnect", "complete-response-lost", "object-changed",
                 "lose_submit_once", "missing_image", "wrong_task", "duplicate_image",
                 "two-target-independent", "manual-import-busy", 'duplicate_id',
                 'null_id', 'empty_id', 'missing_id', 'restart-after-object-complete'):
        assert by_case[case]["passed"], by_case[case]
    duplicate = by_case["duplicate_id"]
    assert duplicate["actual"]["state"] == "attention"
    assert duplicate['actual']['actualImages'] == 0
    assert duplicate["passed"] is True
