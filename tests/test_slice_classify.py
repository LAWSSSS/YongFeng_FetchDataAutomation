from app.slice_classify import apply_moves, classify_pair, plan_moves


def test_slice_pairs_follow_the_video_upload_buckets():
    assert classify_pair("complete", "complete") == "uploaded"
    assert classify_pair("complete", "queued") == "missing_magnet"
    assert classify_pair("queued", "complete") == "missing_bed"
    assert classify_pair(None, None) == "both_missing"
    assert classify_pair("review", "complete") == "held"
    assert classify_pair("importing", "missing") == "held"


def test_confirmed_pairs_move_without_overwriting(tmp_path):
    bed = tmp_path / "bed.zip"
    magnet = tmp_path / "magnet.zip"
    bed.write_bytes(b"bed")
    magnet.write_bytes(b"magnet")
    items = [
        {"sourceVideoId": "1", "category": "bed", "state": "complete", "name": "one.zip", "path": str(bed)},
        {"sourceVideoId": "1", "category": "magnet", "state": "complete", "name": "one-m.zip", "path": str(magnet)},
    ]
    plan = plan_moves(items, tmp_path / "sorted")
    assert plan["items"][0]["kind"] == "uploaded"
    result = apply_moves(plan)
    assert (tmp_path / "sorted" / "已上传" / "bed.zip").read_bytes() == b"bed"
    assert not bed.exists()
    again = apply_moves(plan_moves([
        {"sourceVideoId": "1", "category": "bed", "state": "complete", "name": "one.zip", "path": str(tmp_path / "sorted" / "已上传" / "bed.zip")},
        {"sourceVideoId": "1", "category": "magnet", "state": "complete", "name": "one-m.zip", "path": str(tmp_path / "sorted" / "已上传" / "magnet.zip")},
    ], tmp_path / "sorted"))
    assert "bed.zip" in again["skipped"]
    assert len(result["moved"]) == 2
