import hashlib
import io
import json
from pathlib import Path

import pytest

from app.original_restore import RestoreFailure, download_bound_object, edge_hash


def make_item(data):
    version = {"device": 1, "inode": 2, "size": len(data), "modifiedNs": 3}
    identity = "a" * 32
    marker = hashlib.sha256(json.dumps(version, sort_keys=True).encode()).hexdigest()
    return {
        "id": identity,
        "dataset": "27912",
        "name": "test_video.mp4",
        "size": len(data),
        "fileVersion": version,
        "uploadFileVersion": version,
        "object": {"bucket": "bound-bucket", "key": f"prefix/simple-{identity}/test_video.mp4"},
        "metadata": {"uploader-id": identity, "file-version": marker},
        "content": data,
    }


class FlakyBody:
    def __init__(self, data, fail_after=None):
        self.data = data
        self.offset = 0
        self.fail_after = fail_after
        self.failed = False

    def read(self, length=-1):
        if self.fail_after is not None and not self.failed and self.offset >= self.fail_after:
            self.failed = True
            raise OSError("simulated streaming interruption")
        if length < 0:
            length = len(self.data) - self.offset
        if self.fail_after is not None and not self.failed:
            length = min(length, self.fail_after - self.offset)
        end = min(len(self.data), self.offset + length)
        result = self.data[self.offset:end]
        self.offset = end
        return result

    def close(self):
        pass


class FakeS3:
    def __init__(self, item):
        self.item = item
        self.calls = {}
        self.fail_range_once = None
        self.fail_range_always = None

    def head_object(self, **_kwargs):
        return {"ContentLength": len(self.item["content"]), "ETag": '"stable-etag"',
                "Metadata": self.item["metadata"]}

    def get_object(self, **kwargs):
        range_key = kwargs["Range"]
        start, end = [int(value) for value in range_key.removeprefix("bytes=").split("-")]
        self.calls[range_key] = self.calls.get(range_key, 0) + 1
        if range_key == self.fail_range_always or (range_key == self.fail_range_once and self.calls[range_key] == 1):
            raise OSError("simulated response streaming error")
        body_data = self.item["content"][start:end + 1]
        body = FlakyBody(body_data, fail_after=8192 if self.calls[range_key] == 1 and range_key == "bytes=0-199999" else None)
        return {"Body": body, "ContentLength": len(body_data),
                "ContentRange": f"bytes {start}-{end}/{len(self.item['content'])}", "ETag": '"stable-etag"'}


def expected_version(data):
    return {"size": len(data), "edgeHash": edge_hash(data[:65536], data[-65536:])}


def test_download_retries_a_streaming_range_and_publishes_verified_original(tmp_path):
    data = bytes((index * 31) % 251 for index in range(750_123))
    item = make_item(data)
    client = FakeS3(item)
    client.fail_range_once = "bytes=200000-399999"
    destination = tmp_path / item["name"]

    result = download_bound_object(client, item, expected_version(data), destination,
                                   chunk_size=200_000, workers=2, retries=2,
                                   backoff=(0,), sleep=lambda _seconds: None)

    assert result["existing"] is False
    assert destination.read_bytes() == data
    assert client.calls["bytes=200000-399999"] == 2
    assert not list(tmp_path.glob("*.part"))
    assert not list(tmp_path.glob("*.json"))


def test_restore_resumes_only_unverified_ranges_after_a_failed_attempt(tmp_path):
    data = bytes((index * 17) % 253 for index in range(400_000))
    item = make_item(data)
    client = FakeS3(item)
    client.fail_range_always = "bytes=200000-399999"
    destination = tmp_path / item["name"]

    with pytest.raises(RestoreFailure) as failure:
        download_bound_object(client, item, expected_version(data), destination,
                              chunk_size=200_000, workers=1, retries=2,
                              backoff=(0,), sleep=lambda _seconds: None)
    assert failure.value.code == "network"
    assert "bytes=0-199999" in client.calls
    assert not destination.exists()

    client.fail_range_always = None
    before = client.calls["bytes=0-199999"]
    download_bound_object(client, item, expected_version(data), destination,
                          chunk_size=100_000, workers=1, retries=2,
                          backoff=(0,), sleep=lambda _seconds: None)

    assert client.calls["bytes=0-199999"] == before
    assert "bytes=0-99999" not in client.calls
    assert "bytes=100000-199999" not in client.calls
    assert client.calls["bytes=200000-299999"] == 1
    assert client.calls["bytes=300000-399999"] == 1
    assert destination.read_bytes() == data


def test_restore_splits_a_repeatedly_failing_large_range(tmp_path):
    data = bytes((index * 19) % 247 for index in range(2_500_000))
    item = make_item(data)
    client = FakeS3(item)
    client.fail_range_always = "bytes=0-2097151"
    destination = tmp_path / item["name"]

    result = download_bound_object(client, item, expected_version(data), destination,
                                   chunk_size=2_097_152, workers=1, retries=2,
                                   backoff=(0,), sleep=lambda _seconds: None)

    assert result["existing"] is False
    assert destination.read_bytes() == data
    assert client.calls["bytes=0-2097151"] == 2
    assert client.calls["bytes=0-1048575"] == 1
    assert client.calls["bytes=1048576-2097151"] == 1
    assert not list(tmp_path.glob("*.part"))
    assert not list(tmp_path.glob("*.json"))


def test_restore_never_overwrites_a_different_existing_file(tmp_path):
    data = b"complete original video bytes" * 1000
    item = make_item(data)
    client = FakeS3(item)
    destination = tmp_path / item["name"]
    destination.write_bytes(b"user file already here")

    with pytest.raises(RestoreFailure) as failure:
        download_bound_object(client, item, expected_version(data), destination)

    assert failure.value.code == "conflict"
    assert destination.read_bytes() == b"user file already here"
    assert client.calls == {}


def test_restore_rejects_a_cloud_object_with_different_bound_metadata(tmp_path):
    data = b"complete original video bytes" * 1000
    item = make_item(data)
    item["metadata"]["uploader-id"] = "different-upload"
    client = FakeS3(item)
    destination = tmp_path / item["name"]

    with pytest.raises(RestoreFailure) as failure:
        download_bound_object(client, item, expected_version(data), destination)

    assert failure.value.code == "identity"
    assert not destination.exists()
    assert client.calls == {}
