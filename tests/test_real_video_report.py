"""Destructive rehearsal cleanup must never infer ownership from filenames."""
import json

import pytest

from tests.real_video_report import cleanup_media, edge


def setup_root(tmp_path):
    root = tmp_path / 'owned-run'
    root.mkdir()
    source = tmp_path / 'protected-original.mp4'
    source.write_bytes(b'protected')
    (root / 'run.json').write_text(json.dumps({'root': str(root), 'source': str(source)}))
    return root, source


def test_only_explicit_registered_test_copy_is_deleted_unknown_neighbor_is_preserved(tmp_path):
    root, source = setup_root(tmp_path)
    copy = root / 'source.mp4'; copy.write_bytes(b'test-copy')
    unknown = root / 'unknown.mp4'; unknown.write_bytes(b'unknown-user-file')
    (root / 'owned-media.json').write_text(json.dumps({'paths': ['source.mp4']}))
    result = cleanup_media(root)
    assert result['complete'] and result['deletedFiles'] == 1
    assert not copy.exists() and unknown.read_bytes() == b'unknown-user-file'
    assert source.read_bytes() == b'protected'
    assert cleanup_media(root)['deletedFiles'] == 1  # replay is idempotent


@pytest.mark.parametrize('name', ['../protected-original.mp4', 'linked.mp4'])
def test_escape_or_symbolic_link_cannot_authorize_deleting_the_original(tmp_path, name):
    root, source = setup_root(tmp_path)
    if name == 'linked.mp4':
        (root / name).symlink_to(source)
    (root / 'owned-media.json').write_text(json.dumps({'paths': [name]}))
    with pytest.raises(ValueError):
        cleanup_media(root)
    assert source.read_bytes() == b'protected'


def test_changed_file_is_preserved_when_replaying_a_saved_cleanup_manifest(tmp_path):
    root, _ = setup_root(tmp_path)
    path = root / 'copy.mp4'; path.write_bytes(b'before')
    entry = {'path': path.name, 'size': path.stat().st_size, 'edgeHash': edge(path), 'state': 'planned'}
    (root / 'media-cleanup-manifest.json').write_text(json.dumps({'complete': False, 'entries': [entry]}))
    path.write_bytes(b'after!')
    result = cleanup_media(root)
    assert not result['complete'] and path.exists()
    assert result['entries'][0]['state'] == 'retained-version-changed'


def test_missing_run_marker_or_registry_cannot_delete_neighbors(tmp_path):
    root, _ = setup_root(tmp_path)
    path = root / 'unknown.mp4'; path.write_bytes(b'never-inferred')
    with pytest.raises(ValueError, match='explicit test artifact registry'):
        cleanup_media(root)
    assert path.exists()
