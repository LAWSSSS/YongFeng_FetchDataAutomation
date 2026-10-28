import copy
import re

import pytest

from app.slice_export import photo_export_identity, photo_names
from app.slice_packages import image_prefix


NAME = '鲁Q031JM_20261023120000_original.mp4'


def export_source():
    return {'sourceIdentity': 'collection:' + 'a' * 24 + ':v5:original',
            'sourceDatasetId': '27944', 'sourceVideoId': 'source-id',
            'mediaGeneration': 5, 'collectionRevision': 5,
            'observationVersion': {'size': 100, 'edgeHash': 'video-hash'}}


def test_same_time_and_plate_in_different_videos_have_different_photo_names():
    first = photo_names(NAME, 2)
    other = photo_names(NAME.replace('_original.', '_corrected.'), 2)
    assert first != other and set(first).isdisjoint(other)
    assert first == photo_names(NAME, 2)
    assert all(re.fullmatch(r'鲁Q031JM_20261023_120000_v[a-f0-9]{24}_00[12]\.jpg', name) for name in first)
    assert image_prefix({'images': first}) != image_prefix({'images': other})


def test_retries_and_verified_path_moves_keep_the_export_identity():
    source = export_source()
    frames, positions = [b'first-jpeg', b'second-jpeg'], [0.0, 30.0]
    original = photo_export_identity(source, 'magnet', positions, frames)
    moved = copy.deepcopy(source)
    moved.update(path='/different/path.mp4', revision=200, state='ready',
                 fileVersion={'size': 100, 'inode': 999, 'modifiedNs': 999})
    assert photo_export_identity(moved, 'magnet', positions, frames) == original
    assert photo_names(NAME, 2, identity=original) == photo_names(NAME, 2, identity=original)


@pytest.mark.parametrize('change', ['version', 'generation', 'category', 'position', 'content'])
def test_photo_names_isolate_media_versions_and_actual_exported_content(change):
    source = export_source()
    positions, frames, category = [0, 30], [b'first-jpeg', b'second-jpeg'], 'magnet'
    original = photo_export_identity(source, category, positions, frames)
    if change == 'version':
        source['observationVersion']['edgeHash'] = 'changed-video'
    elif change == 'generation':
        source['mediaGeneration'] += 1
    elif change == 'category':
        category = 'bed'
    elif change == 'position':
        positions[1] = 60
    else:
        frames[0] = b'different-photo'
    updated = photo_export_identity(source, category, positions, frames)
    assert set(photo_names(NAME, 2, identity=original)).isdisjoint(photo_names(NAME, 2, identity=updated))


def test_export_identity_rejects_a_mismatched_successful_frame_list():
    with pytest.raises(ValueError):
        photo_export_identity(export_source(), 'bed', [0, 30], [b'one-photo'])


def test_utf8_photo_names_fit_the_filesystem_and_do_not_contain_controls():
    name = photo_names('😀' * 100 + '\x00\n.mp4', 1)[0]
    assert len(name.encode()) < 255
    assert '\x00' not in name and '\n' not in name and '/' not in name
