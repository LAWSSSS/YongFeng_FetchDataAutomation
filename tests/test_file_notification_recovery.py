"""A failed move/update listener must not strand unrelated pending managers."""
import pytest

from app.file_leases import FileLeases


def fail_with_sensitive_message(*_):
    raise ValueError('sensitive-message-not-for-diagnostics')


@pytest.mark.parametrize('kind', ['read', 'write'])
def test_release_listener_failure_still_releases_and_wakes_other_managers(tmp_path, kind, caplog):
    leases = FileLeases()
    path = str(tmp_path / 'video.mp4')
    woken = []
    leases.subscribe_release(fail_with_sensitive_message)
    leases.subscribe_release(woken.append)
    with getattr(leases, kind)(path):
        assert leases.busy(path)
    assert not leases.busy(path)
    assert not leases.readers and not leases.writers
    assert woken == [path]
    assert leases.notification_failures == [{'event': 'release', 'errorType': 'ValueError'}]
    assert 'sensitive-message-not-for-diagnostics' not in caplog.text


def test_release_failure_never_overwrites_the_actual_file_error(tmp_path):
    leases = FileLeases()
    leases.subscribe_release(fail_with_sensitive_message)
    path = str(tmp_path / 'video.mp4')
    with pytest.raises(OSError, match='actual disk error'):
        with leases.write(path):
            raise OSError('actual disk error')
    assert not leases.busy(path)


@pytest.mark.parametrize('primary_fails', [False, True])
def test_move_notifies_all_stores_and_still_fails_for_durable_replay(primary_fails):
    leases = FileLeases()
    synced = []
    if primary_fails:
        leases.on_move = fail_with_sensitive_message
    else:
        leases.subscribe_moves(fail_with_sensitive_message)
    leases.subscribe_moves(lambda source, destination, version: synced.append((source, destination, version)))
    version = {'size': 500, 'inode': 10}
    with pytest.raises(ValueError):
        leases.notify_move('old.mp4', 'new.mp4', version)
    assert synced == [('old.mp4', 'new.mp4', version)]
    assert leases.notification_failures == [{'event': 'move', 'errorType': 'ValueError'}]
