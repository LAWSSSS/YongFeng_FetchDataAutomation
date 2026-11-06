"""Run one direct filesystem transfer in a disposable process."""
import errno
import json
import stat
import sys
from pathlib import Path

from .media import copy_to_destination, fingerprint, transfer_reservation_identity, verify_publication


def emit(event):
    print(json.dumps(event, ensure_ascii=False), flush=True)


def main():
    source, destination = map(Path, sys.argv[1:3])
    recover = len(sys.argv) > 3 and sys.argv[3] == "--recover"
    reservation = json.loads(sys.argv[4]) if len(sys.argv) > 4 and sys.argv[4] else None
    prepared = json.loads(sys.argv[5]) if len(sys.argv) > 5 else {}
    stage = "target_recovery" if recover else "target_lookup"
    try:
        emit({"stage": stage})
        try:
            existing = destination.lstat()
        except FileNotFoundError:
            if recover:
                raise
            pass
        else:
            verified_source = prepared.get('sourceVersion') or {}
            same_file = (bool(verified_source) and stat.S_ISREG(existing.st_mode)
                         and all(verified_source.get(key) == value for key, value in fingerprint(existing).items()))
            if same_file or recover and not reservation:
                version = verify_publication(destination, prepared)
                if same_file and not recover:
                    # A restart can find both names if the link succeeded before the unlink.
                    verify_publication(source, prepared, verified_source)
                emit({'stage': 'source_remove', 'fileVersion': version})
                if same_file and not recover:
                    source.unlink()
                emit({"event": "complete"})
                return 0
            if reservation:
                transfer_reservation_identity(destination, reservation)
                if recover:
                    raise OSError(errno.EINVAL, "目标文件尚未发布。")
            else:
                raise FileExistsError(errno.EEXIST, "目标文件名已存在。")
        old = None
        if len(sys.argv) > 3 and sys.argv[3] and not recover:
            old = Path(sys.argv[3])
            if (old.parent != destination.parent or not old.name.startswith("." + destination.name + ".")
                    or not old.name.endswith(".uploading")):
                raise OSError(errno.EINVAL, "未登记的转移临时文件。")
        copy_to_destination(source, destination, emit, partial_path=old, keep_partial=True, reservation=reservation,
                            verified_source=prepared.get('sourceVersion'), expected_digest=prepared.get('sha256'))
        emit({"event": "complete"})
        return 0
    except OSError as error:
        emit({"event": "error", "errno": error.errno,
              "code": errno.errorcode.get(error.errno, "UNKNOWN"),
              "type": type(error).__name__, "stage": getattr(error, "transfer_stage", stage)})
        return 1
    except Exception:
        emit({"event": "error", "errno": errno.EIO, "code": "EIO",
              "type": "TransferWorkerError", "stage": stage})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
