"""Run one direct filesystem transfer in a disposable process."""
import errno
import json
import stat
import sys
from pathlib import Path

from .media import copy_to_destination, transfer_reservation_identity


def emit(event):
    print(json.dumps(event, ensure_ascii=False), flush=True)


def main():
    source, destination = map(Path, sys.argv[1:3])
    recover = len(sys.argv) > 3 and sys.argv[3] == "--recover"
    reservation = json.loads(sys.argv[4]) if len(sys.argv) > 4 else None
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
            if reservation:
                transfer_reservation_identity(destination, reservation)
                if recover:
                    raise OSError(errno.EINVAL, "目标文件尚未发布。")
            elif recover:
                if not stat.S_ISREG(existing.st_mode):
                    raise OSError(errno.EINVAL, "保存路径不是普通文件。")
                emit({"event": "complete"})
                return 0
            else:
                raise FileExistsError(errno.EEXIST, "目标文件名已存在。")
        old = None
        if len(sys.argv) > 3 and sys.argv[3] and not recover:
            old = Path(sys.argv[3])
            if (old.parent != destination.parent or not old.name.startswith("." + destination.name + ".")
                    or not old.name.endswith(".uploading")):
                raise OSError(errno.EINVAL, "未登记的转移临时文件。")
        copy_to_destination(source, destination, emit, partial_path=old, keep_partial=True, reservation=reservation)
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
