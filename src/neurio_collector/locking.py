"""Exclusive lock so a second collector or a restore cannot use the same data directory."""

from __future__ import annotations

import fcntl
import os


class LockError(RuntimeError):
    pass


class InstanceLock:
    def __init__(self, path: str):
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.path = path
        self.fp = open(path, "a+")
        try:
            fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.fp.close()
            raise LockError("collector lock is held; stop the other process before continuing") from exc
        self.fp.seek(0)
        self.fp.truncate()
        self.fp.write(str(os.getpid()))
        self.fp.flush()

    def close(self) -> None:
        try:
            fcntl.flock(self.fp.fileno(), fcntl.LOCK_UN)
        finally:
            self.fp.close()
