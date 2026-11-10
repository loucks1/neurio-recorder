"""SQLite online backup and restore.

Copying a live database file while a WAL exists is not a consistent backup.
These helpers use the SQLite backup API. Restore refuses to run while the
collector lock is held or while a non-empty WAL is still beside the destination.
A filesystem copy or hypervisor snapshot of the container is not a substitute.
"""

from __future__ import annotations

import os
import sqlite3

from .locking import InstanceLock, LockError


class BackupError(RuntimeError):
    pass


def online_backup(src_path: str, dest_path: str) -> None:
    if os.path.exists(dest_path):
        raise BackupError("backup destination already exists; refusing to overwrite")
    parent = os.path.dirname(dest_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    source = sqlite3.connect(src_path, timeout=5)
    dest = sqlite3.connect(dest_path)
    try:
        source.backup(dest)
        check = dest.execute("PRAGMA integrity_check").fetchone()
        if check is None or check[0] != "ok":
            raise BackupError("backup integrity_check failed")
        version = dest.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()
        if version is None:
            raise BackupError("backup is missing schema_version")
    except Exception:
        dest.close()
        source.close()
        if os.path.exists(dest_path):
            os.remove(dest_path)
        raise
    else:
        dest.close()
        source.close()


def verify_backup(path: str) -> None:
    if not os.path.exists(path):
        raise BackupError("backup file is missing")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        check = conn.execute("PRAGMA integrity_check").fetchone()
        if check is None or check[0] != "ok":
            raise BackupError("integrity_check failed")
        version = conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()
        if version is None or version[0] not in {"1", "2"}:
            raise BackupError("unexpected schema_version")
    finally:
        conn.close()


def restore_backup(src_path: str, dest_path: str, lock_path: str) -> None:
    verify_backup(src_path)
    try:
        lock = InstanceLock(lock_path)
    except LockError as exc:
        raise BackupError(
            "collector lock is held. Stop the collector before restore."
        ) from exc
    try:
        wal = dest_path + "-wal"
        if os.path.exists(wal) and os.path.getsize(wal) > 0:
            raise BackupError(
                "destination WAL is non-empty. Stop the collector and checkpoint before restore."
            )
        safety = dest_path + ".pre-restore"
        if os.path.exists(dest_path):
            if os.path.exists(safety):
                raise BackupError(
                    "a pre-restore safety copy already exists. Move it aside before restoring again."
                )
            online_backup(dest_path, safety)
        temporary = dest_path + ".restore-tmp"
        if os.path.exists(temporary):
            os.remove(temporary)
        online_backup(src_path, temporary)
        os.replace(temporary, dest_path)
        for suffix in ("-wal", "-shm"):
            sidecar = dest_path + suffix
            if os.path.exists(sidecar) and os.path.getsize(sidecar) == 0:
                os.remove(sidecar)
    finally:
        lock.close()
