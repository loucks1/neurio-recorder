#!/usr/bin/env python3
"""Daily online backup of a running Neurio recorder database.

Runs the collector's SQLite backup API inside the Compose service, then
verifies the copy. Older backup files are never deleted. If the copy count
or total size is already at the cap, or free space is too low, the script
exits before creating another file.

Environment:
  NEURIO_BACKUP_ROOT
      Host directory that contains data/, backups/, and backup.lock.
      Default: /srv/neurio-recorder
  NEURIO_COMPOSE_FILE
      Compose file passed to ``docker compose -f``.
      Default: $NEURIO_BACKUP_ROOT/compose.mqtt.yml
  NEURIO_COMPOSE_SERVICE
      Service name. Default: neurio-recorder
  NEURIO_CONTAINER_DB
      Database path inside the container. Default: /data/neurio.sqlite
  NEURIO_CONTAINER_BACKUP_DIR
      Backup directory inside the container. Default: /backups
  NEURIO_BACKUP_MAX_FILES
      Default: 30
  NEURIO_BACKUP_MAX_BYTES
      Default: 5 GiB
  NEURIO_BACKUP_MIN_FREE_BYTES
      Default: 2 GiB. Required free space is the greater of this value and
      twice the live database file size.
"""

from __future__ import annotations

import datetime
import fcntl
import os
import pathlib
import shutil
import subprocess


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    return int(raw)


def main() -> None:
    root = pathlib.Path(os.environ.get("NEURIO_BACKUP_ROOT", "/srv/neurio-recorder"))
    compose_file = pathlib.Path(
        os.environ.get("NEURIO_COMPOSE_FILE", str(root / "compose.mqtt.yml"))
    )
    service = os.environ.get("NEURIO_COMPOSE_SERVICE", "neurio-recorder")
    container_db = os.environ.get("NEURIO_CONTAINER_DB", "/data/neurio.sqlite")
    container_backup_dir = os.environ.get("NEURIO_CONTAINER_BACKUP_DIR", "/backups").rstrip("/")
    max_files = _int_env("NEURIO_BACKUP_MAX_FILES", 30)
    max_bytes = _int_env("NEURIO_BACKUP_MAX_BYTES", 5 * 1024**3)
    min_free = _int_env("NEURIO_BACKUP_MIN_FREE_BYTES", 2 * 1024**3)

    if not compose_file.is_file():
        raise SystemExit(f"compose file not found: {compose_file}")
    host_db = root / "data" / "neurio.sqlite"
    if not host_db.is_file():
        raise SystemExit(f"database not found: {host_db}")
    backup_dir = root / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    lock_path = root / "backup.lock"
    lock = open(lock_path, "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("another backup holds the lock; nothing deleted")

    files = list(backup_dir.glob("neurio-*.sqlite"))
    total = sum(path.stat().st_size for path in files)
    if len(files) >= max_files or total >= max_bytes:
        raise SystemExit(
            f"Backup cap reached ({max_files} copies or {max_bytes} bytes); "
            "manual archival required, nothing deleted"
        )
    db_size = host_db.stat().st_size
    if shutil.disk_usage(root).free < max(min_free, db_size * 2):
        raise SystemExit("Insufficient backup headroom; nothing deleted")

    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = f"{container_backup_dir}/neurio-daily-{stamp}.sqlite"
    base = [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "exec",
        "-T",
        service,
        "python3",
        "-m",
        "neurio_collector",
    ]
    subprocess.run(base + ["backup", "--db", container_db, "--dest", dest], check=True)
    subprocess.run(base + ["verify-backup", "--src", dest], check=True)


if __name__ == "__main__":
    main()
