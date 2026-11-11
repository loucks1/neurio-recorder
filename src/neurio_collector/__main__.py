"""Command line.

The default command serves the read-only API and the MQTT hourly subscriber.
Backup, restore, and config checks do not bind a port and do not connect to
the broker.
"""

from __future__ import annotations

import argparse
import signal
import sys

from .backup import BackupError, online_backup, restore_backup, verify_backup
from .config import Config, ConfigError
from .db import Database
from .locking import InstanceLock, LockError
from .poller import Poller


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        return _serve()
    command = argv[0]
    parser = argparse.ArgumentParser(prog="neurio_collector")
    sub = parser.add_subparsers(dest="command")
    backup = sub.add_parser("backup")
    backup.add_argument("--db", required=True)
    backup.add_argument("--dest", required=True)
    restore = sub.add_parser("restore")
    restore.add_argument("--src", required=True)
    restore.add_argument("--db", required=True)
    restore.add_argument("--lock", required=True)
    verify = sub.add_parser("verify-backup")
    verify.add_argument("--src", required=True)
    sub.add_parser("check-config")
    parsed = parser.parse_args(argv)
    try:
        if parsed.command == "backup":
            online_backup(parsed.db, parsed.dest)
            verify_backup(parsed.dest)
            print(parsed.dest)
            return 0
        if parsed.command == "verify-backup":
            verify_backup(parsed.src)
            print("ok")
            return 0
        if parsed.command == "restore":
            restore_backup(parsed.src, parsed.db, parsed.lock)
            print(parsed.db)
            return 0
        if parsed.command == "check-config":
            Config.from_env().validate()
            print("config ok")
            return 0
    except (ConfigError, BackupError, LockError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    parser.error(f"unknown command {command}")
    return 2


def _serve() -> int:
    try:
        cfg = Config.from_env()
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    try:
        lock = InstanceLock(cfg.lock_path)
    except LockError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    db = Database(cfg.db_path)
    poller = Poller(cfg, db)
    from .api import make_server

    server = make_server(cfg, db, poller)

    def stop(signum, _frame):
        poller.stop()
        server.shutdown()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    poller.start()
    try:
        server.serve_forever()
    finally:
        poller.stop()
        server.server_close()
        db.close()
        lock.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
