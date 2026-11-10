#!/bin/sh
# Online backup helper. The collector may keep running. Set DB_PATH and BACKUP_DIR.
set -eu
: "${DB_PATH:?set DB_PATH to the live neurio.sqlite}"
: "${BACKUP_DIR:?set BACKUP_DIR to the backup directory}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
dest="$BACKUP_DIR/neurio-$stamp.sqlite"
"$PYTHON_BIN" -m neurio_collector backup --db "$DB_PATH" --dest "$dest"
"$PYTHON_BIN" -m neurio_collector verify-backup --src "$dest"
printf '%s\n' "$dest"
