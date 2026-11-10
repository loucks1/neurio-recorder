#!/bin/sh
# Restore helper. Stop the collector first. This script does not start or stop Docker.
set -eu
: "${DB_PATH:?set DB_PATH}"
: "${LOCK_PATH:?set LOCK_PATH to collector.lock}"
: "${SRC:?set SRC to a verified backup sqlite file}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
"$PYTHON_BIN" -m neurio_collector verify-backup --src "$SRC"
"$PYTHON_BIN" -m neurio_collector restore --src "$SRC" --db "$DB_PATH" --lock "$LOCK_PATH"
"$PYTHON_BIN" -m neurio_collector verify-backup --src "$DB_PATH"
