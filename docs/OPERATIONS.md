# Operations

This process subscribes to one MQTT hourly topic and acknowledges a record only after it is stored. It does not poll the Neurio and it does not call Home Assistant.

## Health

`/healthz` answers when the process is up. `/readyz` answers when the database can be read and the schema version is `3`. A stale hourly feed does not fail readiness, and it does not by itself restart the container. A failure isolated to the merged-view summary (the `merge` section of `/api/v1/status`) does not fail readiness and does not clear `db_ok`. `/api/v1/status` reports `device_fresh` from the last hourly message against `NEURIO_HOURLY_STALE_AFTER_SECONDS` (default 3900).

`docker_image_built` and `deployed` in the status payload stay false. The process does not learn those from the environment.

## Migration

The first start of this version opens the database and migrates it to schema 3:

- Dropped: `sample_channels`, `sample_cts`, `energy_spans`, `counter_epochs`, `samples`, `channels`, `poll_attempts`.
- Kept, rows unchanged: `meta` (the `schema_version` value becomes `3`), `sensors`, `hourly_records`, `hourly_backfill_provenance` when that table is already there.
- `VACUUM` runs on that upgrade and reclaims the freed pages. A later start does not vacuum again unless a raw table is present.

Back up before deploying. `backup` and `verify-backup` do not migrate. `verify-backup` accepts schema versions 1, 2, and 3 so the pre-deploy copy still checks. Starting the process, or anything else that constructs `Database`, is what migrates.

## Storage

The database is SQLite in WAL mode with `synchronous=FULL`. One process holds `DATA_DIR/collector.lock`. Hourly rows are not aged out. A full disk is reported as a write failure on `/readyz` and does not delete rows.

## Backup

Use the SQLite backup API. Copying `neurio.sqlite` while `neurio.sqlite-wal` is non-empty is not a consistent backup.

```sh
python3 -m neurio_collector backup --db /data/neurio.sqlite --dest /backups/neurio-YYYYmmddTHHMMSSZ.sqlite
python3 -m neurio_collector verify-backup --src /backups/neurio-YYYYmmddTHHMMSSZ.sqlite
```

`deploy/backup.sh` calls those commands. Set `DB_PATH` and `BACKUP_DIR`.

`deploy/online-backup.py` is the optional host-side daily script. It runs the same commands with `docker compose exec` and never deletes older files. `NEURIO_COMPOSE_FILE` defaults to `compose.mqtt.yml`. `deploy/neurio-recorder-backup.service` and `deploy/neurio-recorder-backup.timer` are example systemd units. If a host unit still names `compose.yml`, change it to `compose.mqtt.yml`.

Restore, only after the recorder is stopped:

```sh
python3 -m neurio_collector restore \
  --src /backups/neurio-YYYYmmddTHHMMSSZ.sqlite \
  --db /data/neurio.sqlite \
  --lock /data/collector.lock
```

`deploy/restore.sh` does this and does not start or stop Docker. Restore refuses the lock if another process holds it, refuses a non-empty WAL, and writes one `neurio.sqlite.pre-restore` safety copy. A second restore refuses while that safety copy is still present. Move the safety copy aside before restoring again. After restore, run `verify-backup` on the destination and start the recorder. The start migrates a pre-schema-3 file.
