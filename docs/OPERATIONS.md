# Operations

This process polls one configured source. Device mode fetches `NEURIO_URL`. Home Assistant mode fetches `GET {HA_URL}/api/states/{HA_ENTITY}` and does not call the Neurio. It does not change Home Assistant, MQTT, firewall rules, or the monitor. This collector's minimum interval is 10 seconds and its default is 15, so it cannot be configured to a 5-second poll.

## Endpoint

Device mode requires an explicit `NEURIO_URL`. There is no default, so an unconfigured process does not probe the network. An example is:

`http://192.168.1.50:80/current-sample`

The same host without an explicit port, `http://192.168.1.50/current-sample`, is the same HTTP endpoint. Cloud hosts and URLs with credentials are rejected. Redirects are not followed. HTTP 401 is recorded and not retried with a guessed password.

Home Assistant mode requires `HA_URL`, `HA_ENTITY`, and either `HA_TOKEN_FILE` or `HA_TOKEN`. The example origin is `http://192.168.1.10:8123`. Plain HTTP to a literal RFC1918 address also needs `HA_ALLOW_RFC1918_HTTP=accept-unencrypted-bearer`. DNS names are rejected on HTTP. HTTPS certificate checks stay on.

## Health

`/healthz` answers when the process is up. `/readyz` answers when the database can be read. A stale or unreachable source does not fail readiness, and it does not by itself restart the container. `/api/v1/status` reports `device_fresh` separately from `process_ok` and `db_ok`. Device mode, Home Assistant mode, and MQTT mode with a raw topic compare the last raw-sample success with `NEURIO_STALE_AFTER_SECONDS` (default 60). Hourly-only MQTT mode (`MQTT_RAW_TOPIC` empty) compares the last hourly message with `NEURIO_HOURLY_STALE_AFTER_SECONDS` (default 3900).

The status payload repeats a November 6, 2026 recorder snapshot as historical context: last valid net at `2026-11-06T19:30:50-06:00`, unavailable at `2026-11-06T19:31:06-06:00`. Those times are not a measurement taken by this process. Static fields such as `docker_image_built` and `deployed` are not updated from the environment.

## Storage

`NEURIO_RAW_RETENTION_DAYS=0` keeps `raw_json`. A positive number nulls older raw payloads only. The sample row, integer counters, epochs, spans, and quality flags stay. Poll-attempt rows are deleted after `NEURIO_POLL_LOG_RETENTION_DAYS` (default 30). If the variable is omitted, raw retention defaults to 14 days. A full disk is reported as a write failure on `/readyz` and does not trigger deletion.

The database is SQLite in WAL mode with `synchronous=FULL`. One process holds `DATA_DIR/collector.lock`.

## Backup

Use the SQLite backup API. Copying `neurio.sqlite` while `neurio.sqlite-wal` is non-empty is not a consistent backup. A filesystem copy or a hypervisor snapshot of the container is not a substitute.

From a machine that can see the volume, with the collector running:

```sh
python3 -m neurio_collector backup --db /data/neurio.sqlite --dest /backups/neurio-YYYYmmddTHHMMSSZ.sqlite
python3 -m neurio_collector verify-backup --src /backups/neurio-YYYYmmddTHHMMSSZ.sqlite
```

`verify-backup` runs `PRAGMA integrity_check` and checks `schema_version`.

`deploy/backup.sh` calls those commands. Set `DB_PATH` and `BACKUP_DIR`.

`deploy/online-backup.py` is the optional host-side daily script. It runs the same commands with `docker compose exec` and never deletes older files. Paths, the compose file, the service name, and the size caps are environment variables. `deploy/neurio-recorder-backup.service` and `deploy/neurio-recorder-backup.timer` are example systemd units.

Restore, only after the collector is stopped:

```sh
python3 -m neurio_collector restore \
  --src /backups/neurio-YYYYmmddTHHMMSSZ.sqlite \
  --db /data/neurio.sqlite \
  --lock /data/collector.lock
```

`deploy/restore.sh` does this and does not start or stop Docker. Restore refuses the lock if another process holds it, refuses a non-empty WAL, and writes one `neurio.sqlite.pre-restore` safety copy. A second restore refuses while that safety copy is still present. Move the safety copy aside before restoring again. After restore, run `verify-backup` on the destination and start the collector. Confirm sample counts and that a replay of the last payload does not add a second energy span.

Keep backup files on a path the operator chooses. The backup command does not delete older copies.

## Retention command

```sh
python3 -m neurio_collector retention --db /data/neurio.sqlite --raw-days 0 --poll-days 30
```

The running poller also applies retention about every six hours. Pass the same raw-day value the process was started with. `--raw-days 0` keeps raw JSON.
