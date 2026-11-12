# Neurio recorder

A Python service that stores hourly energy records from an ESPHome Neurio bridge (see `loucks1/esphome-neurio-openevse`). It subscribes to `neurio/hourly/energy` at QoS 1 and publishes the record id to `neurio/hourly/ack` only after the row is durable. A record that fails to parse or store is never acknowledged, so the bridge keeps it queued. The MQTT session is persistent (`clean_session` false). The client is paho-mqtt, pinned in `requirements.txt`.

The process does not poll the monitor, does not read Home Assistant, and does not subscribe to a raw sample topic.

Readings go into SQLite. The read-only API serves hourly history. Online backup uses the SQLite backup API so a copy is consistent while the recorder is running.

The image runs as uid 10001 with a read-only root filesystem, all capabilities dropped, and no new privileges. The broker password is a file mounted read-only, not a value in the image or the repository.

Fixture tests are not live verification of a device.

## Data model

SQLite is in WAL mode with `synchronous=FULL`. One process holds `DATA_DIR/collector.lock`. Instants are stored in UTC. Hourly energy values are integer milli-kWh.

| Table | What it stores |
|---|---|
| `meta` | `schema_version` (currently `3`) and any other keys already present |
| `sensors` | The sensor id and `sensor_token` already recorded. Kept so `GET /api/v1/channels` can return `sensor_id`. The token is not served. New hourly records do not add rows |
| `hourly_records` | One row per bridge hourly energy record: the record id, energy values in integer milli-kWh, the raw JSON, and the time the record was acknowledged |
| `hourly_backfill_provenance` | Left untouched when it already exists. This process does not create or modify it |
| `cloud_hourly_records` | Neurio cloud hourly stats, keyed by the hour's end. Created by cloud ingest on first use; absent until then |
| `cloud_sync_state` | Cloud ingest progress: harvested range, last attempt/success/error. Created with `cloud_hourly_records` |

Opening the database migrates it to schema 3. The migration drops `sample_channels`, `sample_cts`, `energy_spans`, `counter_epochs`, `samples`, `channels`, and `poll_attempts`, then runs `VACUUM` when it actually dropped tables or the schema version was not yet 3. It does not change rows in `hourly_records`, `hourly_backfill_provenance`, or `sensors`. Running it again is a no-op aside from rewriting `schema_version` to `3`.

`poll_attempts` was an attempt log. MQTT mode appended a row for each hourly message and connection error, and `/api/v1/status` used to read the latest row. Freshness and storage do not depend on it: the running process keeps the same fields in memory, and after a restart freshness comes from `hourly_records.received_at_utc`. The table is dropped and nothing writes it.

## API

`/healthz` is process liveness. `/readyz` is database readiness and stays up when the hourly feed is stale. The API is GET-only.

`GET /api/v1/hourly?limit=168&cursor=<record_id>` pages `hourly_records` by `record_id`. `limit` defaults to 100 and must be from 1 to 1000. `cursor` defaults to 0 and is exclusive (`record_id` greater than the cursor). The response is `{"hourly_records": [...], "next_cursor": "<id>" | null}`. Each record has `record_id`, `device_timestamp_raw`, `device_timestamp_utc`, `retries`, `received_at_utc`, `acked_at_utc`, and the kWh strings `net_import_kwh`, `net_export_kwh`, `net_delta_net_kwh`, `generation_delta_export_kwh`, `consumption_delta_import_kwh`, `phase_a_delta_import_kwh`, and `phase_b_delta_import_kwh`.

`GET /api/v1/channels` still returns `{"channels": [...]}`. Each preserved `sensors` row is one object with `identity_key`, `type`, `ch`, `first_seen_utc`, `last_seen_utc`, `reading_notice`, `label`, and `sensor_id` (omitted only for the historical missing-id placeholder). `sensor_id` is the stored id. `sensor_token` is not included. `type`, `ch`, `identity_key`, and `label` are null because those columns lived on `channels`, which is dropped. rate-plan-tool reads this route when it does not already know the sensor id.

`GET /api/v1/status` reports `device_fresh` from the last hourly message. A running process uses the subscriber's last hourly message time. After a restart, before this process has seen a message, it uses the newest `hourly_records.received_at_utc`. The threshold is `NEURIO_HOURLY_STALE_AFTER_SECONDS` (default 3900). Status counts hourly records only. It also carries a `cloud_ingest` section — configured, harvested range, last sync attempt/success/error, hours stored, newest cloud hour — which reports an empty, unconfigured state until cloud ingest is enabled and has run.

`/api/v1/energy`, `/api/v1/samples`, `/api/v1/spans`, `/api/v1/latest`, and the CSV exports are gone.

## Configuration

There is no default bind address. `check-config` exits 0 when the configuration is complete and exits 2 when it is not.

`0.0.0.0` and `::` are refused unless `ALLOW_CONTAINER_WILDCARD_BIND=host-publish-pinned`. In Compose, pair that opt-in with a host publish address pinned to one IP.

`SOURCE_MODE` must be `mqtt`. `NEURIO_URL`, Home Assistant variables, and a non-empty `MQTT_RAW_TOPIC` are rejected. An empty `MQTT_RAW_TOPIC` is ignored so an older compose file that set it to empty still starts.

`NEURIO_POLL_INTERVAL_SECONDS` is the base of the reconnect backoff. It must be greater than 0 and at most 3600. Nothing is polled.

| Variable | Required | Role |
|---|---|---|
| `SOURCE_MODE` | yes | `mqtt` |
| `BIND_ADDRESS` | yes | IP address. No hostname |
| `BIND_PORT` | yes | 1–65535 |
| `DATA_DIR` | yes | Absolute path of the SQLite directory |
| `MQTT_BROKER_HOST` | yes | Broker hostname or IP. No scheme, port, or path |
| `MQTT_BROKER_PORT` | no | Default 1883 |
| `MQTT_TLS` | no | Default `false`. TLS verifies against the system CA store |
| `MQTT_USERNAME` | broker login | Must be set together with a password, or both left unset |
| `MQTT_PASSWORD_FILE` | broker login | Preferred. File contents are the broker password. Do not also set `MQTT_PASSWORD` |
| `MQTT_PASSWORD` | broker login | Inline alternative to the file |
| `MQTT_CLIENT_ID` | no | Default `neurio-recorder` |
| `MQTT_HOURLY_TOPIC` | no | Default `neurio/hourly/energy`. Exact topic; wildcards are rejected |
| `MQTT_ACK_TOPIC` | no | Default `neurio/hourly/ack`. The record id is published here as bare decimal text |
| `MQTT_KEEPALIVE_SECONDS` | no | Default 60 |
| `ALLOW_CONTAINER_WILDCARD_BIND` | with `0.0.0.0` | Must be `host-publish-pinned` |
| `NEURIO_HOURLY_STALE_AFTER_SECONDS` | no | Default 3900. Does not fail `/readyz` |
| `NEURIO_POLL_INTERVAL_SECONDS` | no | Default 15. Reconnect backoff base |
| `NEURIO_POLL_TIMEOUT_SECONDS` | no | Default 3. MQTT socket timeout, greater than 0 and at most 120 |
| `NEURIO_POLL_MAX_BACKOFF_SECONDS` | no | Default 300 |
| `NEURIO_POLL_JITTER_RATIO` | no | Default 0.2 |
| `NEURIO_MAX_RESPONSE_BYTES` | no | Default 1000000. Oversize MQTT payloads are dropped |

```sh
export SOURCE_MODE=mqtt
export BIND_ADDRESS=127.0.0.1
export BIND_PORT=8095
export DATA_DIR=/absolute/path/to/data
export MQTT_BROKER_HOST=192.168.1.10
PYTHONPATH=src python3 -m neurio_collector check-config
PYTHONPATH=src python3 -m neurio_collector
```

On a broker that requires a login, also set `MQTT_USERNAME` and `MQTT_PASSWORD_FILE`.

## Cloud ingest

Alongside the MQTT hourly records, the recorder can pull hourly energy history from the Neurio cloud, so the cloud and local records sit side by side in one database ahead of the cloud's eventual shutdown. Set `NEURIO_CLIENT_ID` and `NEURIO_CLIENT_SECRET` (or `NEURIO_CLIENT_SECRET_FILE`) to enable it; with neither set, nothing about the process changes. The connection is outbound-only and independent of the MQTT source.

The syncer authenticates with the OAuth client-credentials grant, discovers the sensor id from `/v1/users/current` (or takes `NEURIO_CLOUD_SENSOR_ID`), and reads `GET /v1/samples/stats` at `granularity=hours`. Those values are per-hour interval watt-seconds: they are stored as-is (never differenced), converted to milli-kWh, and keyed by the hour's end in the dedicated `cloud_hourly_records` table — separate from the bridge's `hourly_records`, so a cloud value and an ESP32 value for the same hour both survive. Each row keeps the time it was first fetched, which doubles as the cloud-lateness measurement.

Two jobs run on a `NEURIO_CLOUD_SYNC_INTERVAL_SECONDS` cadence (default 3600): a one-time **harvest** that backfills from `NEURIO_CLOUD_HARVEST_FROM` (default 2016-01-01Z) to now in 10-day chunks, persisting its progress after each chunk so a restart resumes instead of restarting; and a **tail sync** that re-fetches the trailing `NEURIO_CLOUD_TAIL_HOURS` (default 48) every cycle, so late or corrected cloud values replace the earlier ones in place. Cloud failures appear in the `cloud_ingest` section of `/api/v1/status` and nowhere else — they never affect the MQTT subscriber or the read API. `NEURIO_CLOUD_API_BASE` defaults to `https://api.neur.io/v1` and accepts plain http only for a loopback host, so a local test server can stand in for the cloud.

## Deploy with Docker Compose

`compose.mqtt.yml` is the deployment. It sets `SOURCE_MODE=mqtt`, `MQTT_HOURLY_TOPIC=neurio/hourly/energy`, and `MQTT_ACK_TOPIC=neurio/hourly/ack`.

Broker host and port come from the environment, defaulting to `192.168.86.7` and `1883`. `MQTT_USERNAME` is required. The broker password is the Compose file secret `mqtt_password`: the host file `MQTT_PASSWORD_HOST_FILE` (default `/srv/neurio-recorder/mqtt_password`) is mounted read-only at `/run/secrets/mqtt_password`.

The same `.env` keys as the current production file are interpolated: `PUBLISH_ADDRESS`, `DATA_HOST_PATH`, `BACKUP_HOST_PATH`, `MQTT_BROKER_HOST`, `MQTT_USERNAME`, and `MQTT_PASSWORD_HOST_FILE`.

Startup migrates the live database. Back it up first. The migration drops the raw tables listed above and runs `VACUUM`. Hourly rows, provenance rows, and the sensor row stay.

1. Back up the existing database from the running container:

```sh
docker compose -f compose.mqtt.yml exec -T neurio-recorder python3 -m neurio_collector backup --db /data/neurio.sqlite --dest /backups/neurio-YYYYmmddTHHMMSSZ.sqlite
docker compose -f compose.mqtt.yml exec -T neurio-recorder python3 -m neurio_collector verify-backup --src /backups/neurio-YYYYmmddTHHMMSSZ.sqlite
```

Take that backup with the current image if you still need the raw tables. `verify-backup` on this version accepts schema versions 1, 2, and 3, and it does not migrate the file it checks. Starting this version is what migrates the live database.

2. Create the data and backup directories if they are not already there, and give uid 10001 write access (`chown 10001:10001`, mode such as `0750`).
3. Create the password file, mode `0600`, containing only the broker password.
4. In `.env`, set `PUBLISH_ADDRESS`, `MQTT_USERNAME`, and any overrides.
5. Build and start:

```sh
docker compose -f compose.mqtt.yml up -d --build
docker compose -f compose.mqtt.yml exec -T neurio-recorder python3 -m neurio_collector check-config
```

`/healthz` means the process is up. `device_fresh` is the hourly-message check. A full disk is reported on `/readyz` and does not delete hourly rows.

If a backup timer still points `NEURIO_COMPOSE_FILE` at `compose.yml`, point it at `compose.mqtt.yml`. The example unit and the default in `deploy/online-backup.py` already do.

## Backup and restore

Copying `neurio.sqlite` while `neurio.sqlite-wal` is non-empty is not a consistent backup. Use the SQLite backup API. The backup command does not delete older copies and does not migrate the source.

```sh
python3 -m neurio_collector backup --db /data/neurio.sqlite --dest /backups/neurio-YYYYmmddTHHMMSSZ.sqlite
python3 -m neurio_collector verify-backup --src /backups/neurio-YYYYmmddTHHMMSSZ.sqlite
```

`deploy/backup.sh` runs those two commands. Set `DB_PATH` and `BACKUP_DIR`.

Restore only after the recorder is stopped:

```sh
python3 -m neurio_collector restore \
  --src /backups/neurio-YYYYmmddTHHMMSSZ.sqlite \
  --db /data/neurio.sqlite \
  --lock /data/collector.lock
```

`deploy/restore.sh` does the same. Restore refuses a held lock, refuses a non-empty WAL, and writes one `neurio.sqlite.pre-restore` safety copy. A second restore refuses while that copy is still present. Restoring a schema 2 backup and then starting this version migrates that restored file and drops its raw tables. The backup file itself is unchanged.

`deploy/online-backup.py` is an optional host script for a daily timer. It never deletes an old copy. Example units are `deploy/neurio-recorder-backup.service` and `deploy/neurio-recorder-backup.timer`.

## Tests

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## License

MIT. See `LICENSE`.
