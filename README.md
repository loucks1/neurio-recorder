# Neurio recorder

A Python service that records a Neurio/PWRview energy monitor. It reads `current-sample` data in one of three modes:

- **Device.** HTTP GET of the monitor's LAN `current-sample` URL.
- **Home Assistant.** HTTP GET of one entity, by default `sensor.neurio_raw`. Home Assistant may fill that entity from a LAN poll or from MQTT. This service only reads the entity state, and it does not change Home Assistant.
- **MQTT.** Subscribes to an ESPHome Neurio bridge (see `loucks1/esphome-neurio-openevse`) on a local MQTT broker. Raw samples arrive on `neurio/raw` and are stored exactly like device samples, with `source_kind` set to `mqtt_bridge`. Hourly energy records arrive on `neurio/hourly/energy` (QoS 1) and are stored in `hourly_records`. The recorder publishes the record id to `neurio/hourly/ack` only after the record is durably stored; a record that fails to parse or store is never acknowledged, so the bridge keeps it queued and republishes it. The MQTT client is paho-mqtt (see Dependencies below); packet framing, keepalive, and QoS 1 acknowledgement are paho's, not hand-rolled.

Dependencies: everything in this project is the Python standard library except the MQTT client, which is paho-mqtt, pinned exactly in `requirements.txt`. MQTT is a solved protocol with sharp edges (framing, keepalive, QoS handshakes), so it is a library rather than hand-written code. The Docker image pip-installs that one package at build time; paho is imported lazily, so the device and Home Assistant modes never touch it.

Readings go into SQLite. Raw JSON is kept indefinitely when `NEURIO_RAW_RETENTION_DAYS=0` (the Compose example). The read-only API serves history and CSV. Online backup uses the SQLite backup API so a copy is consistent while the collector is running.

The image runs as uid 10001 with a read-only root filesystem, all capabilities dropped, and no new privileges. The Home Assistant token is a file mounted read-only, not a value in the image or the repository.

Fixture tests are not live verification of a device.

## Data model

SQLite is in WAL mode with `synchronous=FULL`. One process holds `DATA_DIR/collector.lock`. Instants are stored in UTC. Integer watt-seconds are the energy unit. See `docs/ENERGY_RULES.md` for channels, spans, gaps, and time zones, and `docs/EXPORTS.md` for the API and CSV columns.

| Table | What it stores |
|---|---|
| `sensors` | Device `sensorId`, or `ha-relay:{entity}` in relay mode, plus a `sensor_token` (first 16 hex characters of SHA-256) used when identity is redacted |
| `channels` | Channel type, number, and stable `identity_key` |
| `samples` | Receive time, device timestamp, clock quality, payload hash, `raw_json`, and Home Assistant provenance columns |
| `sample_channels` | `p_W`, `q_VAR`, `v_V`, `eImp_Ws`, `eExp_Ws` |
| `sample_cts` | Per-CT power. Not added into household totals |
| `counter_epochs`, `energy_spans` | Counter baselines and non-negative deltas |
| `poll_attempts` | Poll outcomes. Aged out separately from samples |
| `hourly_records` | MQTT mode only. One row per bridge hourly energy record: the record id, its energy values in integer milli-kWh, the raw JSON, and the time the record was acknowledged |

A repeated payload hash is a duplicate and does not create another span. A counter decrease opens a new epoch and does not store a negative delta.

## Configuration

There is no default bind address and no default device URL. `check-config` exits 0 when the selected mode is complete and exits 2 when it is not.

`0.0.0.0` and `::` are refused unless `ALLOW_CONTAINER_WILDCARD_BIND=host-publish-pinned`. In Compose, pair that opt-in with a host publish address pinned to one IP. Do not publish `8095:8095`, which listens on every host address.

`NEURIO_POLL_INTERVAL_SECONDS` must be from 10 to 3600. The default is 15. A 5-second interval is rejected so this process cannot match a fast Home Assistant poll. In MQTT mode nothing is polled; the interval is only the base of the reconnect backoff and has no minimum.

| Variable | Required | Role |
|---|---|---|
| `SOURCE_MODE` | yes | `device`, `homeassistant`, or `mqtt` |
| `BIND_ADDRESS` | yes | IP address. No hostname |
| `BIND_PORT` | yes | 1–65535 |
| `DATA_DIR` | yes | Absolute path of the SQLite directory |
| `NEURIO_URL` | device mode | `http://` LAN URL whose path is `/current-sample`. Cloud hosts and URLs with credentials are rejected |
| `HA_URL` | relay mode | Origin only. No userinfo, path, query, or fragment |
| `HA_ENTITY` | relay mode | Entity id, such as `sensor.neurio_raw` |
| `HA_SOURCE_ID` | relay mode | Must start with `ha-relay:`. Default `ha-relay:{HA_ENTITY}` |
| `HA_TOKEN_FILE` | relay mode | Preferred. File contents are the long-lived token. Do not also set `HA_TOKEN` |
| `HA_TOKEN` | relay mode | Inline alternative to the file. Not for Compose |
| `HA_ALLOW_RFC1918_HTTP` | HTTP to RFC1918 | Must be `accept-unencrypted-bearer` or the process refuses the URL |
| `MQTT_BROKER_HOST` | mqtt mode | Broker hostname or IP address. No scheme, port, or path |
| `MQTT_BROKER_PORT` | no | Default 1883 |
| `MQTT_TLS` | no | Default `false`. TLS verifies against the system CA store |
| `MQTT_USERNAME` | broker login | Must be set together with a password, or both left unset for an anonymous login |
| `MQTT_PASSWORD_FILE` | broker login | Preferred. File contents are the broker password. Do not also set `MQTT_PASSWORD` |
| `MQTT_PASSWORD` | broker login | Inline alternative to the file |
| `MQTT_CLIENT_ID` | no | Default `neurio-recorder` |
| `MQTT_RAW_TOPIC` | no | Default `neurio/raw`. Exact topic; wildcards are rejected |
| `MQTT_HOURLY_TOPIC` | no | Default `neurio/hourly/energy`. Exact topic; wildcards are rejected |
| `MQTT_ACK_TOPIC` | no | Default `neurio/hourly/ack`. The record id is published here as bare decimal text |
| `MQTT_KEEPALIVE_SECONDS` | no | Default 60 |
| `ALLOW_CONTAINER_WILDCARD_BIND` | with `0.0.0.0` | Must be `host-publish-pinned` |
| `NEURIO_RAW_RETENTION_DAYS` | no | `0` keeps `raw_json`. A positive number clears older payloads only. **Unset means 14** |
| `NEURIO_POLL_LOG_RETENTION_DAYS` | no | Deletes aged poll-attempt rows. Default 30. Must be at least 1 |
| `NEURIO_TIMEZONE` | no | Export zone. Default `America/Chicago`. Fixed offsets are rejected |
| `NEURIO_CONSUMPTION_SOURCE` | no | `consumption_channel` (default), `none`, or `derive_net_plus_generation` |
| `NEURIO_TOPOLOGY_VERIFIED` | no | Must be `true` before `derive_net_plus_generation` is accepted. Default `false` |
| `NEURIO_GENERATION_REGISTER` | no | `unset` (default), `e_imp`, or `e_exp` |
| `NEURIO_REDACT_IDENTITY` | no | Default `true`. Omits sensor ids from the API and CSV |
| `NEURIO_POLL_TIMEOUT_SECONDS` | no | Default 3. Must be shorter than the interval in device and relay modes. In mqtt mode it is the MQTT socket timeout: greater than 0 and at most 120 |
| `NEURIO_POLL_MAX_BACKOFF_SECONDS` | no | Default 300 |
| `NEURIO_POLL_JITTER_RATIO` | no | Default 0.2 |
| `NEURIO_STALE_AFTER_SECONDS` | no | Default 60. Staleness does not fail `/readyz` |
| `NEURIO_MAX_ALLOCATABLE_GAP_SECONDS` | no | Default 45 |

Device-mode local run:

```sh
export SOURCE_MODE=device
export BIND_ADDRESS=127.0.0.1
export BIND_PORT=8095
export DATA_DIR=/absolute/path/to/data
export NEURIO_URL=http://192.168.1.50/current-sample
export NEURIO_POLL_INTERVAL_SECONDS=15
export NEURIO_RAW_RETENTION_DAYS=0
PYTHONPATH=src python3 -m neurio_collector check-config
PYTHONPATH=src python3 -m neurio_collector
```

Relay mode uses the same process. Leave `NEURIO_URL` unset. The process reads the token file once, sends it only as `Authorization: Bearer`, and strips it from stored payloads and error text.

MQTT mode uses the same process. Leave `NEURIO_URL` and the Home Assistant variables unset:

```sh
export SOURCE_MODE=mqtt
export BIND_ADDRESS=127.0.0.1
export BIND_PORT=8095
export DATA_DIR=/absolute/path/to/data
export MQTT_BROKER_HOST=192.168.1.10
PYTHONPATH=src python3 -m neurio_collector check-config
PYTHONPATH=src python3 -m neurio_collector
```

On a broker that requires a login, also set `MQTT_USERNAME` and `MQTT_PASSWORD_FILE`. The recorder subscribes to the raw topic at QoS 0 and the hourly topic at QoS 1, and reconnects with backoff when the broker drops the connection.

`/healthz` is process liveness. `/readyz` is database readiness and stays up when the source is stale. `GET /api/v1/status` reports `device_fresh` separately. Other routes: `/api/v1/latest`, `/api/v1/channels`, `/api/v1/samples`, `/api/v1/spans`, `/api/v1/energy`, and `/api/v1/export/`. The API is GET-only.

## Deploy with Docker Compose

The image has no pip install. Runtime code is the Python 3.12 standard library.

1. Create the data and backup directories and give uid 10001 write access (`chown 10001:10001`, mode such as `0750`).
2. Create the token file, mode `0600`, containing only the long-lived Home Assistant token. Compose mounts it read-only at `/run/secrets/ha_token`.
3. Copy `.env.example` to `.env` and set `PUBLISH_ADDRESS`, `HA_URL`, and the host paths. The example values are placeholders on `192.168.1.0/24`.
4. Build and start:

```sh
docker compose up -d --build
docker compose exec -T neurio-recorder python3 -m neurio_collector check-config
```

`compose.yml` keeps the process read-only, drops all capabilities, sets `no-new-privileges`, limits memory and CPU, and publishes one host address. A green healthcheck means the process answered `/healthz`. It does not mean the monitor answered. Use `/api/v1/status` and `device_fresh` for that. Do not restart the container because the device is stale.

Plain HTTP to a literal RFC1918 Home Assistant address is rejected unless `HA_ALLOW_RFC1918_HTTP=accept-unencrypted-bearer`. The flag does not allow DNS names, public addresses, link-local, or metadata addresses, and it does not turn off HTTPS certificate checks. Redirects are not followed. Environment proxies are not used.

For device mode, drop the secret, set `SOURCE_MODE=device` and `NEURIO_URL`, and do not set the Home Assistant variables.

The shipped `compose.yml` stays the Home Assistant variant; there is no Compose example for MQTT mode. To run MQTT mode in a container, adapt the service environment: drop the `ha_token` secret and the `HA_*` variables, and set `SOURCE_MODE=mqtt` with `MQTT_BROKER_HOST` (plus `MQTT_USERNAME` and a password file if the broker requires a login).

## Backup and restore

Copying `neurio.sqlite` while `neurio.sqlite-wal` is non-empty is not a consistent backup. A filesystem copy or a hypervisor snapshot is not a substitute. Use the SQLite backup API. The backup command does not delete older copies.

Inside the running container, or anywhere the `neurio_collector` module and the files are visible:

```sh
python3 -m neurio_collector backup --db /data/neurio.sqlite --dest /backups/neurio-YYYYmmddTHHMMSSZ.sqlite
python3 -m neurio_collector verify-backup --src /backups/neurio-YYYYmmddTHHMMSSZ.sqlite
```

`deploy/backup.sh` runs those two commands. Set `DB_PATH` and `BACKUP_DIR`.

Restore only after the collector is stopped:

```sh
python3 -m neurio_collector restore \
  --src /backups/neurio-YYYYmmddTHHMMSSZ.sqlite \
  --db /data/neurio.sqlite \
  --lock /data/collector.lock
python3 -m neurio_collector verify-backup --src /data/neurio.sqlite
```

`deploy/restore.sh` does the same. It does not start or stop the container. Restore refuses a held lock, refuses a non-empty WAL, and writes one `neurio.sqlite.pre-restore` safety copy. A second restore refuses while that copy is still present. Move it aside before restoring again.

`deploy/online-backup.py` is an optional host script for a daily timer. It locks, refuses to run when the backup directory already holds 30 copies or 5 GiB (or the configured caps), refuses when free space is under 2 GiB or twice the database size, then `docker compose exec`s `backup` and `verify-backup`. It never deletes an old copy. Paths come from the environment; see the script docstring. Example units are `deploy/neurio-recorder-backup.service` and `deploy/neurio-recorder-backup.timer`. Copy the script onto the host, edit the paths, then enable the timer.

Retention, which the running poller also applies about every six hours:

```sh
python3 -m neurio_collector retention --db /data/neurio.sqlite --raw-days 0 --poll-days 30
```

More detail is in `docs/OPERATIONS.md`.

## Limitations

- In Home Assistant mode the normalized device timestamp and device `sensorId` stay null. Ordering uses `last_updated`. If the entity document contains a device timestamp or sensor id, those values are kept only in `samples.raw_json`. Clearing raw JSON drops them. Channel counters are still parsed from `attributes.channels`. Short gaps can be allocated on `ha_last_updated`; they are not device-clock spans.
- `NEURIO_RAW_RETENTION_DAYS` defaults to 14 when omitted. The Compose example sets `0` so raw JSON is kept. Zero does not delete rows.
- Poll-attempt rows still age out. A full disk is reported on `/readyz` and does not trigger deletion.
- `/api/v1/status` includes static fields (`docker_image_built`, `deployed`, `lan_device_verified`) that the process does not update from the environment. Use `device_fresh`, `db_ok`, and the database to judge a running system. The observation timestamps in that payload are a recorded November 2026 channel snapshot, not a measurement this process took.
- `rateplantool_contract` stays false. This service does not implement `oauth2/token`, `users/current`, or `samples`, and it does not invent hour-boundary cumulatives. The general CSV is not a cumulative watt-second import. See `docs/RATEPLANTOOL_CONTRACT.md`.
- Household topology is unverified until you set `NEURIO_TOPOLOGY_VERIFIED=true`. The default consumption source is the dedicated `CONSUMPTION` channel.
- This service does not modify Home Assistant's own poll or MQTT configuration.

## Tests

```sh
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Synthetic payloads use sensor id `SYNTHETIC-NOT-A-DEVICE`. The observed channel fixture is labeled in its `_provenance` object.

## License

MIT. See `LICENSE`.
