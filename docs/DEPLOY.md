# Deploy neurio-recorder on CT105

`scripts/deploy.sh <sha>` runs as root on the CT105 Docker host (`docker.lan`, LAN address `192.168.86.5`). It fetches that commit, takes backups, builds a new image, recreates only the `neurio-recorder` service, waits for health, and rolls the image back if a check fails.

Nothing in CI runs this script, and it has not been exercised on CT105. Review it, then run it there (or with `--dry-run` first).

## Usage

```sh
scripts/deploy.sh --help
scripts/deploy.sh --dry-run <sha>
scripts/deploy.sh <sha>
```

`<sha>` is a 7–40 character git commit id. A full id is used as-is. A short id is resolved through the GitHub API, or through `GIT_CLONE` when that is set. The public repository is `loucks1/neurio-recorder`.

The first run can be a copy of this script on the host. Later runs can use the copy in `/opt/neurio-recorder/source-<sha>/scripts/deploy.sh`. The script does not depend on the current working directory.

`--dry-run` resolves the commit and prints the actions. It does not fetch, back up, tag, build, or recreate the container.

## What it does

1. Finds the running source directory from the Compose working-dir label on `neurio-lan-collector-neurio-recorder-1`, unless `CURRENT_SOURCE` is set.
2. Chooses the health URL (see below) and reads the live schema from `GET /api/v1/status`. If that request fails, it reads `schema_version` from the database in read-only mode and does not migrate it. When the database file exists and neither read works, the script stops before it changes the service.
3. Fetches the commit into `/opt/neurio-recorder/source-<sha>` from a GitHub tarball, with a shallow `git fetch` of that commit if the tarball fails. `SOURCE_TARBALL` or `GIT_CLONE` replaces that download. An existing complete tree for the same commit is reused and is not deleted.
4. Copies the live `compose.cloud.override.yml` into the new tree. That file is what supplies `NEURIO_CLIENT_SECRET_FILE`. The secret file it points at is not read, copied, or printed. `.env` is copied too when it is present, mode `0600`, and its contents are not printed.
5. Tags `neurio-lan-collector:local` as `neurio-lan-collector:pre-<sha>` when that rollback tag does not already exist. An existing `pre-<sha>` tag is left alone so a retry cannot replace the last known-good image.
6. Archives the current source tree to `/opt/neurio-recorder/backups/src.pre-<sha>.tgz` (mode `0600`). The archive leaves out `.git`, `.env`, SQLite files, and filenames containing `secret`, `password`, or `token`. `.env` is the separate mode-`0600` copy.
7. Runs `python -m neurio_collector backup` and then `verify-backup` in the rollback image as uid/gid `10001:10001`, with the data and backup directories mounted explicitly. The copy is `/opt/neurio-recorder/backups/neurio-pre-<sha>-<UTCts>.sqlite`. The script prints its sha256 and does not overwrite an existing backup name.
8. Builds `neurio-lan-collector:hourly-<sha>` and tags it `neurio-lan-collector:local`. The build context is the new source directory. `.dockerignore` excludes `.env`, so that file is not copied into the image.
9. Recreates only the recorder service:

```sh
docker compose --project-directory /opt/neurio-recorder/source-<sha> \
  -f /opt/neurio-recorder/source-<sha>/compose.mqtt.yml \
  -f /opt/neurio-recorder/source-<sha>/compose.cloud.override.yml \
  -p neurio-lan-collector \
  up -d --no-build --force-recreate --no-deps neurio-recorder
```

`--project-directory` is what makes this the same command as running `docker compose -f compose.mqtt.yml -f compose.cloud.override.yml -p neurio-lan-collector up -d --no-build --force-recreate --no-deps neurio-recorder` from that source tree. Compose loads `.env` from the project directory.

10. For up to 90 seconds, checks `GET /healthz`, `GET /readyz` (`ready` and `db_ok`), and `GET /api/v1/status` (`db_ok`, and `schema_version` an integer greater than or equal to the schema read in step 2). A stale hourly feed does not fail the deploy: `/readyz` stays up when `device_fresh` is false, and the script does not look at that field.
11. On a failed check, tags `pre-<sha>` back to `:local`, runs the same `up` command from the previous source directory, and prints a database restore command. It does not restore the database.

The script never removes volumes, never deletes backups, and never deletes unused images. A name that already exists gets a numeric suffix instead of being overwritten. Temp files used for the source download are removed; backup files are not.

Paths and names above are the defaults. Every one of them can be overridden with the environment variables in the next section.

## Health URL

`compose.mqtt.yml` publishes the API as `${PUBLISH_ADDRESS:-192.168.86.20}:8095:8095`. That binds host port 8095 on that one address. It does not listen on `127.0.0.1` unless `PUBLISH_ADDRESS` is `127.0.0.1`. The process inside the container binds `0.0.0.0:8095`, which is why the image's own healthcheck can use `http://127.0.0.1:8095` from inside the container network namespace. That address is not the one to use from the CT105 host.

`192.168.86.20` is the example default in the compose file and in `.env.example`. It is not this host. CT105 is `192.168.86.5`.

The script picks the URL in this order:

1. `HEALTH_BASE`, if set (for example `HEALTH_BASE=http://192.168.86.5:8095`).
2. The binding reported by `docker port <container> 8095/tcp` for the running container. A specific address is used as-is. `0.0.0.0` or `::` is the only case turned into `127.0.0.1`, because those bindings answer on loopback.
3. `PUBLISH_ADDRESS` from the environment, then from the current source `.env`. The file is not sourced; only that key is read.

If none of those is available, the script stops before it recreates the service and tells you to set `HEALTH_BASE`. It does not invent `192.168.86.20`. When a chosen URL does use `192.168.86.20`, it prints a warning because that is the compose example rather than CT105.

## Rollback and restore

Image rollback is automatic after a failed health check. The summary prints the same commands. Replace `<sha>` and the previous source path with the values from the summary:

```sh
docker tag neurio-lan-collector:pre-<sha> neurio-lan-collector:local
docker compose --project-directory <previous-source> \
  -f <previous-source>/compose.mqtt.yml \
  -f <previous-source>/compose.cloud.override.yml \
  -p neurio-lan-collector \
  up -d --no-build --force-recreate --no-deps neurio-recorder
```

Database restore is manual. Stop the recorder first so it drops `collector.lock`. Do not remove the data volume. The summary prints a `docker run` of the rollback image as `10001:10001` that executes `python -m neurio_collector restore` with the backup mounted at `/backups` and the data directory mounted at `/data`. Then start the service again with `up -d --no-build --no-deps neurio-recorder` from the source tree you intend to run.

## Environment

| Variable | Default |
|---|---|
| `APP_DIR` | `/opt/neurio-recorder` |
| `DATA_DIR` | `$APP_DIR/data` |
| `DB_PATH` | `$DATA_DIR/neurio.sqlite` |
| `LOCK_PATH` | `$DATA_DIR/collector.lock` |
| `BACKUP_DIR` | `$APP_DIR/backups` |
| `COMPOSE_PROJECT` | `neurio-lan-collector` |
| `SERVICE` | `neurio-recorder` |
| `CONTAINER_NAME` | `$COMPOSE_PROJECT-$SERVICE-1` |
| `CONTAINER_USER` | `10001:10001` |
| `IMAGE_NAME` | `neurio-lan-collector` |
| `IMAGE_TAG_PREFIX` | `hourly` |
| `COMPOSE_FILE_MAIN` | `compose.mqtt.yml` |
| `COMPOSE_FILE_OVERRIDE` | `compose.cloud.override.yml` |
| `HEALTH_BASE` | empty; discovered from Docker or `PUBLISH_ADDRESS` |
| `HEALTH_PORT` | `8095` |
| `HEALTH_WAIT_SECONDS` | `90` |
| `HEALTH_INTERVAL_SECONDS` | `5` |
| `HEALTH_CURL_MAX_TIME` | `10` (connect and total timeout, seconds, for each health request) |
| `PREVIOUS_SCHEMA` | empty; read from the live service or the database |
| `GITHUB_REPO` | `loucks1/neurio-recorder` |
| `GITHUB_TOKEN` or `GH_TOKEN` | optional, for the tarball and short-sha lookup |
| `GIT_CLONE` | optional existing git checkout |
| `SOURCE_TARBALL` | optional archive of the commit |
| `CURRENT_SOURCE` | running container's Compose working directory |

`DB_PATH` and `LOCK_PATH` have to live directly in `DATA_DIR`. The backup container mounts that directory at `/data` and `BACKUP_DIR` at `/backups`, so the copy lands on the host path even when the running service's `BACKUP_HOST_PATH` differs. The script warns when `.env` `DATA_HOST_PATH` or `BACKUP_HOST_PATH` does not match `DATA_DIR` or `BACKUP_DIR`.

When the script runs as root it sets `BACKUP_DIR` to owner `10001:10001` and mode `0750` so the backup container can write it. It does not change ownership of the data directory.

## CI

`.github/workflows/ci.yml` runs `python -m unittest discover -s tests` with Python 3.12, the version in the `Dockerfile` and in `requires-python`, on pull requests and on pushes to `main`. A second job runs shellcheck 0.10.0 on `scripts/`.
