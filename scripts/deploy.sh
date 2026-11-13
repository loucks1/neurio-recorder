#!/usr/bin/env bash
# Deploy one git commit of neurio-recorder on the CT105 Docker host.
#
# Run as root on CT105 (docker.lan, 192.168.86.5). The script fetches that
# commit, keeps a rollback image and a consistent SQLite backup, builds
# neurio-lan-collector:hourly-<sha>, points :local at it, and recreates only
# the neurio-recorder service. If health checks fail, it points :local back
# at the previous image and recreates the service from the previous source
# tree. It does not restore the database.
#
# Volume removal, backup deletion, and unused-image deletion are intentionally
# absent. See docs/DEPLOY.md.
#
# Usage: scripts/deploy.sh [--dry-run] <sha>
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/deploy.sh [--dry-run] <sha>

Deploy neurio-recorder at the given git commit on this host (CT105).

  --dry-run   Print the actions without fetching, backing up, building,
              tagging, or recreating the container.

Environment (defaults shown):
  APP_DIR=/opt/neurio-recorder
  DATA_DIR=$APP_DIR/data
  DB_PATH=$DATA_DIR/neurio.sqlite
  LOCK_PATH=$DATA_DIR/collector.lock
  BACKUP_DIR=$APP_DIR/backups
  COMPOSE_PROJECT=neurio-lan-collector
  SERVICE=neurio-recorder
  CONTAINER_NAME=$COMPOSE_PROJECT-$SERVICE-1
  CONTAINER_USER=10001:10001
  IMAGE_NAME=neurio-lan-collector
  IMAGE_TAG_PREFIX=hourly
  COMPOSE_FILE_MAIN=compose.mqtt.yml
  COMPOSE_FILE_OVERRIDE=compose.cloud.override.yml
  HEALTH_BASE=            empty: use the address Docker published, else
                          PUBLISH_ADDRESS from the environment or .env
  HEALTH_PORT=8095        container port passed to `docker port`
  HEALTH_WAIT_SECONDS=90
  HEALTH_INTERVAL_SECONDS=5
  HEALTH_CURL_MAX_TIME=10
  PREVIOUS_SCHEMA=        empty: read from the live /api/v1/status, else
                          a read-only query of DB_PATH (no migration)
  GITHUB_REPO=loucks1/neurio-recorder
  GITHUB_TOKEN / GH_TOKEN optional; sent only as a curl Authorization header
  GIT_CLONE               optional existing checkout used to archive <sha>
  SOURCE_TARBALL          optional pre-downloaded archive of <sha>
  CURRENT_SOURCE          optional; otherwise the running container's
                          Compose working directory
EOF
}

log() { printf '%s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

DRY_RUN=0
SHA_ARG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; break ;;
    -*) die "unknown option: $1 (see --help)" ;;
    *) SHA_ARG="$1"; shift; break ;;
  esac
done
[[ $# -eq 0 ]] || die "unexpected extra arguments: $*"
[[ -n "$SHA_ARG" ]] || { usage >&2; exit 2; }

APP_DIR="${APP_DIR:-/opt/neurio-recorder}"
DATA_DIR="${DATA_DIR:-$APP_DIR/data}"
DB_PATH="${DB_PATH:-$DATA_DIR/neurio.sqlite}"
LOCK_PATH="${LOCK_PATH:-$DATA_DIR/collector.lock}"
BACKUP_DIR="${BACKUP_DIR:-$APP_DIR/backups}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-neurio-lan-collector}"
SERVICE="${SERVICE:-neurio-recorder}"
IMAGE_NAME="${IMAGE_NAME:-neurio-lan-collector}"
IMAGE_TAG_PREFIX="${IMAGE_TAG_PREFIX:-hourly}"
COMPOSE_FILE_MAIN="${COMPOSE_FILE_MAIN:-compose.mqtt.yml}"
COMPOSE_FILE_OVERRIDE="${COMPOSE_FILE_OVERRIDE:-compose.cloud.override.yml}"
HEALTH_BASE="${HEALTH_BASE:-}"
HEALTH_PORT="${HEALTH_PORT:-8095}"
HEALTH_WAIT_SECONDS="${HEALTH_WAIT_SECONDS:-90}"
HEALTH_INTERVAL_SECONDS="${HEALTH_INTERVAL_SECONDS:-5}"
HEALTH_CURL_MAX_TIME="${HEALTH_CURL_MAX_TIME:-10}"
PREVIOUS_SCHEMA="${PREVIOUS_SCHEMA:-}"
GITHUB_REPO="${GITHUB_REPO:-loucks1/neurio-recorder}"
CONTAINER_NAME="${CONTAINER_NAME:-${COMPOSE_PROJECT}-${SERVICE}-1}"
CONTAINER_USER="${CONTAINER_USER:-10001:10001}"

case "$APP_DIR" in /*) ;; *) die "APP_DIR must be an absolute path" ;; esac
case "$DATA_DIR" in /*) ;; *) die "DATA_DIR must be an absolute path" ;; esac
case "$DB_PATH" in /*) ;; *) die "DB_PATH must be an absolute path" ;; esac
case "$LOCK_PATH" in /*) ;; *) die "LOCK_PATH must be an absolute path" ;; esac
case "$BACKUP_DIR" in /*) ;; *) die "BACKUP_DIR must be an absolute path" ;; esac
[[ "$HEALTH_PORT" =~ ^[0-9]+$ ]] || die "HEALTH_PORT must be an integer"
[[ "$HEALTH_WAIT_SECONDS" =~ ^[0-9]+$ ]] || die "HEALTH_WAIT_SECONDS must be a non-negative integer"
[[ "$HEALTH_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || die "HEALTH_INTERVAL_SECONDS must be a non-negative integer"
[[ "$HEALTH_CURL_MAX_TIME" =~ ^[0-9]+$ ]] || die "HEALTH_CURL_MAX_TIME must be a positive integer"
[[ "$HEALTH_CURL_MAX_TIME" -ge 1 ]] || die "HEALTH_CURL_MAX_TIME must be a positive integer"
[[ "$CONTAINER_USER" =~ ^[0-9]+:[0-9]+$ ]] || die "CONTAINER_USER must be numeric uid:gid"
[[ "$IMAGE_NAME" =~ ^[A-Za-z0-9_.-]+$ ]] || die "IMAGE_NAME must be a single docker image name"
[[ "$IMAGE_TAG_PREFIX" =~ ^[A-Za-z0-9_.-]+$ ]] || die "IMAGE_TAG_PREFIX must be a docker tag token"
[[ "$GITHUB_REPO" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || die "GITHUB_REPO must look like owner/name"
case "$COMPOSE_FILE_MAIN" in */*) die "COMPOSE_FILE_MAIN must be a file name, not a path" ;; esac
case "$COMPOSE_FILE_OVERRIDE" in */*) die "COMPOSE_FILE_OVERRIDE must be a file name, not a path" ;; esac
if [[ "$(dirname "$DB_PATH")" != "$DATA_DIR" ]]; then
  die "DB_PATH must be a file inside DATA_DIR so it can be mounted at /data"
fi
if [[ "$(dirname "$LOCK_PATH")" != "$DATA_DIR" ]]; then
  die "LOCK_PATH must be a file inside DATA_DIR so it can be mounted at /data"
fi

# Populated by helpers. Declared up front so the exit summary can run
# under `set -u` even when a step fails early.
SHA=""
NEW_SOURCE=""
CURRENT_SOURCE=""
DB_BACKUP_PATH=""
DB_BACKUP_SHA256=""
SRC_TAR_PATH=""
ENV_BACKUP_PATH=""
IMAGE_LOCAL_ID=""
IMAGE_NEW_ID=""
IMAGE_PRE_ID=""
HEALTH_URL=""
HEALTH_RESULT="not run"
ROLLED_BACK="no"
OBSERVED_SCHEMA=""
RECREATED=0
SUMMARY_PRINTED=0
TMP_FETCH=""
LAST_HEALTH_ERROR=""
COMPOSE_ARGV=()
DOCKER_PY_ARGV=()

# Reached from print_summary, which the EXIT trap calls. Shellcheck does
# not count that trap as a reference (SC2317).
# shellcheck disable=SC2317
shell_join() {
  local out="" piece quoted
  for piece in "$@"; do
    printf -v quoted '%q' "$piece"
    if [[ -n "$out" ]]; then
      out+=" "
    fi
    out+="$quoted"
  done
  printf '%s\n' "$out"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

read_env_key() {
  # Read one key from a dotenv file. Do not source the file: .env holds
  # credentials, and this function refuses to return secret-like keys.
  local file="$1"
  local key="$2"
  local line="" value=""
  case "$key" in
    *SECRET*|*PASSWORD*|*TOKEN*|*KEY*)
      die "refusing to read ${key} from ${file}"
      ;;
  esac
  [[ -f "$file" ]] || return 1
  line="$(grep -E "^[[:space:]]*(export[[:space:]]+)?${key}=" "$file" | tail -n 1 || true)"
  [[ -n "$line" ]] || return 1
  value="${line#*=}"
  value="${value%$'\r'}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  if [[ ${#value} -ge 2 && ( "$value" == \"*\" || "$value" == \'*\' ) ]]; then
    value="${value:1:${#value}-2}"
  fi
  printf '%s\n' "$value"
}

resolve_sha() {
  local ref="${1,,}"
  local url json token
  local -a curl_args
  if [[ "$ref" =~ ^[0-9a-f]{40}$ ]]; then
    printf '%s\n' "$ref"
    return 0
  fi
  if [[ ! "$ref" =~ ^[0-9a-f]{7,39}$ ]]; then
    die "sha must be a git commit id (7-40 hex characters), got: $1"
  fi
  if [[ -n "${GIT_CLONE:-}" && -d "${GIT_CLONE}/.git" ]]; then
    git -C "$GIT_CLONE" rev-parse --verify "${ref}^{commit}"
    return 0
  fi
  need_cmd curl
  url="https://api.github.com/repos/${GITHUB_REPO}/commits/${ref}"
  curl_args=(-fsS --max-time 30 -H "Accept: application/vnd.github+json" "$url")
  token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
  if [[ -n "$token" ]]; then
    curl_args=(-fsS --max-time 30 -H "Accept: application/vnd.github+json" -H "Authorization: Bearer ${token}" "$url")
  fi
  log "resolving ${ref} via ${url}"
  if ! json="$(curl "${curl_args[@]}")"; then
    die "could not resolve ${ref} via the GitHub API"
  fi
  STATUS_JSON="$json" python3 - <<'PY'
import json, os, sys
data = json.loads(os.environ["STATUS_JSON"])
sha = data.get("sha") or ""
if len(sha) != 40:
    sys.exit("GitHub API did not return a full commit id")
print(sha)
PY
}

find_current_source() {
  local dir="" latest="" candidate
  if [[ -n "${CURRENT_SOURCE:-}" ]]; then
    printf '%s\n' "$CURRENT_SOURCE"
    return 0
  fi
  if docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
    dir="$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' "$CONTAINER_NAME" 2>/dev/null || true)"
  fi
  if [[ -z "$dir" || ! -d "$dir" ]]; then
    latest=""
    shopt -s nullglob
    for candidate in "$APP_DIR"/source-*; do
      [[ -d "$candidate" ]] || continue
      if [[ -z "$latest" || "$candidate" -nt "$latest" ]]; then
        latest="$candidate"
      fi
    done
    shopt -u nullglob
    dir="$latest"
    if [[ -n "$dir" ]]; then
      log "warning: using newest source directory ${dir}; set CURRENT_SOURCE to override"
    fi
  fi
  [[ -n "$dir" && -d "$dir" ]] || die "cannot locate the running source directory (set CURRENT_SOURCE)"
  printf '%s\n' "$dir"
}

url_from_binding() {
  # `docker port` prints host bindings such as 192.168.86.5:8095 or 0.0.0.0:8095.
  local binding="$1"
  local host="" port=""
  if [[ "$binding" == \[*\]:* ]]; then
    host="${binding%%]:*}"
    host="${host#[}"
    port="${binding##*:}"
  else
    host="${binding%:*}"
    port="${binding##*:}"
  fi
  # 0.0.0.0 / :: means every interface, so loopback on this host works.
  # A specific address does not: compose.mqtt.yml publishes only that IP.
  if [[ "$host" == "0.0.0.0" || "$host" == "::" || -z "$host" ]]; then
    host="127.0.0.1"
  fi
  printf 'http://%s:%s\n' "$host" "$port"
}

resolve_health_base() {
  local binding="" addr=""
  if [[ -n "$HEALTH_BASE" ]]; then
    printf '%s\n' "${HEALTH_BASE%/}"
    return 0
  fi
  if binding="$(docker port "$CONTAINER_NAME" "${HEALTH_PORT}/tcp" 2>/dev/null)"; then
    binding="${binding%%$'\n'*}"
    if [[ -n "$binding" ]]; then
      url_from_binding "$binding"
      return 0
    fi
  fi
  addr="${PUBLISH_ADDRESS:-}"
  if [[ -z "$addr" && -n "$CURRENT_SOURCE" && -f "$CURRENT_SOURCE/.env" ]]; then
    addr="$(read_env_key "$CURRENT_SOURCE/.env" PUBLISH_ADDRESS || true)"
  fi
  if [[ -n "$addr" ]]; then
    printf 'http://%s:%s\n' "$addr" "$HEALTH_PORT"
    return 0
  fi
  return 1
}

warn_health_url() {
  local base="$1"
  case "$base" in
    http://192.168.86.20:*|http://192.168.86.20)
      log "warning: health URL ${base} uses 192.168.86.20, which is only the example default in compose.mqtt.yml. CT105 is 192.168.86.5. Set HEALTH_BASE or PUBLISH_ADDRESS if that example is not the address published on this host."
      ;;
  esac
}

capture_previous_schema() {
  local body="" parsed=""
  if [[ -n "$PREVIOUS_SCHEMA" ]]; then
    [[ "$PREVIOUS_SCHEMA" =~ ^[0-9]+$ ]] || die "PREVIOUS_SCHEMA must be an integer"
    log "previous schema (from PREVIOUS_SCHEMA): ${PREVIOUS_SCHEMA}"
    return 0
  fi
  if [[ -n "$HEALTH_URL" ]] && body="$(curl -fsS --connect-timeout "$HEALTH_CURL_MAX_TIME" --max-time "$HEALTH_CURL_MAX_TIME" "${HEALTH_URL}/api/v1/status" 2>/dev/null)"; then
    if parsed="$(STATUS_JSON="$body" python3 - <<'PY'
import json, os, sys
body = json.loads(os.environ["STATUS_JSON"])
schema = body.get("schema_version")
if isinstance(schema, bool) or not (
    isinstance(schema, int) or (isinstance(schema, str) and schema.isdigit())
):
    sys.exit(1)
print(int(schema))
PY
    )"; then
      PREVIOUS_SCHEMA="$parsed"
      log "previous schema from ${HEALTH_URL}/api/v1/status: ${PREVIOUS_SCHEMA}"
      return 0
    fi
  fi
  if [[ -f "$DB_PATH" ]]; then
    if parsed="$(python3 - "$DB_PATH" <<'PY'
import sqlite3, sys
from urllib.parse import quote
path = sys.argv[1]
conn = sqlite3.connect("file:" + quote(path, safe="/") + "?mode=ro", uri=True)
try:
    row = conn.execute(
        "SELECT value FROM meta WHERE key='schema_version'"
    ).fetchone()
finally:
    conn.close()
if row is None or not str(row[0]).isdigit():
    sys.exit(1)
print(int(row[0]))
PY
    )"; then
      PREVIOUS_SCHEMA="$parsed"
      log "previous schema from read-only ${DB_PATH}: ${PREVIOUS_SCHEMA}"
      return 0
    fi
    if [[ "$DRY_RUN" -eq 1 ]]; then
      log "warning: database exists at ${DB_PATH} but its schema could not be read"
      return 0
    fi
    die "database exists at ${DB_PATH} but its schema could not be read; refusing to deploy without a baseline"
  fi
  log "warning: no previous schema (status endpoint unreachable and no database file)"
}

check_ready_json() {
  local out=""
  if ! out="$(STATUS_JSON="$1" python3 - <<'PY'
import json, os, sys
try:
    body = json.loads(os.environ["STATUS_JSON"])
except json.JSONDecodeError as exc:
    print("readyz is not json: %s" % exc)
    sys.exit(1)
if body.get("ready") is not True:
    print("readyz ready is not true")
    sys.exit(1)
if body.get("db_ok") is not True:
    print("readyz db_ok is not true")
    sys.exit(1)
PY
  )"; then
    LAST_HEALTH_ERROR="${out:-readyz rejected}"
    return 1
  fi
}

check_status_json() {
  local out=""
  if ! out="$(STATUS_JSON="$1" PREVIOUS_SCHEMA="$PREVIOUS_SCHEMA" python3 - <<'PY'
import json, os, sys
try:
    body = json.loads(os.environ["STATUS_JSON"])
except json.JSONDecodeError as exc:
    print("status is not json: %s" % exc)
    sys.exit(1)
if body.get("db_ok") is not True:
    print("status db_ok is not true")
    sys.exit(1)
schema = body.get("schema_version")
if isinstance(schema, bool) or not (
    isinstance(schema, int) or (isinstance(schema, str) and schema.isdigit())
):
    print("status schema_version is not an integer: %r" % (schema,))
    sys.exit(1)
schema_n = int(schema)
previous = os.environ.get("PREVIOUS_SCHEMA", "").strip()
if previous:
    if not previous.isdigit():
        print("PREVIOUS_SCHEMA is not an integer")
        sys.exit(1)
    prev_n = int(previous)
    if schema_n < prev_n:
        print("schema %s is older than the previous schema %s" % (schema_n, prev_n))
        sys.exit(1)
print(schema_n)
PY
  )"; then
    LAST_HEALTH_ERROR="${out:-status rejected}"
    return 1
  fi
  OBSERVED_SCHEMA="$out"
}

health_once() {
  local base="$1"
  local body="" err=""
  if ! err="$(curl -fsS --connect-timeout "$HEALTH_CURL_MAX_TIME" --max-time "$HEALTH_CURL_MAX_TIME" -o /dev/null "${base}/healthz" 2>&1)"; then
    LAST_HEALTH_ERROR="healthz: ${err:-request failed}"
    return 1
  fi
  if ! body="$(curl -fsS --connect-timeout "$HEALTH_CURL_MAX_TIME" --max-time "$HEALTH_CURL_MAX_TIME" "${base}/readyz" 2>/dev/null)"; then
    LAST_HEALTH_ERROR="readyz: request failed"
    return 1
  fi
  check_ready_json "$body" || return 1
  if ! body="$(curl -fsS --connect-timeout "$HEALTH_CURL_MAX_TIME" --max-time "$HEALTH_CURL_MAX_TIME" "${base}/api/v1/status" 2>/dev/null)"; then
    LAST_HEALTH_ERROR="status: request failed"
    return 1
  fi
  check_status_json "$body"
}

wait_healthy() {
  local base="$1"
  local deadline=$((SECONDS + HEALTH_WAIT_SECONDS))
  local attempt=0
  while (( SECONDS <= deadline )); do
    attempt=$((attempt + 1))
    if health_once "$base"; then
      LAST_HEALTH_ERROR=""
      log "health ok at ${base} (healthz, readyz ready/db_ok, status db_ok, schema ${OBSERVED_SCHEMA} >= ${PREVIOUS_SCHEMA:-unknown})"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      break
    fi
    sleep "$HEALTH_INTERVAL_SECONDS"
  done
  log "health failed at ${base} after ${attempt} attempt(s): ${LAST_HEALTH_ERROR:-unknown}"
  return 1
}

run_stderr() {
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf 'DRY-RUN:' >&2
    printf ' %q' "$@" >&2
    printf '\n' >&2
    return 0
  fi
  "$@" >&2
}

unique_path() {
  local path="$1"
  local i=2
  if [[ ! -e "$path" ]]; then
    printf '%s\n' "$path"
    return 0
  fi
  while [[ -e "${path}.${i}" ]]; do
    i=$((i + 1))
  done
  printf '%s\n' "${path}.${i}"
}

image_id() {
  local id=""
  if [[ "$DRY_RUN" -eq 1 ]]; then
    printf '%s\n' "(dry-run)"
    return 0
  fi
  if ! id="$(docker image inspect --format '{{.Id}}' "$1" 2>/dev/null)"; then
    printf '%s\n' "(none)"
    return 0
  fi
  id="${id#sha256:}"
  printf '%s\n' "${id:0:12}"
}

compose_argv() {
  local dir="$1"
  shift
  COMPOSE_ARGV=(
    docker compose
    --project-directory "$dir"
    -f "$dir/$COMPOSE_FILE_MAIN"
    -f "$dir/$COMPOSE_FILE_OVERRIDE"
    -p "$COMPOSE_PROJECT"
    "$@"
  )
}

compose_up() {
  local dir="$1"
  [[ -f "$dir/$COMPOSE_FILE_MAIN" ]] || { log "error: missing ${dir}/${COMPOSE_FILE_MAIN}"; return 1; }
  [[ -f "$dir/$COMPOSE_FILE_OVERRIDE" ]] || { log "error: missing ${dir}/${COMPOSE_FILE_OVERRIDE}"; return 1; }
  compose_argv "$dir" up -d --no-build --force-recreate --no-deps "$SERVICE"
  run_stderr "${COMPOSE_ARGV[@]}"
}

set_image_python() {
  local image="$1"
  local backup_mode="$2"
  shift 2
  DOCKER_PY_ARGV=(
    docker run --rm
    --user "$CONTAINER_USER"
    --read-only
    --network none
    --cap-drop ALL
    --security-opt no-new-privileges:true
    --tmpfs "/tmp:rw,nosuid,size=16m,mode=1777"
    -e PYTHONDONTWRITEBYTECODE=1
    -e HOME=/tmp
    -v "${DATA_DIR}:/data"
    -v "${BACKUP_DIR}:/backups:${backup_mode}"
    --entrypoint python
    "$image"
    -m neurio_collector
    "$@"
  )
}

backup_sqlite() {
  local dest_host="$1"
  local dest_name db_name image
  dest_name="$(basename "$dest_host")"
  db_name="$(basename "$DB_PATH")"
  image="${IMAGE_NAME}:pre-${SHA}"
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    if [[ "$DRY_RUN" -eq 1 ]]; then
      image="${IMAGE_NAME}:local"
    else
      die "rollback image ${image} is missing; refusing to back up without it"
    fi
  fi
  set_image_python "$image" rw backup --db "/data/${db_name}" --dest "/backups/${dest_name}"
  run_stderr "${DOCKER_PY_ARGV[@]}"
  set_image_python "$image" ro verify-backup --src "/backups/${dest_name}"
  run_stderr "${DOCKER_PY_ARGV[@]}"
}

fetch_github_tarball() {
  local extract="$1"
  local sha="$2"
  local url="https://github.com/${GITHUB_REPO}/archive/${sha}.tar.gz"
  local archive="${TMP_FETCH}/archive.tar.gz"
  local token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
  local -a curl_args
  curl_args=(-fsSL --retry 3 --retry-delay 2 --output "$archive" "$url")
  if [[ -n "$token" ]]; then
    curl_args=(-fsSL --retry 3 --retry-delay 2 -H "Authorization: Bearer ${token}" --output "$archive" "$url")
  fi
  log "fetching ${url}"
  if ! curl "${curl_args[@]}"; then
    return 1
  fi
  if ! tar -C "$extract" -xzf "$archive"; then
    return 1
  fi
}

fetch_github_git() {
  local extract="$1"
  local sha="$2"
  local checkout="${TMP_FETCH}/checkout"
  need_cmd git
  log "fetching ${GITHUB_REPO}@${sha} with git"
  git init --quiet "$checkout"
  git -C "$checkout" remote add origin "https://github.com/${GITHUB_REPO}.git"
  GIT_TERMINAL_PROMPT=0 git -C "$checkout" fetch --quiet --depth 1 origin "$sha"
  git -C "$checkout" archive --format=tar FETCH_HEAD | tar -C "$extract" -xf -
}

move_extracted() {
  local extract="$1"
  local dest="$2"
  local -a entries=()
  local item
  shopt -s nullglob dotglob
  for item in "$extract"/*; do
    entries+=("$item")
  done
  shopt -u nullglob dotglob
  [[ ${#entries[@]} -gt 0 ]] || die "source archive was empty"
  if [[ ${#entries[@]} -eq 1 && -d "${entries[0]}" ]]; then
    mv "${entries[0]}" "$dest"
    return 0
  fi
  mkdir -p "$dest"
  for item in "${entries[@]}"; do
    mv "$item" "$dest/"
  done
}

fetch_source() {
  local dest="$1"
  local sha="$2"
  local extract=""
  if [[ -d "$dest" && -f "$dest/Dockerfile" && -f "$dest/$COMPOSE_FILE_MAIN" ]]; then
    log "reusing existing source directory ${dest}"
    return 0
  fi
  if [[ -e "$dest" ]]; then
    die "${dest} exists but is not a complete source tree; move it aside. This script does not delete it."
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    log "DRY-RUN: would fetch ${GITHUB_REPO}@${sha} into ${dest}"
    return 0
  fi
  mkdir -p "$(dirname "$dest")"
  TMP_FETCH="$(mktemp -d "${TMPDIR:-/tmp}/neurio-src.XXXXXX")"
  extract="${TMP_FETCH}/extract"
  mkdir -p "$extract"
  if [[ -n "${SOURCE_TARBALL:-}" ]]; then
    [[ -f "$SOURCE_TARBALL" ]] || die "SOURCE_TARBALL is not a file: ${SOURCE_TARBALL}"
    tar -C "$extract" -xf "$SOURCE_TARBALL"
  elif [[ -n "${GIT_CLONE:-}" ]]; then
    [[ -d "${GIT_CLONE}/.git" ]] || die "GIT_CLONE is not a git checkout: ${GIT_CLONE}"
    git -C "$GIT_CLONE" archive --format=tar "$sha" | tar -C "$extract" -xf -
  elif ! fetch_github_tarball "$extract" "$sha"; then
    log "GitHub tarball fetch failed; trying a git fetch of ${sha}"
    [[ "$extract" == "${TMP_FETCH}/extract" ]] || die "unexpected extract path"
    rm -rf -- "$extract"
    mkdir -p "$extract"
    fetch_github_git "$extract" "$sha"
  fi
  move_extracted "$extract" "$dest"
  [[ -f "$dest/Dockerfile" ]] || die "extracted tree is missing Dockerfile"
  [[ -f "$dest/$COMPOSE_FILE_MAIN" ]] || die "extracted tree is missing ${COMPOSE_FILE_MAIN}"
}

copy_live_config() {
  local override_src="${CURRENT_SOURCE}/${COMPOSE_FILE_OVERRIDE}"
  [[ -f "$override_src" ]] || die "live ${COMPOSE_FILE_OVERRIDE} not found in ${CURRENT_SOURCE}"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    log "DRY-RUN: would copy ${COMPOSE_FILE_OVERRIDE} and .env (mode 0600) from ${CURRENT_SOURCE} into ${NEW_SOURCE}"
    if [[ -f "${CURRENT_SOURCE}/.env" ]]; then
      ENV_BACKUP_PATH="${BACKUP_DIR}/.env.pre-${SHA}"
    fi
    return 0
  fi
  # Copy the override file only. It points at the secret file; the secret
  # file itself is not read or copied. Do not print either file.
  cp -a "$override_src" "${NEW_SOURCE}/${COMPOSE_FILE_OVERRIDE}"
  if [[ -f "${CURRENT_SOURCE}/.env" ]]; then
    ENV_BACKUP_PATH="$(unique_path "${BACKUP_DIR}/.env.pre-${SHA}")"
    install -m 0600 "${CURRENT_SOURCE}/.env" "$ENV_BACKUP_PATH"
    install -m 0600 "${CURRENT_SOURCE}/.env" "${NEW_SOURCE}/.env"
    log "copied .env to ${NEW_SOURCE}/.env and ${ENV_BACKUP_PATH} (mode 0600); contents not shown"
  else
    log "warning: no .env in ${CURRENT_SOURCE}. Compose will fall back to the example defaults in the compose file, including PUBLISH_ADDRESS 192.168.86.20."
  fi
}

warn_mount_mismatch() {
  local env_file="${CURRENT_SOURCE}/.env"
  local data_host="" backup_host=""
  [[ -f "$env_file" ]] || return 0
  data_host="$(read_env_key "$env_file" DATA_HOST_PATH || true)"
  backup_host="$(read_env_key "$env_file" BACKUP_HOST_PATH || true)"
  if [[ -n "$data_host" && "$data_host" != "$DATA_DIR" ]]; then
    log "warning: .env DATA_HOST_PATH is ${data_host} but this script reads ${DATA_DIR}. Set DATA_DIR if the live database is the compose path."
  fi
  if [[ -n "$backup_host" && "$backup_host" != "$BACKUP_DIR" ]]; then
    log "warning: .env BACKUP_HOST_PATH is ${backup_host} but this script writes ${BACKUP_DIR}. The printed restore command mounts BACKUP_DIR itself."
  fi
}

tag_rollback_image() {
  local pre="${IMAGE_NAME}:pre-${SHA}"
  if docker image inspect "$pre" >/dev/null 2>&1; then
    log "keeping existing rollback tag ${pre}"
  elif docker image inspect "${IMAGE_NAME}:local" >/dev/null 2>&1; then
    run_stderr docker tag "${IMAGE_NAME}:local" "$pre"
  elif [[ "$DRY_RUN" -eq 1 ]]; then
    log "DRY-RUN: would tag ${IMAGE_NAME}:local as ${pre}"
  else
    die "no ${IMAGE_NAME}:local image to keep as ${pre}; refusing to deploy without a rollback image"
  fi
  IMAGE_PRE_ID="$(image_id "$pre")"
}

archive_source() {
  local -a tar_excludes
  SRC_TAR_PATH="$(unique_path "${BACKUP_DIR}/src.pre-${SHA}.tgz")"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    log "DRY-RUN: would archive ${CURRENT_SOURCE} to ${SRC_TAR_PATH}"
    return 0
  fi
  tar_excludes=(
    --exclude=.git
    --exclude=.env
    --exclude='*.sqlite'
    --exclude='*.sqlite-*'
    --exclude='*secret*'
    --exclude='*password*'
    --exclude='*token*'
  )
  (umask 077; tar -C "$CURRENT_SOURCE" "${tar_excludes[@]}" -czf "$SRC_TAR_PATH" .)
  chmod 0600 "$SRC_TAR_PATH"
}

finish_database_backup() {
  local stamp
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  DB_BACKUP_PATH="$(unique_path "${BACKUP_DIR}/neurio-pre-${SHA}-${stamp}.sqlite")"
  if [[ ! -f "$DB_PATH" && "$DRY_RUN" -eq 0 ]]; then
    die "database not found: ${DB_PATH}"
  fi
  if [[ ! -f "$DB_PATH" ]]; then
    log "warning: database not found: ${DB_PATH}"
  fi
  backup_sqlite "$DB_BACKUP_PATH"
  if [[ "$DRY_RUN" -eq 1 ]]; then
    DB_BACKUP_SHA256="(dry-run)"
    return 0
  fi
  [[ -f "$DB_BACKUP_PATH" ]] || die "backup command returned but ${DB_BACKUP_PATH} is missing"
  chmod 0600 "$DB_BACKUP_PATH"
  DB_BACKUP_SHA256="$(sha256sum -- "$DB_BACKUP_PATH" | awk '{print $1}')"
  log "db backup sha256: ${DB_BACKUP_SHA256}"
}

build_image() {
  run_stderr docker build \
    -t "${IMAGE_NAME}:${IMAGE_TAG_PREFIX}-${SHA}" \
    -t "${IMAGE_NAME}:local" \
    "$NEW_SOURCE"
  IMAGE_NEW_ID="$(image_id "${IMAGE_NAME}:${IMAGE_TAG_PREFIX}-${SHA}")"
  IMAGE_LOCAL_ID="$(image_id "${IMAGE_NAME}:local")"
}

rollback() {
  local pre="${IMAGE_NAME}:pre-${SHA}"
  if ! docker image inspect "$pre" >/dev/null 2>&1; then
    ROLLED_BACK="no (pre image missing)"
    log "error: ${pre} is missing; cannot roll the image back"
    return 1
  fi
  docker tag "$pre" "${IMAGE_NAME}:local" || return 1
  if ! compose_up "$CURRENT_SOURCE"; then
    ROLLED_BACK="tag restored; recreate of previous source failed"
    return 1
  fi
  ROLLED_BACK="yes"
  IMAGE_LOCAL_ID="$(image_id "${IMAGE_NAME}:local")"
}

# shellcheck disable=SC2317
restore_command() {
  local db_name lock_name dest_name
  db_name="$(basename "$DB_PATH")"
  lock_name="$(basename "$LOCK_PATH")"
  dest_name="$(basename "${DB_BACKUP_PATH:-neurio-pre-${SHA}.sqlite}")"
  set_image_python "${IMAGE_NAME}:pre-${SHA}" ro \
    restore \
    --src "/backups/${dest_name}" \
    --db "/data/${db_name}" \
    --lock "/data/${lock_name}"
  shell_join "${DOCKER_PY_ARGV[@]}"
}

# shellcheck disable=SC2317
print_summary() {
  local rollback_tag="" rollback_up="" stop_line="" restore_line="" start_line=""
  if [[ -n "$CURRENT_SOURCE" ]]; then
    rollback_tag="$(shell_join docker tag "${IMAGE_NAME}:pre-${SHA}" "${IMAGE_NAME}:local")"
    compose_argv "$CURRENT_SOURCE" up -d --no-build --force-recreate --no-deps "$SERVICE"
    rollback_up="$(shell_join "${COMPOSE_ARGV[@]}")"
    compose_argv "$CURRENT_SOURCE" stop "$SERVICE"
    stop_line="$(shell_join "${COMPOSE_ARGV[@]}")"
    restore_line="$(restore_command)"
    compose_argv "$CURRENT_SOURCE" up -d --no-build --no-deps "$SERVICE"
    start_line="$(shell_join "${COMPOSE_ARGV[@]}")"
  else
    rollback_tag="(previous source unknown)"
    rollback_up=""
    stop_line=""
    restore_line=""
    start_line=""
  fi
  cat <<EOF

======== neurio-recorder deploy ========
sha:             ${SHA:-unknown}
dry-run:         ${DRY_RUN}
result:          ${HEALTH_RESULT}
rolled back:     ${ROLLED_BACK}
new source:      ${NEW_SOURCE:-not created}
previous source: ${CURRENT_SOURCE:-unknown}
image local:     ${IMAGE_LOCAL_ID:-none}
image hourly:    ${IMAGE_NEW_ID:-none}
image pre:       ${IMAGE_PRE_ID:-none}
db backup:       ${DB_BACKUP_PATH:-none}
db sha256:       ${DB_BACKUP_SHA256:-none}
source tarball:  ${SRC_TAR_PATH:-none}
env backup:      ${ENV_BACKUP_PATH:-none}
schema:          ${PREVIOUS_SCHEMA:-unknown} -> ${OBSERVED_SCHEMA:-unknown}
health url:      ${HEALTH_URL:-unresolved}
health detail:   ${LAST_HEALTH_ERROR:-none}

Image rollback (does not restore the database):
  ${rollback_tag}
  ${rollback_up}

Database restore is not applied. Stop the recorder, restore, then start it.
Do not remove volumes while doing this:
  ${stop_line}
  ${restore_line}
  ${start_line}
========================================
EOF
}

# shellcheck disable=SC2317
on_exit() {
  local code=$?
  trap - EXIT
  if [[ "$DRY_RUN" -eq 0 && "$RECREATED" -eq 1 && "$ROLLED_BACK" == "no" && "$code" -ne 0 ]]; then
    log "recreate did not finish cleanly; rolling the image back"
    rollback || log "error: automatic rollback failed"
    if [[ "$ROLLED_BACK" == "yes" ]]; then
      HEALTH_RESULT="${HEALTH_RESULT}; rolled back after error"
    fi
  fi
  if [[ -n "$TMP_FETCH" && -d "$TMP_FETCH" && "$TMP_FETCH" == "${TMPDIR:-/tmp}/neurio-src."?* ]]; then
    rm -rf -- "$TMP_FETCH"
    TMP_FETCH=""
  fi
  if [[ "$SUMMARY_PRINTED" -eq 0 ]]; then
    print_summary || true
    SUMMARY_PRINTED=1
  fi
  exit "$code"
}

SHA="$(resolve_sha "$SHA_ARG")"
[[ "$SHA" =~ ^[0-9a-f]{40}$ ]] || die "resolved sha is not 40 hex characters"
NEW_SOURCE="${APP_DIR}/source-${SHA}"
trap 'on_exit' EXIT

need_cmd docker
need_cmd curl
need_cmd python3
need_cmd tar
need_cmd install
need_cmd sha256sum
docker compose version >/dev/null 2>&1 || die "docker compose is not available"

if [[ "$DRY_RUN" -eq 0 && "${EUID:-$(id -u)}" -ne 0 ]]; then
  log "warning: not running as root; docker and chmod steps may fail"
fi

CURRENT_SOURCE="$(find_current_source)"
[[ -d "$CURRENT_SOURCE" ]] || die "current source directory does not exist: ${CURRENT_SOURCE}"
log "current source: ${CURRENT_SOURCE}"
log "target sha:     ${SHA}"
log "new source:     ${NEW_SOURCE}"

if ! HEALTH_URL="$(resolve_health_base)"; then
  if [[ "$DRY_RUN" -eq 1 ]]; then
    log "warning: could not resolve a health URL. Set HEALTH_BASE, for example http://192.168.86.5:${HEALTH_PORT}. compose.mqtt.yml publishes PUBLISH_ADDRESS only, so 127.0.0.1 is not reachable from this host unless that is the published address. This script does not assume 192.168.86.20."
    HEALTH_URL=""
  else
    die "could not resolve a health URL. Set HEALTH_BASE, for example http://192.168.86.5:${HEALTH_PORT}. compose.mqtt.yml publishes on PUBLISH_ADDRESS only, so 127.0.0.1:${HEALTH_PORT} is not reachable from this host unless that is the published address. This script does not assume 192.168.86.20."
  fi
fi
if [[ -n "$HEALTH_URL" ]]; then
  log "health url:     ${HEALTH_URL}"
  warn_health_url "$HEALTH_URL"
fi

capture_previous_schema
warn_mount_mismatch

if [[ "$DRY_RUN" -eq 0 ]]; then
  mkdir -p "$BACKUP_DIR"
  if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
    # The backup container writes this directory as CONTAINER_USER.
    chown "$CONTAINER_USER" "$BACKUP_DIR"
    chmod 0750 "$BACKUP_DIR"
  else
    log "warning: not root, so not changing ownership of ${BACKUP_DIR}. The backup container runs as ${CONTAINER_USER} and must be able to write it."
  fi
  if command -v flock >/dev/null 2>&1; then
    exec 9>"${APP_DIR}/deploy.lock"
    flock -n 9 || die "another deploy holds ${APP_DIR}/deploy.lock"
  else
    log "warning: flock not found; continuing without a deploy lock"
  fi
fi

fetch_source "$NEW_SOURCE" "$SHA"
copy_live_config
tag_rollback_image
archive_source
finish_database_backup
build_image

if [[ "$DRY_RUN" -eq 1 ]]; then
  log "DRY-RUN: would recreate ${SERVICE} from ${NEW_SOURCE} and check ${HEALTH_URL:-<unresolved>}"
  compose_argv "$NEW_SOURCE" up -d --no-build --force-recreate --no-deps "$SERVICE"
  printf 'DRY-RUN:' >&2
  printf ' %q' "${COMPOSE_ARGV[@]}" >&2
  printf '\n' >&2
  HEALTH_RESULT="skipped (dry-run)"
  exit 0
fi

# From here a failure leaves the new image tagged :local, so roll it back.
RECREATED=1
compose_up "$NEW_SOURCE"

if [[ -z "$HEALTH_BASE" ]]; then
  if refreshed="$(resolve_health_base)"; then
    HEALTH_URL="$refreshed"
    log "health url after recreate: ${HEALTH_URL}"
    warn_health_url "$HEALTH_URL"
  fi
fi

if wait_healthy "$HEALTH_URL"; then
  HEALTH_RESULT="ok"
  exit 0
fi

HEALTH_RESULT="failed"
failed_detail="${LAST_HEALTH_ERROR:-unknown}"
log "health checks failed; retagging ${IMAGE_NAME}:pre-${SHA} as :local and recreating from ${CURRENT_SOURCE}"
if rollback; then
  if wait_healthy "$HEALTH_URL"; then
    HEALTH_RESULT="failed on new sha; rollback health ok"
    LAST_HEALTH_ERROR="new sha failed (${failed_detail}); rollback is healthy"
  else
    HEALTH_RESULT="failed on new sha; rollback health failed"
    LAST_HEALTH_ERROR="new sha failed (${failed_detail}); rollback also failed (${LAST_HEALTH_ERROR:-unknown})"
  fi
else
  HEALTH_RESULT="failed on new sha; rollback failed"
  LAST_HEALTH_ERROR="new sha failed (${failed_detail}); rollback did not finish"
fi
exit 1
