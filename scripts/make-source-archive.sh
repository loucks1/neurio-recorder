#!/bin/sh
# Pack a source archive. Does not build or run an image.
set -eu
root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
out_dir=${1:-"$root/dist"}
mkdir -p "$out_dir"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
archive="$out_dir/neurio-lan-collector-src-$stamp.tar.gz"
tar -C "$root" -czf "$archive" \
  --exclude='./.git' \
  --exclude='./dist' \
  --exclude='./uploads' \
  --exclude='./agent-tools' \
  --exclude='./.env' \
  --exclude='__pycache__' \
  --exclude='*.sqlite' \
  --exclude='*.sqlite-*' \
  Dockerfile \
  .dockerignore \
  .gitignore \
  .env.example \
  compose.yml \
  LICENSE \
  README.md \
  pyproject.toml \
  requirements.txt \
  requirements.lock \
  src \
  tests \
  docs \
  deploy \
  scripts
printf '%s\n' "$archive"
