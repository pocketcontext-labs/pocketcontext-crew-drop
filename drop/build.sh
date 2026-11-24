#!/usr/bin/env bash
# Build the single executable Drop stages into a Linux/amd64 NibRunner guest.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ $(uname -sm) == 'Linux x86_64' ]] || { echo 'Build on Linux/amd64' >&2; exit 1; }
asset=internal/dropsupervisor/assets/litestream-linux-amd64.gz
raw_sha=f2d452c1527be81f06d5f135655b81b0e8d9a338cb4c2825cf2243134bc2dbd6
mkdir -p drop/build
tmp=$(mktemp -d drop/build/.compile.XXXXXXXX)
trap 'rm -rf "$tmp"' EXIT
if [[ ! -f "$asset" ]] || ! gzip -dc "$asset" | sha256sum | grep -Fqx "$raw_sha  -"; then
  archive=litestream-0.5.18-linux-x86_64.tar.gz
  curl -fsSL --retry 3 "https://github.com/benbjohnson/litestream/releases/download/v0.5.18/$archive" -o "$tmp/$archive"
  echo "d3687b9fd0f2e9426bbd3da828429ff3f1dfc5c9b74d54fdf028e4ee510e7dea  $tmp/$archive" | sha256sum -c
  tar -xzf "$tmp/$archive" -C "$tmp" --wildcards --no-anchored litestream
  lsbin=$(find "$tmp" -type f -name litestream -print -quit)
  echo "$raw_sha  $lsbin" | sha256sum -c
  gzip -n -c "$lsbin" > "$tmp/litestream.gz"
  mv "$tmp/litestream.gz" "$asset"
fi
make ui
tags="$(cat build-tags.txt),crew_drop"
revision=$(git rev-parse HEAD)
if [[ -n "$(git status --porcelain --untracked-files=normal)" ]]; then revision="$revision-dirty"; fi
pkg=github.com/pocketcontext-labs/pocketcontext-crew/internal/buildinfo
output=pocketcontext-crew-drop-linux-amd64
CC="${CC:-musl-gcc}" CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -buildvcs=false -tags "$tags" \
  -ldflags "-s -w -linkmode external -extldflags=-static -X $pkg.Version=0.45.4-drop.1 -X $pkg.Commit=$revision" \
  -o "$tmp/$output" ./cmd/pocketcontext-crew
if readelf -lW "$tmp/$output" | grep -E 'INTERP|DYNAMIC'; then echo 'Expected a static ELF' >&2; exit 1; fi
bytes=$(stat -c %s "$tmp/$output")
(( bytes <= 90 * 1024 * 1024 )) || { echo "Drop release exceeds 90 MiB: $bytes bytes" >&2; exit 1; }
mv "$tmp/$output" "drop/build/$output"
(cd drop/build && sha256sum "$output")
echo "$bytes bytes"
