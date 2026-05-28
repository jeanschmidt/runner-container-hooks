#!/usr/bin/env bash
# Cross-compile the in-pod RPC server to static musl binaries for amd64 + arm64.
# Output: packages/k8s/dist/rpc-server-{amd64,arm64} (gitignored).
#
# Requirements:
#   - rustup with x86_64-unknown-linux-musl and aarch64-unknown-linux-musl targets
#   - On non-Linux hosts (or for arm64 cross): `cargo install cross` (uses Docker)
#
# CI uses `cross` via the build matrix; local Linux dev can use rustup + musl-tools.

set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CRATE="$HERE/../rpc-server"
# Note: NOT packages/k8s/dist — that's the ncc-bundled JS release output.
OUT="$CRATE/binaries"
mkdir -p "$OUT"

build_target() {
  local target="$1"
  local out_name="$2"

  if command -v cross >/dev/null 2>&1; then
    ( cd "$CRATE" && cross build --release --target "$target" )
  else
    ( cd "$CRATE" && cargo build --release --target "$target" )
  fi

  cp "$CRATE/target/$target/release/rpc-server" "$OUT/$out_name"
  chmod +x "$OUT/$out_name"

  local size
  size=$(wc -c < "$OUT/$out_name")
  echo "built $out_name: $size bytes"
}

build_target x86_64-unknown-linux-musl rpc-server-amd64
build_target aarch64-unknown-linux-musl rpc-server-arm64
