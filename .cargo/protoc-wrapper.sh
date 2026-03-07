#!/usr/bin/env bash
set -euo pipefail

# Prefer a real protoc on PATH when available.
if command -v protoc >/dev/null 2>&1; then
  exec protoc "$@"
fi

# Otherwise fallback to protoc-bin-vendored binaries downloaded by Cargo.
CARGO_HOME_DIR="${CARGO_HOME:-$HOME/.cargo}"

for candidate in \
  "$CARGO_HOME_DIR"/registry/src/*/protoc-bin-vendored-*/bin/protoc \
  "$CARGO_HOME_DIR"/registry/src/*/protoc-bin-vendored-*/bin/protoc.exe
do
  if [ -x "$candidate" ]; then
    exec "$candidate" "$@"
  fi
done

echo "error: protoc not found. Install protobuf-compiler or add protoc-bin-vendored." >&2
exit 1
