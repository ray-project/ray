#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
DYNAMO_ROOT="${DYNAMO_ROOT:-$DYNAMO_SOURCE}"
WHEEL_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --source) DYNAMO_ROOT="${2:-}"; shift 2 ;;
    --wheel-dir) WHEEL_DIR="${2:-}"; shift 2 ;;
    *) echo "Usage: $0 --wheel-dir DIRECTORY [--source DIRECTORY]" >&2; exit 2 ;;
  esac
done
[[ -n "$WHEEL_DIR" ]] || { echo "Usage: $0 --wheel-dir DIRECTORY [--source DIRECTORY]" >&2; exit 2; }
[[ "$WHEEL_DIR" = /* ]] || WHEEL_DIR="$PWD/$WHEEL_DIR"
[[ ! -e "$WHEEL_DIR" ]] || { echo "wheel directory exists: $WHEEL_DIR" >&2; exit 2; }
[[ -d "$DYNAMO_ROOT/.git" ]] || { echo "missing Dynamo source: $DYNAMO_ROOT" >&2; exit 2; }

PATCH="$SCRIPT_DIR/../patches/dynamo-cache-affinity-only.patch"
EXPECTED_COMMIT=dfc15c35d9cecffd909e8b10ab6ec62d4fa3d844
[[ "$(git -C "$DYNAMO_ROOT" rev-parse HEAD)" == "$EXPECTED_COMMIT" ]] || {
  echo "Dynamo must be pinned at $EXPECTED_COMMIT" >&2; exit 2;
}
if git -C "$DYNAMO_ROOT" apply --reverse --check "$PATCH" >/dev/null 2>&1; then
  echo "Dynamo cache-affinity patch already applied"
elif git -C "$DYNAMO_ROOT" apply --check "$PATCH" >/dev/null 2>&1; then
  git -C "$DYNAMO_ROOT" apply "$PATCH"
else
  echo "Dynamo cache-affinity patch cannot be applied" >&2; exit 2
fi

mkdir -p "$WHEEL_DIR"

if ! command -v protoc >/dev/null; then
  command -v curl >/dev/null || { echo "protoc or curl is required" >&2; exit 2; }
  command -v unzip >/dev/null || { echo "protoc or unzip is required" >&2; exit 2; }
  [[ "$(uname -s)-$(uname -m)" == "Linux-x86_64" ]] || {
    echo "install protoc or extend this fallback for $(uname -s)-$(uname -m)" >&2
    exit 2
  }
  PROTOC_DIR="$WHEEL_DIR/protoc-29.3"
  mkdir -p "$PROTOC_DIR"
  curl --fail --location --silent --show-error \
    --output "$PROTOC_DIR/protoc.zip" \
    https://github.com/protocolbuffers/protobuf/releases/download/v29.3/protoc-29.3-linux-x86_64.zip
  unzip -q "$PROTOC_DIR/protoc.zip" -d "$PROTOC_DIR"
  export PROTOC="$PROTOC_DIR/bin/protoc"
else
  PROTOC="$(command -v protoc)"
  export PROTOC
fi

if [[ ! -f "${LIBCLANG_PATH:-}/libclang.so" ]]; then
  LIBCLANG_ROOT="$WHEEL_DIR/libclang"
  uv pip install --python "$AGENTIC_RUNTIME_PYTHON" --target "$LIBCLANG_ROOT" libclang
  LIBCLANG_FILE="$(find "$LIBCLANG_ROOT" -name libclang.so -type f -print -quit)"
  [[ -n "$LIBCLANG_FILE" ]] || { echo "libclang installation failed" >&2; exit 1; }
  LIBCLANG_PATH="$(dirname "$LIBCLANG_FILE")"
  export LIBCLANG_PATH
fi
command -v gcc >/dev/null || { echo "gcc is required to build Dynamo" >&2; exit 2; }
GCC_INCLUDE="$(gcc -print-file-name=include)"
[[ -d "$GCC_INCLUDE" ]] || { echo "GCC headers not found" >&2; exit 1; }
export BINDGEN_EXTRA_CLANG_ARGS="-isystem$GCC_INCLUDE${BINDGEN_EXTRA_CLANG_ARGS:+ $BINDGEN_EXTRA_CLANG_ARGS}"

if ! command -v cargo >/dev/null; then
  command -v curl >/dev/null || { echo "Cargo or curl is required" >&2; exit 2; }
  export RUSTUP_HOME="$WHEEL_DIR/rustup"
  export CARGO_HOME="$WHEEL_DIR/cargo"
  export PATH="$CARGO_HOME/bin:$PATH"
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --no-modify-path
fi
command -v cargo >/dev/null || { echo "Cargo installation failed" >&2; exit 1; }

export CARGO_TARGET_DIR="${DYNAMO_CARGO_TARGET_DIR:-$WHEEL_DIR/cargo-target}"
(
  cd "$DYNAMO_ROOT/lib/bindings/python"
  uv tool uvx --from 'maturin[patchelf]' maturin build --release \
    --features select-service --interpreter "$AGENTIC_RUNTIME_PYTHON" --out "$WHEEL_DIR"
)
mapfile -t WHEELS < <(find "$WHEEL_DIR" -maxdepth 1 -name 'ai_dynamo_runtime-*.whl' -type f)
[[ ${#WHEELS[@]} -eq 1 ]] || { echo "expected one Dynamo wheel" >&2; exit 1; }
"$AGENTIC_RUNTIME_PYTHON" -m pip install --no-deps --force-reinstall "${WHEELS[0]}"
