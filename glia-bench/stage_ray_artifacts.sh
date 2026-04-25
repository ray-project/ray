#!/usr/bin/env bash
# Symlink ray-2.55.0's compiled artifacts (C extension, protobuf-generated
# code, prebuilt C++ workers, dashboard build) into a source tree. Needed
# because ``pip install -e python/`` makes the editable finder resolve
# ``import ray`` against the source tree, which from a fresh git clone has
# no compiled binaries.
#
# Artifact source is found in this order:
#   1. A cached wheel extraction at ``$RAY_WHEEL_CACHE`` (default
#      ``$HOME/.cache/glia-bench/ray-2.55.0``).
#   2. Download the wheel via ``pip download ray==2.55.0`` and extract it
#      into the cache.
#
# Cache is preferred over ``<site-packages>/ray/`` so that the symlinks are
# stable across ``pip install -e python/`` (which uninstalls the wheel and
# would otherwise turn every symlink into a dangling pointer).
#
# Run this once per source tree you intend to benchmark (typically both
# the pristine ray-2.55.0 tree and this fork's M6 tree).
#
# Usage:
#   ./stage_ray_artifacts.sh <tree>         # point <tree> at a python/ray dir
# Examples:
#   ./stage_ray_artifacts.sh "$PRISTINE_TREE"
#   ./stage_ray_artifacts.sh ../python/ray     # this fork's M6 tree
set -euo pipefail

TREE="${1:?usage: $0 <python/ray dir>}"
TREE="$(cd "$TREE" && pwd)"
if [ ! -f "$TREE/__init__.py" ]; then
    echo "ERROR: $TREE is not a python/ray directory (no __init__.py)" >&2
    exit 1
fi

CACHE_DIR="${RAY_WHEEL_CACHE:-$HOME/.cache/glia-bench/ray-2.55.0}"

download_wheel() {
    # pip downloads the right platform-specific wheel for the active env.
    local tmp wheel
    tmp="$(mktemp -d)"
    echo "  downloading ray==2.55.0 wheel into $CACHE_DIR ..." >&2
    pip download ray==2.55.0 --no-deps -d "$tmp" >/dev/null
    wheel="$(ls "$tmp"/ray-2.55.0-*.whl 2>/dev/null | head -1)"
    if [ -z "$wheel" ]; then
        echo "ERROR: pip download ray==2.55.0 did not produce a wheel" >&2
        rm -rf "$tmp"
        exit 1
    fi
    mkdir -p "$CACHE_DIR"
    unzip -q -o "$wheel" -d "$CACHE_DIR"
    rm -rf "$tmp"
}

find_source() {
    # 1. Cached extraction (stable across `pip install -e python/`).
    if [ -f "$CACHE_DIR/ray/_raylet.so" ]; then
        echo "$CACHE_DIR/ray"
        return
    fi
    # 2. Download + extract into the cache.
    download_wheel
    if [ -f "$CACHE_DIR/ray/_raylet.so" ]; then
        echo "$CACHE_DIR/ray"
        return
    fi
    echo "ERROR: failed to stage ray-2.55.0 artifacts — wheel did not contain _raylet.so" >&2
    exit 1
}

SRC="$(find_source)"
echo "source: $SRC"

# Directory-level artifacts — the source tree from a fresh git clone has
# nothing at these paths, so we symlink the whole directory from the wheel.
# - _raylet.so                                       : Cython C extension (the core of Ray)
# - core/generated/                                  : protobuf-generated Python modules
# - cpp/                                             : prebuilt C++ workers, if shipped
# - thirdparty_files/                                : bundled third-party Python deps
# - dashboard/client/build/                          : built React UI for the dashboard
# - _private/runtime_env/agent/thirdparty_files/     : bundled aiohttp et al (compiled .so's)
DIR_ARTIFACTS=(
    _raylet.so
    core/generated
    serve/generated
    cpp
    thirdparty_files
    dashboard/client/build
    _private/runtime_env/agent/thirdparty_files
)

# File-level artifacts — paths that live inside directories the source
# tree already populates (with ``__init__.py``, .cc, .h files, etc.).
# Symlinking the parent would hide the source files; we must stage the
# individual binaries instead.
# - core/libjemalloc.so         : jemalloc shared library (ray links at runtime)
# - core/src/ray/gcs/gcs_server : GCS server binary
# - core/src/ray/raylet/raylet  : raylet binary
FILE_ARTIFACTS=(
    core/libjemalloc.so
    core/src/ray/gcs/gcs_server
    core/src/ray/raylet/raylet
)

stage_one() {
    local src="$1" dst="$2"
    if [ ! -e "$src" ]; then
        echo "  skip: $dst (source $src not present in wheel)" >&2
        return
    fi
    if [ -e "$dst" ] && [ ! -L "$dst" ]; then
        echo "  skip: $dst exists (not a symlink; leaving alone)"
        return
    fi
    mkdir -p "$(dirname "$dst")"
    ln -sfn "$src" "$dst"
    echo "  staged: $dst -> $src"
}

for item in "${DIR_ARTIFACTS[@]}"; do
    stage_one "$SRC/$item" "$TREE/$item"
done
for item in "${FILE_ARTIFACTS[@]}"; do
    stage_one "$SRC/$item" "$TREE/$item"
done

echo "done: $TREE"
