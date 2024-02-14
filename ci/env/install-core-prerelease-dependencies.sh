#!/usr/bin/env bash

set -ex

# install all unbounded dependencies in setup.py for ray core
# TOOD(scv119) reenable grpcio once https://github.com/grpc/grpc/issues/31885 is fixed.
# TOOD(scv119) reenable jsonschema once https://github.com/ray-project/ray/issues/33411 is fixed.
DEPS=(aiosignal frozenlist requests protobuf)

python -m pip uninstall -y "${DEPS[@]}"
python -m pip install -U --upgrade-strategy=eager "${DEPS[@]}"
