#!/usr/bin/env bash

set -e

# install all unbounded dependencies in setup.py for ray core
# TODO(scv119) reenable grpcio once https://github.com/grpc/grpc/issues/31885 is fixed.
# TODO(scv119) reenable jsonschema once https://github.com/ray-project/ray/issues/33411 is fixed.
DEPS=(aiosignal frozenlist requests protobuf)
python -m pip install -U --pre --upgrade-strategy=eager "${DEPS[@]}"
