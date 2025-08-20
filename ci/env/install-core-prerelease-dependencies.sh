#!/usr/bin/env bash

set -e

# install all unbounded dependencies in setup.py and any additional test dependencies
# for the min build for ray core
# TODO(scv119) reenable grpcio once https://github.com/grpc/grpc/issues/31885 is fixed.
# TODO(scv119) reenable jsonschema once https://github.com/ray-project/ray/issues/33411 is fixed.
DEPS=(requests protobuf pytest-httpserver==1.1.3)
python -m pip install -U --pre --upgrade-strategy=eager "${DEPS[@]}"
