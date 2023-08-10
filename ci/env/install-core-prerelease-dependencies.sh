#!/usr/bin/env bash

set -e

# install all unbounded dependencies in setup.py for ray core
# TOOD(scv119) reenable grpcio once https://github.com/grpc/grpc/issues/31885 is fixed.
# TOOD(scv119) reenable jsonschema once https://github.com/ray-project/ray/issues/33411 is fixed.
for dependency in aiosignal frozenlist requests protobuf
do
    python -m pip install -U --pre --upgrade-strategy=eager $dependency
done
