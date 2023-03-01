#!/usr/bin/env bash

set -e

# install all unbounded dependencies in setup.py for ray core
# TOOD(scv119) reenable grpcio once https://github.com/grpc/grpc/issues/31885 is fixed.
for dependency in attrs jsonschema aiosignal frozenlist requests protobuf
do
    python -m pip install -U --pre --upgrade-strategy=eager $dependency
done
