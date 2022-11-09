#!/usr/bin/env bash

set -e

# install all unbounded dependencies in setup.py for ray core
for dependency in attrs jsonschema aiosignal frozenlist requests grpcio protobuf
do
    python -m pip install -U --pre --upgrade-strategy=eager $dependency
done
