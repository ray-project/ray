#!/usr/bin/env bash

set -e

# install all unbounded dependencies in setup.py and any additional test dependencies
# for the min build for ray core
DEPS=(requests protobuf pytest-httpserver==1.1.3 grpcio==1.74.0 jsonschema==4.23.0)
python -m pip install -U --pre --upgrade-strategy=eager "${DEPS[@]}"
