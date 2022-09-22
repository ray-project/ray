#!/usr/bin/env bash

# unbounded dependencies in setup.py for ray core
for dependency in attrs grpcio jsonschema pyyaml aiosignal frozenlist requests
do
    python -m pip install -U --pre $dependency
done