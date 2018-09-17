#!/bin/bash

REDIS_BIN="redis_server"
MODULE="libray_redis_module.so"

$REDIS_BIN --loadmodule $MODULE "$@"
