#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

if [ ! -f redis/src/redis-server ]; then
  redis_vname="redis-3.2.3"
  if [ ! -f redis/utils/whatisdoing.sh ]; then
    mkdir -p "./redis" && wget -O- "http://download.redis.io/releases/$redis_vname.tar.gz" | tar xvz --strip-components=1 -C "./redis"
  fi
  cd redis
  make
fi
