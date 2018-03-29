#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../

if [ ! -f $TP_DIR/pkg/redis/src/redis-server ]; then
  redis_vname="4.0-rc2"
  # This check is to make sure the tarball has been fully extracted. The only
  # relevant bit about redis/utils/whatisdoing.sh is that it is one of the last
  # files in the tarball.
  if [ ! -f $TP_DIR/pkg/redis/utils/whatisdoing.sh ]; then
    mkdir -p "$TP_DIR/pkg/redis"
    curl -sL "https://github.com/antirez/redis/archive/$redis_vname.tar.gz" | tar xz --strip-components=1 -C "$TP_DIR/pkg/redis"
  fi
  pushd $TP_DIR/pkg/redis
  make
  popd
fi
