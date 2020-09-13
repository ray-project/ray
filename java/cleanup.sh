#!/usr/bin/env bash

# Stop backend processes
ray stop
# Kill Java workers
# shellcheck disable=SC2009
ps aux | grep DefaultWorker | grep -v grep | awk '{print $2}' | xargs kill -9
# Remove temp files
rm -rf /tmp/ray
