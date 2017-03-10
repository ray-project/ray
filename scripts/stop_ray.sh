#!/usr/bin/env bash

killall plasma_manager
killall plasma_store
killall local_scheduler
killall global_scheduler

# Find the PID of the monitor process and kill it.
kill $(ps aux | grep monitor.py | awk '{ print $2 }') 2> /dev/null

# Find the PID of the Redis process and kill it.
kill $(ps aux | grep redis-server | awk '{ print $2 }') 2> /dev/null

# Find the PIDs of the worker processes and kill them.
kill $(ps aux | grep default_worker.py | awk '{ print $2 }') 2> /dev/null

# Kill the processes related to the web UI.
killall polymer

# Find the PID of the Ray UI backend process and kill it.
kill $(ps aux | grep ray_ui.py | awk '{ print $2 }') 2> /dev/null
