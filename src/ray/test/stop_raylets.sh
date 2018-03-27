#!/usr/bin/env bash

killall raylet
killall plasma_store
killall redis-server
sleep 1
rm /tmp/store* /tmp/raylet*
