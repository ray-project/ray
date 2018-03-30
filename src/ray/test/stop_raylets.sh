#!/usr/bin/env bash

killall raylet
sleep 1
killall plasma_store
sleep 1
killall redis-server
sleep 1
rm /tmp/store* /tmp/raylet*
