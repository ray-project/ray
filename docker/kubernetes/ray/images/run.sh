#!/bin/bash

function run_head() {
    ray start --head --redis-port=6379
    while true; do
        local_scheduler=$(ps -ef | grep local_scheduler | grep -v grep | awk '{print $2}')
        if [[ "${local_scheduler}" == "" ]]; then
            echo "ray local_scheduler exit"
            exit 1
        fi

        global_scheduler=$(ps -ef | grep global_scheduler | grep -v grep | awk '{print $2}')
        if [[ "${global_scheduler}" == "" ]]; then
            echo "ray global_scheduler exit"
            exit 1
        fi

        plasma_store=$(ps -ef | grep plasma_store | grep -v grep | awk '{print $2}')
        if [[ "${plasma_store}" == "" ]]; then
            echo "ray plasma_store exit"
            exit 1
        fi

        plasma_manager=$(ps -ef | grep plasma_manager | grep -v grep | awk '{print $2}')
        if [[ "${plasma_manager}" == "" ]]; then
            echo "ray plasma_manager exit"
            exit 1
        fi

        plasma_manager=$(ps -ef | grep plasma_manager | grep -v grep | awk '{print $2}')
        if [[ "${plasma_manager}" == "" ]]; then
            echo "ray plasma_manager exit"
            exit 1
        fi
        sleep 10
    done
}

function run_slave() {
    ray start --redis-address=ray-master:6379
    while true; do
        local_scheduler=$(ps -ef | grep local_scheduler | grep -v grep | awk '{print $2}')
        if [[ "${local_scheduler}" == "" ]]; then
            echo "ray local_scheduler exit"
            exit 1
        fi
        sleep 10
    done
}

if [[ "${HEAD}" -eq "true" ]]; then
  run_head
  exit 0
fi

run_slave