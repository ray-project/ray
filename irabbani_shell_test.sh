#!/bin/bash

# This is a test script to make sure
# cgroupv2 is mounted in read-write mode
# in the CI environment that is tagged to run
# cgroup tests

CGROUP2_MOUNT=$(grep 'cgroup2' /proc/mounts)

if [[ -z "${CGROUP2_MOUNT}" ]]; then
    echo "Error: cgroupv2 is not mounted." >&2
    exit 1
fi

CGROUP2_RW=$(echo "${CGROUP2_MOUNT}" | grep 'rw')

if [[ -z "${CGROUP2_RW}" ]]; then
    echo "Error: cgroupv2 is not mounted in read-write mode." >&2
    exit 1
else
    echo "cgroupv2 is mounted in read-write mode."
    exit 0
fi
