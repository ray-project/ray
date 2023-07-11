#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail

echo "yes N | sudo mkfs -t ext4 /dev/nvme1n1 || true" >> ~/.bashrc
echo "mkdir -p /tmp/data0" >> ~/.bashrc
echo "mkdir -p /tmp/data1" >> ~/.bashrc
echo "sudo chmod 0777 /tmp/data0" >> ~/.bashrc
echo "sudo chmod 0777 /tmp/data1" >> ~/.bashrc
echo "sudo mount /dev/nvme1n1 /tmp/data1 || true" >> ~/.bashrc
