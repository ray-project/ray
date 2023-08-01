#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the air_example_vicuna_13b_lightning_deepspeed_finetuning test.

set -exo pipefail

echo "sudo lsblk -f" >> ~/.bashrc
echo "yes N | sudo mkfs -t ext4 /dev/nvme1n1 || true" >> ~/.bashrc
echo "mkdir -p /mnt/local_storage" >> ~/.bashrc
echo "sudo chmod 0777 /mnt/local_storage" >> ~/.bashrc
echo "sudo mount /dev/nvme1n1 /mnt/local_storage || true" >> ~/.bashrc
pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
