#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the air_example_vicuna_13b_lightning_deepspeed_finetuning test.

set -exo pipefail

cat >> ~/.bashrc <<EOF
sudo lsblk -f
yes N | sudo mkfs -t ext4 /dev/nvme1n1 || true
mkdir -p /mnt/local_storage
sudo chmod 0777 /mnt/local_storage
sudo mount /dev/nvme1n1 /mnt/local_storage || true
EOF
pip3 uninstall -y pytorch-lightning
pip3 install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118
pip3 install lightning==2.0.3  transformers==4.30.2 accelerate==0.20.3 deepspeed==0.12.3