#!/usr/bin/env bash

conda uninstall -y terminado
pip install -U pip
pip install "ray[rllib]"
pip install terminado
pip install boto3==1.4.8 cython==0.29.0

python3 wait_cluster.py

rllib train -f atari_impala_xlarge.yaml --ray-address=auto --queue-trials
