#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail

pip install -U "git+https://github.com/ray-project/xgboost_ray@master#egg=xgboost_ray" petastorm
sudo mkdir -p /data || true
sudo chown ray:1000 /data || true
rm -rf /data/classification.parquet || true
curl -so create_test_data.py https://raw.githubusercontent.com/ray-project/ray/releases/1.3.0/release/xgboost_tests/create_test_data.py
python create_test_data.py /data/classification.parquet --seed 1234 --num-rows 1000000 --num-cols 40 --num-partitions 100 --num-classes 2
