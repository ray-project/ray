#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the agent stress test.

set -exo pipefail

pip3 install -U --force-reinstall --no-deps "git+https://github.com/ray-project/xgboost_ray@5a840af05d487171883dadbfdd37b138b607bed8#egg=xgboost_ray" "git+https://github.com/ray-project/lightgbm_ray@4c4d3413f86db769bddb6d08e2480a04bc75d712#egg=lightgbm_ray"
sudo mkdir -p /data || true
sudo chown ray:1000 /data || true
rm -rf /data/classification.parquet || true
curl -so create_test_data.py https://raw.githubusercontent.com/ray-project/ray/releases/1.3.0/release/xgboost_tests/create_test_data.py
python create_test_data.py /data/classification.parquet --seed 1234 --num-rows 1000000 --num-cols 40 --num-partitions 100 --num-classes 2
