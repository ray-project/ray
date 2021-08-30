#!/bin/bash

pip install pytest
# Uninstall any existing xgboost_ray repositories
pip uninstall -y xgboost_ray || true

# Install xgboost package
pip install -U "${XGBOOST_RAY_PACKAGE:-xgboost_ray}"

# Create test dataset
sudo mkdir -p /data || true
sudo chown ray:1000 /data || true
rm -rf /data/classification.parquet || true
cp -R /tmp/ray_tmp_mount/xgboost_tests ~/xgboost_tests || echo "Copy failed"
python ~/xgboost_tests/create_test_data.py /data/classification.parquet --seed 1234 --num-rows 1000000 --num-cols 40 --num-partitions 100 --num-classes 2
