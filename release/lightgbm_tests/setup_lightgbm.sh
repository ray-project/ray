#!/bin/bash

pip install pytest
# Uninstall any existing lightgbm_ray repositories
pip uninstall -y lightgbm_ray || true

# Install lightgbm package
pip install -U "${LIGHTGBM_RAY_PACKAGE:-lightgbm_ray}"

# Create test dataset
sudo mkdir -p /data || true
sudo chown ray:1000 /data || true
rm -rf /data/classification.parquet || true
cp -R /tmp/ray_tmp_mount/lightgbm_tests ~/lightgbm_tests || echo "Copy failed"
python ~/lightgbm_tests/create_test_data.py /data/classification.parquet --seed 1234 --num-rows 1000000 --num-cols 40 --num-partitions 100 --num-classes 2
