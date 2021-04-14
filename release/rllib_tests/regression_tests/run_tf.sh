#!/usr/bin/env bash

conda uninstall -y terminado
pip install -U pip
pip install "ray[rllib]"
pip install terminado
pip install tensorflow-gpu==2.3.0  # Change for new CUDA version
pip install boto3==1.4.8 cython==0.29.0

# Run tf learning tests.
rllib train -f compact-regression-tests-tf.yaml
