#!/usr/bin/env bash

conda uninstall -y terminado
pip install -U pip
pip install "ray[rllib]"
pip install terminado
pip install torch==1.6 torchvision
pip install boto3==1.4.8 cython==0.29.0

# Run torch learning tests.
rllib train -f compact-regression-tests-torch.yaml
