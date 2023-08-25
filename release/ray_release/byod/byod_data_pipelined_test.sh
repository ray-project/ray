#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the horovod tests

set -exo pipefail

pip install -U git+https://github.com/ray-project/ray_shuffling_data_loader.git@add-embedding-model
HOROVOD_WITH_GLOO=1 HOROVOD_WITHOUT_MPI=1 HOROVOD_WITHOUT_TENSORFLOW=1 HOROVOD_WITHOUT_MXNET=1 HOROVOD_WITH_PYTORCH=1 pip3 install -U git+https://github.com/horovod/horovod.git
