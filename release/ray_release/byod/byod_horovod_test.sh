#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the horovod tests

set -exo pipefail

pip install torch==2.0.1+cu118 torchvision==0.15.2+cu118 \
  torch-scatter==2.1.1+pt20cu118 torch-sparse==0.6.17+pt20cu118 \
  torch-cluster==1.6.1+pt20cu118 torch-spline-conv==1.2.2+pt20cu118 \
  --extra-index-url https://download.pytorch.org/whl/cu118 \
  --find-links https://data.pyg.org/whl/torch-2.0.1+cu118.html

HOROVOD_WITH_GLOO=1 HOROVOD_WITHOUT_MPI=1 HOROVOD_WITHOUT_TENSORFLOW=1 HOROVOD_WITHOUT_MXNET=1 HOROVOD_WITH_PYTORCH=1 pip3 install -U horovod

# Required because horovod is compiled from source on this image, which has
# a higher version of libstdc++, not compatible with the conda environment.
ln -sf /usr/lib/gcc/x86_64-linux-gnu/11/libstdc++.so /home/ray/anaconda3/lib/libstdc++.so
ln -sf /usr/lib/gcc/x86_64-linux-gnu/11/libstdc++.so /home/ray/anaconda3/lib/libstdc++.so.6
