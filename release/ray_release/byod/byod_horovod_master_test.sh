#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the horovod tests

set -exo pipefail

HOROVOD_WITH_GLOO=1 HOROVOD_WITHOUT_MPI=1 HOROVOD_WITHOUT_TENSORFLOW=1 HOROVOD_WITHOUT_MXNET=1 HOROVOD_WITH_PYTORCH=1 pip3 install -U git+https://github.com/horovod/horovod.git

# Required because horovod is compiled from source on this image, which has
# a higher version of libstdc++, not compatible with the conda environment.
ln -sf /usr/lib/gcc/x86_64-linux-gnu/11/libstdc++.so /home/ray/anaconda3/lib/libstdc++.so
ln -sf /usr/lib/gcc/x86_64-linux-gnu/11/libstdc++.so /home/ray/anaconda3/lib/libstdc++.so.6
