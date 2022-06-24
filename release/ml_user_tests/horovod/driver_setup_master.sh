#!/bin/bash

cd "${0%/*}" || exit 1

sudo apt update
sudo apt -y install build-essential
pip install cmake

pip install -U -r ./driver_requirements.txt

HOROVOD_WITH_GLOO=1 HOROVOD_WITHOUT_MPI=1 HOROVOD_WITHOUT_TENSORFLOW=1 HOROVOD_WITHOUT_MXNET=1 HOROVOD_WITH_PYTORCH=1 pip install -U git+https://github.com/horovod/horovod.git