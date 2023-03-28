#!/usr/bin/env bash

# This script installs horovod.

# TODO: eventually pin this to master.
HOROVOD_WITH_GLOO=1 HOROVOD_WITHOUT_MPI=1 HOROVOD_WITHOUT_MXNET=1 pip install --no-cache-dir -U git+https://github.com/horovod/horovod.git@0b19c5ce6c5c93e7ed3bbf680290f918b2a0bdbb