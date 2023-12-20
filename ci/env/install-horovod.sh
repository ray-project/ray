#!/usr/bin/env bash

# This script installs horovod.

# TODO: eventually pin this to master.
HOROVOD_WITH_GLOO=1 HOROVOD_WITHOUT_MPI=1 HOROVOD_WITHOUT_MXNET=1 pip install --no-cache-dir -U horovod==0.28.1
