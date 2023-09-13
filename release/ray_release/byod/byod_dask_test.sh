#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the horovod tests
# shellcheck disable=SC2102

set -exo pipefail

pip3 install dask[complete]
pip3 install boto3 s3fs
pip3 install -U s3fs
