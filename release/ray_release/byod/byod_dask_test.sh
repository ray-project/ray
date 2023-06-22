#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image 
# to run the horovod tests

set -exo pipefail

pip3 install dask[complete]
pip3 install boto3 s3fs
