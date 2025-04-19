#!/bin/bash

DATA_DIR="/mnt/local_storage/imagenet"
mkdir -p $DATA_DIR
pushd $DATA_DIR || exit
aws s3 cp s3://anyscale-imagenet/ILSVRC/Data/CLS-LOC/imagenet-64k.zip ./imagenet-64k.zip
unzip -q imagenet-64k.zip
rm imagenet-64k.zip
popd || exit
