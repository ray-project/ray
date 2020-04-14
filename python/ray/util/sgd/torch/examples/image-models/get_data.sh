#!/usr/bin/env bash
curl https://s3.amazonaws.com/fast-ai-imageclas/imagenette2.tgz -O
mkdir data
tar zxvf imagenette2.tgz -C data
