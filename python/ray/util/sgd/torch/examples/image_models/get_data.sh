#!/usr/bin/env bash
curl https://s3.amazonaws.com/fast-ai-imageclas/imagenette2.tgz -O
tar zxvf imagenette2.tgz
mv imagenette2 data
