#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to run the object detection example notebooks.

set -exo pipefail

# Install Python dependencies
pip3 install --no-cache-dir \
    boto3==1.26.76 \
    imageio-ffmpeg==0.6.0 \
    opencv-python-headless==4.11.0.86 \
    pillow==11.1.0 \
    pycocotools==2.0.8 \
    requests==2.31.0 \
    smart-open==6.2.0 \
    torch==2.6.0 \
    torchvision==0.21.0 \
    xmltodict==0.14.2 \
    torchmetrics==1.6.1 \
    decord==0.6.0 \
    jupytext==0.6.5
