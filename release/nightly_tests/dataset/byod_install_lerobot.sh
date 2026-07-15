#!/bin/bash
# Install the read_lerobot datasource's runtime deps for the LeRobot release
# tests (read_lerobot decodes LeRobot v3 camera streams via torchcodec / Pillow).
#
# lerobot is capped <0.6: lerobot 0.6.0's datasets/pyav_utils.py imports
# `av.option` at load, which the pinned av (>=16) no longer exposes, so 0.6.0
# fails to import. The 0.5.x line decodes via torchcodec and pins av<16.
# torchcodec is capped <0.10: 0.10 targets torch 2.10, whose libtorchcodec fails
# to load against the image's torch 2.9 (c10 ABI symbol error). We do NOT touch
# torch here -- the base (ray-ml) image's torch stands.

set -exo pipefail

pip3 install --no-cache-dir \
  "lerobot[dataset]>=0.5.0,<0.6" \
  "torchcodec<0.10" \
  av \
  pillow \
  huggingface_hub \
  s3fs
