#!/bin/bash
# This script is used to build an extra layer on top of the base anyscale/ray image
# to install JAX with TPU support and libtpu.
set -euo pipefail

# Install JAX with TPU support and libtpu. Adjust wheels if needed per TPU gen.
pip install -U "jax[tpu]" -f https://storage.googleapis.com/jax-releases/libtpu_releases.html
