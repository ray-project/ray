# syntax=docker/dockerfile:1.3-labs

# Ray image for AWS Trainium (Trn2 / Trn3) and Inferentia nodes.
#
# Adds the AWS Neuron user-space stack on top of an Ubuntu 26.04 CPU base-extra
# image, which already carries the EFA user-space libraries (libfabric). The
# Neuron and EFA kernel drivers come from the host AMI and stay out of the image.

ARG FULL_BASE_IMAGE
FROM "$FULL_BASE_IMAGE"

ARG NEURON_RUNTIME_LIB_VERSION=2.35.33.0-d2725ccb8
ARG NEURON_COLLECTIVES_VERSION=2.35.49.0-354ad3064
ARG NEURON_TOOLS_VERSION=2.33.38.0-f874d5618
ARG TORCH_NEURONX_VERSION=2.14.3.*
# The Neuron compiler. torch-neuronx imports it but does not declare it.
ARG NEURONX_CC_VERSION=2.28.*

RUN <<EOF
#!/bin/bash

set -euo pipefail

source /etc/os-release

# Neuron runtime, collectives and tools (neuron-ls, nccom-test).
# Do not install aws-neuronx-dkms: the driver is part of the host AMI.
curl -fsSL https://apt.repos.neuron.amazonaws.com/GPG-PUB-KEY-AMAZON-AWS-NEURON.PUB \
    | sudo gpg --dearmor -o /usr/share/keyrings/neuron-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/neuron-archive-keyring.gpg] https://apt.repos.neuron.amazonaws.com ${VERSION_CODENAME} main" \
    | sudo tee /etc/apt/sources.list.d/neuron.list

sudo apt-get update -y
sudo apt-get install -y --no-install-recommends \
    "aws-neuronx-runtime-lib=${NEURON_RUNTIME_LIB_VERSION}" \
    "aws-neuronx-collectives=${NEURON_COLLECTIVES_VERSION}" \
    "aws-neuronx-tools=${NEURON_TOOLS_VERSION}"

sudo rm -f /etc/apt/sources.list.d/neuron.list
sudo rm -rf /var/lib/apt/lists/*

# TorchNeuron. It needs glibc >= 2.43, so it does not load on Ubuntu 22.04 or 24.04.
uv pip install --system --no-cache-dir --index-strategy unsafe-best-match \
    --extra-index-url https://pip.repos.neuron.amazonaws.com \
    "torch-neuronx==${TORCH_NEURONX_VERSION}" \
    "neuronx-cc==${NEURONX_CC_VERSION}"

EOF

# Prefer conda's newer libstdc++ over the system one; put the Neuron and EFA
# tools on PATH.
ENV LD_LIBRARY_PATH=/home/ray/anaconda3/lib:/opt/aws/neuron/lib:$LD_LIBRARY_PATH
ENV PATH=/opt/aws/neuron/bin:/opt/amazon/efa/bin:$PATH

# Fail the build, not the first run, if the stack is not usable.
RUN <<EOF
#!/bin/bash

set -euo pipefail

# Neuron runtime and collectives 2.35 need libfabric >= 2.4.
fi_info --version
fi_info --version | awk '/^libfabric:/ { split($2, v, "."); exit !(v[1] > 2 || (v[1] == 2 && v[2] >= 4)) }'

# The collectives transport must resolve libfabric to the EFA installer's copy.
ldd /opt/aws/neuron/lib/libnccom-net.so | tee /dev/stderr | grep -q "libfabric.so.* => /opt/amazon/efa/"

which neuron-ls nccom-test

# torch auto-loads torch_neuronx as a device backend, and that import asks the
# Neuron driver for the instance type, so it only works on Neuron hardware.
# Without hardware, check that the native library and its glibc / libstdc++
# requirements resolve, which is what breaks on an older base image.
TORCH_DEVICE_BACKEND_AUTOLOAD=0 python - <<PYEOF
import ctypes
import importlib.util
import os

import torch

pkg = importlib.util.find_spec("torch_neuronx").submodule_search_locations[0]
ctypes.CDLL(os.path.join(pkg, "_C.so"))
import neuronxcc

print("torch", torch.__version__, "- torch_neuronx native library loads")
PYEOF

EOF

# RAY_AUTH_MODE=disabled
#   Anyscale's agent on the head node calls the Ray dashboard API without an
#   auth token, so with token auth on, the release test job is never submitted.
ENV RAY_AUTH_MODE=disabled
