# syntax=docker/dockerfile:1.3-labs

ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG RAYCI_IS_GPU_BUILD=false
ARG RAYCI_LIGHTNING_1=false
ARG RAYCI_LIGHTNING_2=false
ARG PYTHON
ARG BUILD_VARIANT=build
ARG PYTHON_DEPSET=python/deplocks/ci/ml-$BUILD_VARIANT-ci_depset_py$PYTHON.lock
ARG THIRDPARTY_DEPSET=python/deplocks/ci/ci_ml_thirdparty_depset_py$PYTHON.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock
COPY "$THIRDPARTY_DEPSET" /home/ray/thirdparty_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

set -x

./ci/env/install-hdfs.sh

# Install system packages for doc testing (not in base image which lacks DOC_TESTING=1)
sudo apt-get update -qq && sudo apt-get install -y graphviz tesseract-ocr

# Install HEBO for testing (not supported on Python 3.12+)
if [[ "${PYTHON-}" != "3.12" ]]; then
  pip install HEBO==0.3.5
fi

# Install doc-testing Python packages not covered by depsets
pip install pydot pytesseract==0.3.13

# Install thirdparty dependencies
mkdir -p python/ray/thirdparty_files
uv pip install -r /home/ray/thirdparty_depset.lock --no-deps --target=python/ray/thirdparty_files

# Install Python dependencies from depset lock file
uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

# Inject our own mirror for the CIFAR10 dataset
SITE_PACKAGES=$(python -c 'from distutils.sysconfig import get_python_lib; print(get_python_lib())')

TF_CIFAR="${SITE_PACKAGES}/tensorflow/python/keras/datasets/cifar10.py"
TF_KERAS_CIFAR="${SITE_PACKAGES}/tf_keras/src/datasets/cifar10.py"
TORCH_CIFAR="${SITE_PACKAGES}/torchvision/datasets/cifar.py"
KERAS_CIFAR="${SITE_PACKAGES}/keras/src/datasets/cifar10.py"

for f in "$TF_CIFAR" "$TF_KERAS_CIFAR" "$TORCH_CIFAR" "$KERAS_CIFAR"; do
  [ -f "$f" ] && sed -i 's https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz https://air-example-data.s3.us-west-2.amazonaws.com/cifar-10-python.tar.gz g' "$f"
done

# Remove installed ray.
pip uninstall -y ray

EOF
