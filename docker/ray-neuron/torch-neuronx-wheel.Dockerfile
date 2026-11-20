# syntax=docker/dockerfile:1.3-labs

# Builds the TorchNeuron (torch-neuronx) wheel from source on Ubuntu 22.04.
#
# AWS publishes prebuilt torch-neuronx wheels for Ubuntu 26.04 only (they need
# glibc 2.43). For Ubuntu 22.04 the Neuron docs say to build from source:
# https://awsdocs-neuron.readthedocs-hosted.com/en/latest/release-notes/components/torchneuron.html
#
# The build compiles LLVM / MLIR and takes hours, so it starts from plain
# Ubuntu and takes no files from this repository. The wanda cache then only
# misses when this file or one of its build args changes.

ARG BASE_IMAGE=ubuntu:22.04
FROM $BASE_IMAGE AS builder

ARG DEBIAN_FRONTEND=noninteractive
ARG PYTHON_VERSION=3.13

# https://github.com/aws-neuron/torch-neuronx, tag v2.33.0 (Neuron SDK 2.33.0).
ARG TORCH_NEURONX_REF=v2.33.0
ARG TORCH_NEURONX_COMMIT=b1b6c66a2be28bce3bed6ef20632f0d3164119ef
# The torch version this torch-neuronx release is built for.
ARG TORCH_VERSION=2.14.0
# The wheel links against libnrt from the Neuron runtime. Keep in sync with
# docker/ray-neuron/Dockerfile.
ARG NEURON_RUNTIME_LIB_VERSION=2.35.33.0-d2725ccb8

ARG BAZELISK_VERSION=v1.25.0
ARG BAZELISK_SHA256=fd8fdff418a1758887520fa42da7e6ae39aefc788cf5e7f7bb8db6934d279fc4
ARG UV_VERSION=0.9.30
ARG UV_SHA256=8b3762374972daa7a74bbc6896cc73229ca69a07403dd9f9ea3805a51ffd7582

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

RUN <<EOF
apt-get update -y
apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl git gnupg patchelf zlib1g-dev

source /etc/os-release
curl -fsSL https://apt.repos.neuron.amazonaws.com/GPG-PUB-KEY-AMAZON-AWS-NEURON.PUB \
    | gpg --dearmor -o /usr/share/keyrings/neuron-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/neuron-archive-keyring.gpg] https://apt.repos.neuron.amazonaws.com ${VERSION_CODENAME} main" \
    > /etc/apt/sources.list.d/neuron.list
apt-get update -y
apt-get install -y --no-install-recommends \
    "aws-neuronx-runtime-lib=${NEURON_RUNTIME_LIB_VERSION}"

# torch-neuronx pins its Bazel version in .bazelversion; bazelisk fetches it.
curl -fsSL -o /usr/local/bin/bazel \
    "https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-linux-amd64"
echo "${BAZELISK_SHA256}  /usr/local/bin/bazel" | sha256sum -c -
chmod +x /usr/local/bin/bazel

curl -fsSL -o /tmp/uv.tar.gz \
    "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-x86_64-unknown-linux-gnu.tar.gz"
echo "${UV_SHA256}  /tmp/uv.tar.gz" | sha256sum -c -
tar -xzf /tmp/uv.tar.gz -C /usr/local/bin --strip-components=1
rm -f /tmp/uv.tar.gz
EOF

ENV UV_PYTHON_INSTALL_DIR=/opt/python
ENV PATH=/opt/venv/bin:$PATH

RUN <<EOF
uv venv --python "${PYTHON_VERSION}" /opt/venv
uv pip install --python /opt/venv/bin/python --no-cache-dir \
    --index-url https://download.pytorch.org/whl/cpu \
    --extra-index-url https://pypi.org/simple \
    --index-strategy unsafe-best-match \
    "torch==${TORCH_VERSION}" "setuptools>=77.0.3" wheel grpcio-tools
EOF

RUN <<EOF
git clone --depth 1 --branch "${TORCH_NEURONX_REF}" \
    https://github.com/aws-neuron/torch-neuronx.git /build/torch-neuronx
cd /build/torch-neuronx

# A tag can be moved; the commit cannot.
if [[ "$(git rev-parse HEAD)" != "${TORCH_NEURONX_COMMIT}" ]]; then
    echo "torch-neuronx ${TORCH_NEURONX_REF} is $(git rev-parse HEAD), expected ${TORCH_NEURONX_COMMIT}" >&2
    exit 1
fi

export PYTHON_VERSION
export USE_BAZEL=1
# Version the wheel as a release build (2.14.3.0.<build>), not a dev build.
export RELEASE_BRANCH=1
export VERSION_BUILD=0
# Importing torch must not try to load a Neuron backend during the build.
export TORCH_DEVICE_BACKEND_AUTOLOAD=0

# The same two steps as upstream's tools/build: bdist_wheel alone packages
# the Python files before the native libraries exist and leaves them out.
python setup.py build_ext
python setup.py bdist_wheel release

mkdir -p /wheels
cp dist/torch_neuronx-*.whl /wheels/

# Guard against a wheel without its native libraries.
python - <<PYEOF
import glob
import zipfile

(wheel,) = glob.glob("/wheels/*.whl")
names = zipfile.ZipFile(wheel).namelist()
for lib in (
    "torch_neuronx/_C.so",
    "torch_mlir/_mlir_libs/_mlir.so",
    "torch_mlir/_mlir_libs/_stablehlo.so",
    "torch_mlir/_mlir_libs/_torchMlir.so",
):
    assert lib in names, f"{lib} is missing from {wheel}"
print("built", wheel)
PYEOF
EOF

FROM scratch
COPY --from=builder /wheels/ /
