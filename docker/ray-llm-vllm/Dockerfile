# syntax=docker/dockerfile:1.3-labs
#
# Ray LLM image built on vLLM's release image (vllm/vllm-openai). vLLM, torch,
# FlashInfer, DeepEP and NCCL come from that image as they are; everything added
# on top resolves against the package versions it ships
# (/home/ray/vllm-constraints.txt) instead of changing them.

ARG BASE_IMAGE

# Build the ModelExpress (MX) server on the same base image, so the binary links
# against the final image's glibc, and the matching Python client wheel from the
# same commit; only these two are copied into the final image.
FROM "$BASE_IMAGE" AS mx-server-builder

ARG MODELEXPRESS_REF="e865b20f2771f594b06dede75fc6ef0718af186e"
ARG PROTOC_VERSION="29.3"

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update -y
apt-get install -y --no-install-recommends build-essential git

cd /root
curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal
PROTOC_ARCH="$(uname -m)"
if [[ "${PROTOC_ARCH}" == "aarch64" ]]; then
    PROTOC_ARCH="aarch_64"
fi
curl -fsSLo protoc.zip \
    "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip"
python3 -m zipfile -e protoc.zip protoc
chmod +x protoc/bin/protoc

git init -q modelexpress
cd modelexpress
git fetch -q --depth 1 https://github.com/ai-dynamo/modelexpress.git "${MODELEXPRESS_REF}"
git checkout -q FETCH_HEAD

# Redis and Kubernetes metadata backends are built by default; `memory`
# (single server, no external store) needs the feature.
PROTOC=/root/protoc/bin/protoc /root/.cargo/bin/cargo build --release \
    -p modelexpress-server --features memory-backend --bin modelexpress-server

python3 -m pip wheel --no-deps --wheel-dir /root/mx-wheel modelexpress_client/python
EOF

FROM "$BASE_IMAGE"

# Default for RAY_MX_SERVER_BINARY_PATH
# (python/ray/llm/_internal/serve/serving_patterns/model_express/envs.py).
COPY --from=mx-server-builder /root/modelexpress/target/release/modelexpress-server \
    /usr/local/bin/modelexpress-server
COPY --from=mx-server-builder /root/mx-wheel/ /tmp/mx-wheel/
COPY python/requirements/llm/patches/vllm-device-aware-compile-cache.patch \
    python/requirements/llm/patches/vllm-preserve-model-eos-tokens.patch /tmp/patches/

RUN <<EOF
#!/bin/bash

set -euo pipefail

# git applies the patches below and is needed by the base-extra layer; wget and
# gnupg too; ssh and rsync are for the Ray cluster launcher.
apt-get update -y
apt-get install -y --no-install-recommends git gnupg openssh-client rsync wget
rm -rf /var/lib/apt/lists/*

# Ray tooling and release test entrypoints call `python`.
ln -sf python3 /usr/bin/python

# Ray images run as `ray` (UID 1000, GID 100) with passwordless sudo. Ubuntu
# 24.04 images come with an `ubuntu` user that already has UID 1000.
if id ubuntu &>/dev/null; then
    userdel -r ubuntu
fi
useradd -ms /bin/bash -d /home/ray ray --uid 1000 --gid 100
usermod -aG sudo ray
echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers

SITE_PACKAGES="$(python3 -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"

# Pin every package vLLM's image ships, so whatever is installed on top adapts
# to vLLM's versions. Ray is left out: it's what goes on top.
python3 - "${SITE_PACKAGES}" > /home/ray/vllm-constraints.txt <<'PY'
import sys
from importlib.metadata import distributions

pins = {
    dist.metadata["Name"].lower(): dist.version
    for dist in distributions(path=[sys.argv[1]])
    if dist.metadata["Name"]
}
pins.pop("ray", None)
print("\n".join(f"{name}=={version}" for name, version in sorted(pins.items())))
PY

# Build time only: in the image, it would send `python -m uv pip install` from a
# virtualenv (runtime_env's uv plugin) to the system Python instead.
export UV_SYSTEM_PYTHON=1

# The MX client that load_format="modelexpress" imports, from the same commit as
# the server binary, and fastokens, which VLLM_USE_FASTOKENS below turns on.
uv pip install --no-cache --constraint /home/ray/vllm-constraints.txt \
    /tmp/mx-wheel/modelexpress-*.whl "fastokens==0.3.6"

# vllm-device-aware-compile-cache.patch: include the CUDA device index in vLLM's
# compile cache paths so a worker never reloads a torch.compile artifact built
# for a different physical GPU. vllm-project/vllm#38962 was reverted by #53304;
# still needed in vLLM 0.33.0.
# TODO (jeffreywang): Remove vllm-preserve-model-eos-tokens.patch when
# https://github.com/vllm-project/vllm/pull/52153 is merged.
(cd "${SITE_PACKAGES}" && git apply /tmp/patches/*.patch)
# vLLM's image precompiled its packages; recompile the patched files.
python3 -m compileall -q "${SITE_PACKAGES}/vllm"
rm -rf /tmp/mx-wheel /tmp/patches

# Let `ray` add, upgrade and remove packages in the system Python. Re-owning only
# directories is enough for that, and unlike `chown -R` it doesn't copy vLLM's
# multi-GB site-packages into this layer. Tools that replace a file (pip, uv,
# sed -i, patch, git apply) work too; writing into a file vLLM's image shipped
# needs sudo. The include dir takes C headers that wheels like greenlet ship.
INCLUDE_DIR="$(python3 -c 'import sysconfig; print(sysconfig.get_path("include"))')"
PREFIX_DIRS=(/usr/local/bin /usr/local/etc /usr/local/include /usr/local/share)
mkdir -p "${INCLUDE_DIR}" "${PREFIX_DIRS[@]}"
find "${SITE_PACKAGES}" "${INCLUDE_DIR}" "${PREFIX_DIRS[@]}" -type d -exec chown ray:users {} +

EOF

# vLLM's image sets UV_OVERRIDE so any uv install keeps its NCCL. Images built on
# this one (base-extra, release test deps) also keep every other vLLM package;
# ci/docker/ray-llm-vllm-image.Dockerfile restores vLLM's value in final images.
ENV UV_OVERRIDE="/etc/uv-overrides.txt /home/ray/vllm-constraints.txt"

# UV_COMPILE_BYTECODE precompiles packages installed later, including at runtime
# (e.g. via runtime_env). vLLM's /opt/uv/cache isn't writable by `ray`.
ENV UV_COMPILE_BYTECODE=1 \
    UV_CACHE_DIR=/home/ray/.cache/uv \
    VLLM_USE_FASTOKENS=1

USER ray
ENV HOME=/home/ray
WORKDIR /home/ray

# vllm/vllm-openai starts `vllm serve`; Ray images start a shell.
ENTRYPOINT []
CMD ["/bin/bash"]
