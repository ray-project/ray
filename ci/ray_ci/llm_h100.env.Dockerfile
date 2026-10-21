# syntax=docker/dockerfile:1.3-labs
ARG BASE_IMAGE
FROM anyscale/ray:2.55.1-slim-py312-cu128 AS anyscale_runtime
FROM ${BASE_IMAGE}

COPY --from=anyscale_runtime /opt/anyscale /opt/anyscale

USER root
SHELL ["/bin/bash", "-ice"]

RUN <<EOF
set -euo pipefail
apt-get update -qq
apt-get install -y --no-install-recommends openssh-server openssh-client
useradd -ms /bin/bash -u 1000 -g 100 ray
echo 'ray ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/ray
python -m venv --system-site-packages /home/ray/virtualenv
/home/ray/virtualenv/bin/pip install -r /opt/anyscale/runtime-requirements.txt anyscale==0.26.74
cp /root/.bazelrc /home/ray/.bazelrc
sed -i '/experimental_downloader_config/d' /home/ray/.bazelrc
mkdir -p /artifact-mount
chown -R 1000:100 /home/ray /rayci /ray /artifact-mount
rm -rf /var/lib/apt/lists/*
EOF

USER ray
ENV HOME=/home/ray
ENV PATH=/home/ray/virtualenv/bin:/opt/miniforge/bin:${PATH}
ENV ANYSCALE_RAY_SITE_PKG_DIR=/rayci/python
ENV PIP_INDEX_URL=https://pypi.org/simple
ENV UV_INDEX_URL=https://pypi.org/simple
WORKDIR /home/ray
