# syntax=docker/dockerfile:1.3-labs

# This Dockerfile is used to build the Ray image for Anyscale
# This image should contain only ray and its dependencies.

ARG BASE_IMAGE
FROM ${BASE_IMAGE}
ARG PYTHON_VERSION

RUN <<EOF
#!/bin/bash
set -eux
apt-get update
apt-get install -y --no-install-recommends \
		ca-certificates \
		netbase \
		tzdata \
		curl \
		sudo
useradd -ms /bin/bash -d /home/ray ray --uid 1000 --gid 100
usermod -aG sudo ray
echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers
EOF

# Switch to ray user
USER ray
ENV HOME=/home/ray
WORKDIR /home/ray

RUN <<EOF
#!/bin/bash
set -eux
MINIFORGE_VERSION="24.11.3-0"

# Install miniforge
MINIFORGE_LINK="https://github.com/conda-forge/miniforge/releases/download/${MINIFORGE_VERSION}/Miniforge3-${MINIFORGE_VERSION}-Linux-${HOSTTYPE}.sh"
curl -sfL -o /tmp/miniforge.sh "${MINIFORGE_LINK}"
bash /tmp/miniforge.sh -b -p /home/ray/anaconda3 # use anaconda3 to match existing images to avoid surprises.
rm /tmp/miniforge.sh

/home/ray/anaconda3/bin/conda init bash

eval "$(/home/ray/anaconda3/bin/conda shell.bash activate)"
/home/ray/anaconda3/bin/conda install -y "python=${PYTHON_VERSION}"
/home/ray/anaconda3/bin/conda clean -a

EOF

ENV PATH="/home/ray/anaconda3/bin:$PATH"

CMD ["bash"]
