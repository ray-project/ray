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
MINICONDA_VERSION="24.3.0-0"
PYTHON_CODE="$(echo $PYTHON_VERSION | sed 's/\.//g')"

# Install Miniconda
MINICONDA_LINK="https://repo.anaconda.com/miniconda/Miniconda3-py${PYTHON_CODE}_${MINICONDA_VERSION}-Linux-x86_64.sh"
curl -o /tmp/miniconda.sh $MINICONDA_LINK
bash /tmp/miniconda.sh -b -p /home/ray/anaconda3 # use anaconda3 to match existing images to avoid surprises.
rm /tmp/miniconda.sh

/home/ray/anaconda3/bin/conda init bash;
EOF

ENV PATH="/home/ray/anaconda3/bin:$PATH"

CMD ["bash"]
