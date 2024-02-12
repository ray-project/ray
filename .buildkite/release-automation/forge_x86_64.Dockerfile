# syntax=docker/dockerfile:1.3-labs

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update
apt-get upgrade -y
apt-get install -y curl zip

# Install miniconda
curl -sfL https://repo.anaconda.com/miniconda/Miniconda3-py38_23.1.0-1-Linux-x86_64.sh > /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -u -p /root/miniconda3
rm /tmp/miniconda.sh
/root/miniconda3/bin/conda init bash

EOF

CMD ["echo", "ray release-automation forge"]
