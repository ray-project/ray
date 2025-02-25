ARG DOCKER_IMAGE_BASE
FROM $DOCKER_IMAGE_BASE

WORKDIR /rayci

RUN <<EOF
#!/bin/bash

set -euo pipefail

chmod 755 /usr/lib/sudo/sudoers.so
apt-get update && apt-get install -y --reinstall sudo

BUILD=1 DL=1 ./ci/env/install-dependencies.sh

EOF
