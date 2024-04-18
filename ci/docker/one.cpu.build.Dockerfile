ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build
FROM $DOCKER_IMAGE_BASE_BUILD

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

DOC_TESTING=1 TRAIN_TESTING=1 TUNE_TESTING=1 DATA_PROCESSING_TESTING=1 \
  INSTALL_HOROVOD=1 INSTALL_HDFS=1 RLLIB_TESTING=1 \
  ./ci/env/install-dependencies.sh

# serve dependencies
git clone https://github.com/wg/wrk.git /tmp/wrk && pushd /tmp/wrk && make -j && sudo cp wrk /usr/local/bin && popd

# data dependencies
sudo apt-get purge -y mongodb*
sudo apt-get install -y mongodb
sudo rm -rf /var/lib/mongodb/mongod.lock

EOF
