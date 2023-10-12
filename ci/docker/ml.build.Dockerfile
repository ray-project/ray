ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml
FROM $DOCKER_IMAGE_BASE_BUILD

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

COPY . .

# TODO (can): Move mosaicml to train-test-requirements.txt
RUN pip install "mosaicml==0.12.1"
RUN TRAIN_TESTING=1 TUNE_TESTING=1 DATA_PROCESSING_TESTING=1 INSTALL_HOROVOD=1 \
  ./ci/env/install-dependencies.sh
