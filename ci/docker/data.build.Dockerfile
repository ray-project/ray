ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_ml
FROM $DOCKER_IMAGE_BASE_BUILD

ARG ARROW_VERSION
ARG ARROW_MONGO_VERSION

# Unset dind settings; we are using the host's docker daemon.
ENV DOCKER_TLS_CERTDIR=
ENV DOCKER_HOST=
ENV DOCKER_TLS_VERIFY=
ENV DOCKER_CERT_PATH=

SHELL ["/bin/bash", "-ice"]

COPY . .

RUN DATA_PROCESSING_TESTING=1 ARROW_VERSION=$ARROW_VERSION ARROW_MONGO_VERSION=$ARROW_MONGO_VERSION ./ci/env/install-dependencies.sh
RUN pip install "datasets==2.14.0"

# Install MongoDB
RUN sudo apt-get install -y mongodb
RUN sudo rm -rf /var/lib/mongodb/mongod.lock
RUN sudo service mongodb start
