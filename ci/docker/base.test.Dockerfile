ARG DOCKER_IMAGE_BASE_UBUNTU=ubuntu:focal
FROM $DOCKER_IMAGE_BASE_UBUNTU

ARG REMOTE_CACHE_URL
ARG BUILDKITE_PULL_REQUEST
ARG BUILDKITE_COMMIT
ARG BUILDKITE_PULL_REQUEST_BASE_BRANCH
ARG PYTHON=3.7

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Los_Angeles

ENV BUILDKITE=true
ENV CI=true
ENV PYTHON=$PYTHON
ENV RAY_USE_RANDOM_PORTS=1
ENV RAY_DEFAULT_BUILD=1
ENV RAY_INSTALL_JAVA=0
ENV BUILDKITE_PULL_REQUEST=${BUILDKITE_PULL_REQUEST}
ENV BUILDKITE_COMMIT=${BUILDKITE_COMMIT}
ENV BUILDKITE_PULL_REQUEST_BASE_BRANCH=${BUILDKITE_PULL_REQUEST_BASE_BRANCH}
# For wheel build
# https://github.com/docker-library/docker/blob/master/20.10/docker-entrypoint.sh
ENV DOCKER_TLS_CERTDIR=/certs
ENV DOCKER_HOST=tcp://docker:2376
ENV DOCKER_TLS_VERIFY=1
ENV DOCKER_CERT_PATH=/certs/client
ENV TRAVIS_COMMIT=${BUILDKITE_COMMIT}
ENV BUILDKITE_BAZEL_CACHE_URL=${REMOTE_CACHE_URL}

RUN apt-get update -qq && apt-get upgrade -qq
RUN apt-get install -y -qq \
    curl python-is-python3 git build-essential \
    sudo unzip unrar apt-utils dialog tzdata wget rsync \
    language-pack-en tmux cmake gdb vim htop \
    libgtk2.0-dev zlib1g-dev libgl1-mesa-dev \
    clang-format-12 jq \
    clang-tidy-12 clang-12
# Make using GCC 9 explicit.
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 --slave /usr/bin/g++ g++ /usr/bin/g++-9 \
    --slave /usr/bin/gcov gcov /usr/bin/gcov-9
RUN ln -s /usr/bin/clang-format-12 /usr/bin/clang-format && \
    ln -s /usr/bin/clang-tidy-12 /usr/bin/clang-tidy && \
    ln -s /usr/bin/clang-12 /usr/bin/clang

RUN curl -o- https://get.docker.com | sh

# System conf for tests
RUN locale -a
ENV LC_ALL=en_US.utf8
ENV LANG=en_US.utf8
RUN echo "ulimit -c 0" >> /root/.bashrc

# Setup Bazel caches
RUN (echo "build --remote_cache=${REMOTE_CACHE_URL}" >> /root/.bazelrc); \
    (if [ "${BUILDKITE_PULL_REQUEST}" != "false" ]; then (echo "build --remote_upload_local_results=false" >> /root/.bazelrc); fi); \
    cat /root/.bazelrc

# Install some dependencies (miniconda, pip dependencies, etc)
RUN mkdir /ray
WORKDIR /ray

# Below should be re-run each time
COPY . .

RUN ./ci/env/install-dependencies.sh init
