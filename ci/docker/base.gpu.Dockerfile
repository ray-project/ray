ARG BASE_IMAGE=nvidia/cuda:12.1.1-cudnn8-devel-ubuntu20.04
FROM $BASE_IMAGE

ARG REMOTE_CACHE_URL
ARG BUILDKITE_PULL_REQUEST
ARG BUILDKITE_COMMIT
ARG BUILDKITE_PULL_REQUEST_BASE_BRANCH
ARG PYTHON=3.9

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Los_Angeles

ENV RAY_BUILD_ENV=ubuntu20.04_cuda12.1_py$PYTHON
ENV BUILDKITE=true
ENV CI=true
ENV PYTHON=$PYTHON
ENV RAY_USE_RANDOM_PORTS=1
ENV RAY_DEFAULT_BUILD=1
ENV RAY_INSTALL_JAVA=0
ENV BUILDKITE_PULL_REQUEST=${BUILDKITE_PULL_REQUEST}
ENV BUILDKITE_COMMIT=${BUILDKITE_COMMIT}
ENV BUILDKITE_PULL_REQUEST_BASE_BRANCH=${BUILDKITE_PULL_REQUEST_BASE_BRANCH}
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
RUN ln -s /usr/bin/clang-format-12 /usr/bin/clang-format && \
    ln -s /usr/bin/clang-tidy-12 /usr/bin/clang-tidy && \
    ln -s /usr/bin/clang-12 /usr/bin/clang

RUN curl -o- https://get.docker.com | sh -s -- --version 27.2

# System conf for tests
RUN locale -a
ENV LC_ALL=en_US.utf8
ENV LANG=en_US.utf8
RUN echo "ulimit -c 0" >> /root/.bashrc

# Setup Bazel caches
RUN (echo "build --remote_cache=${REMOTE_CACHE_URL}" >> /root/.bazelrc); \
    (if [ "${BUILDKITE_PULL_REQUEST}" != "false" ]; then (echo "build --remote_upload_local_results=false" >> /root/.bazelrc); fi); \
    cat /root/.bazelrc

# Install some dependencies (miniforge, pip dependencies, etc)
RUN mkdir /ray
WORKDIR /ray

# Below should be re-run each time
COPY . .

RUN bash --login -ie -c '\
    BUILD=1 SKIP_PYTHON_PACKAGES=1 ./ci/env/install-dependencies.sh \
'
