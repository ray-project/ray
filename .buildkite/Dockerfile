FROM ubuntu:focal

ARG REMOTE_CACHE_URL
ARG BUILDKITE_PULL_REQUEST

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Los_Angeles
ENV BUILDKITE=true
ENV CI=true
ENV PYTHON=3.6

RUN apt-get update -qq
RUN apt-get install -y -qq \
    curl python-is-python3 git build-essential \
    sudo unzip apt-utils dialog tzdata wget
RUN locale -a

# Setup Bazel caches
RUN (echo "build --remote_cache=${REMOTE_CACHE_URL}" >> /root/.bazelrc); \
    (if [ ${BUILDKITE_PULL_REQUEST} != "false" ]; then (echo "build --remote_upload_local_results=false" >> /root/.bazelrc); fi); \
    cat /root/.bazelrc

RUN mkdir /ray
WORKDIR /ray

# Below should be re-run each time
COPY . .
RUN ./ci/travis/ci.sh init
RUN bash --login -i ./ci/travis/ci.sh build
