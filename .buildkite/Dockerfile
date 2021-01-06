FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Los_Angeles
ENV BUILDKITE=true
ENV CI=true

RUN apt-get update -qq
RUN apt-get install -y -qq \
    curl python-is-python3 git build-essential \
    sudo unzip apt-utils dialog tzdata wget
RUN locale -a

RUN mkdir /bazel-cache; \
    (echo "build --remote_cache=https://ray-bazel-remote.g1jl7rtfvrm4o.us-west-2.cs.amazonlightsail.com" >> /root/.bazelrc)

RUN mkdir /ray
WORKDIR /ray

# Below should be re-run each time
COPY . .
RUN ./ci/travis/ci.sh init
RUN ./ci/travis/ci.sh build

