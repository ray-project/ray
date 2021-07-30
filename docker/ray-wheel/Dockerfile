ARG GPU
ARG DOCKER_PREFIX

FROM "$DOCKER_PREFIX"ray-deps:nightly"$GPU"
# For Click
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

#tag::bzl[]
# Specify bazel (see https://github.com/bazelbuild/bazel/issues/12887)
ENV USE_BAZEL_VERSION=3.7.2
#end::bzl[]
ADD ray.tar /ray
ADD git-rev /ray/git-rev
# Install dependencies needed to build ray
USER root
RUN apt-get update && apt-get install -y curl unzip cmake gcc g++ build-essential libunwind8 libunwind-dev libunwind8-dev openjdk-11-jdk && apt-get clean
#RUN cd /ray && git init && ./ci/travis/install-bazel.sh --system
ENV PATH=$PATH:/home/ray/bin
RUN echo 'build --remote_cache="https://storage.googleapis.com/ray-bazel-cache"' >> $HOME/.bazelrc
RUN echo 'build --remote_upload_local_results=false' >> $HOME/.bazelrc
WORKDIR /ray/
# The result of bazel build is reused in pip install. It if run first to allow
# for failover to serial build if parallel build requires too much resources.
RUN bazel build //:ray_pkg || bazel build --jobs 1 //:ray_pkg
WORKDIR /ray/python/
RUN python3 setup.py bdist_wheel
#RUN pip install -e .
WORKDIR /ray
