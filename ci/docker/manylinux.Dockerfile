# syntax=docker/dockerfile:1.3-labs

FROM quay.io/pypa/manylinux2014_x86_64:2022-12-20-b4884d9

ARG BUILDKITE_BAZEL_CACHE_URL

ENV BUILD_JAR=1
ENV RAY_INSTALL_JAVA=1
ENV BUILDKITE_BAZEL_CACHE_URL=$BUILDKITE_BAZEL_CACHE_URL

COPY ci/build/build-manylinux-forge.sh /tmp/build-manylinux-forge.sh
RUN /tmp/build-manylinux-forge.sh
