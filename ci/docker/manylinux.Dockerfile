# syntax=docker/dockerfile:1.3-labs

ARG HOSTTYPE
FROM quay.io/pypa/manylinux2014_${HOSTTYPE}:2023-11-13-f6b0c51

ARG BUILDKITE_BAZEL_CACHE_URL

ENV BUILD_JAR=1
ENV RAY_INSTALL_JAVA=1
ENV BUILDKITE_BAZEL_CACHE_URL=$BUILDKITE_BAZEL_CACHE_URL

COPY ci/build/build-manylinux-forge.sh /tmp/build-manylinux-forge.sh
RUN /tmp/build-manylinux-forge.sh
