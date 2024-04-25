# syntax=docker/dockerfile:1.3-labs

ARG HOSTTYPE
FROM quay.io/pypa/manylinux_2_28_${HOSTTYPE}:2024-04-23-ef7507e

ARG BUILDKITE_BAZEL_CACHE_URL

ENV BUILD_JAR=1
ENV RAY_INSTALL_JAVA=1
ENV BUILDKITE_BAZEL_CACHE_URL=$BUILDKITE_BAZEL_CACHE_URL

COPY ci/build/build-manylinux-forge.sh /tmp/build-manylinux-forge.sh
RUN /tmp/build-manylinux-forge.sh
