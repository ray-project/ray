# syntax=docker/dockerfile:1.3-labs

ARG HOSTTYPE
FROM quay.io/pypa/manylinux2014_${HOSTTYPE}:2024-07-02-9ac04ee

ARG BUILDKITE_BAZEL_CACHE_URL
ARG RAYCI_DISABLE_JAVA=false
ARG RAY_INSTALL_JAVA=1

ENV BUILD_JAR=1
ENV RAYCI_DISABLE_JAVA=$RAYCI_DISABLE_JAVA
ENV RAY_INSTALL_JAVA=$RAY_INSTALL_JAVA
ENV BUILDKITE_BAZEL_CACHE_URL=$BUILDKITE_BAZEL_CACHE_URL

RUN yum -y install sudo

COPY ci/build/build-manylinux-forge.sh /tmp/build-manylinux-forge.sh

RUN ./tmp/build-manylinux-forge.sh
