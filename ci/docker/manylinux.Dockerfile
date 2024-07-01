# syntax=docker/dockerfile:1.3-labs

ARG HOSTTYPE
FROM quay.io/pypa/manylinux2014_${HOSTTYPE}:2024-07-01-8dac23b

ARG BUILDKITE_BAZEL_CACHE_URL

ENV BUILD_JAR=1
ENV RAY_INSTALL_JAVA=1
ENV BUILDKITE_BAZEL_CACHE_URL=$BUILDKITE_BAZEL_CACHE_URL

COPY ci/build/build-manylinux-forge.sh /tmp/build-manylinux-forge.sh

RUN <<EOF
#!/bin/bash

# Centos 7 is EOL and is no longer available from the usual mirrors, so switch
# to https://vault.centos.org
sed -i 's/enabled=1/enabled=0/g' /etc/yum/pluginconf.d/fastestmirror.conf
sed -i 's/^mirrorlist/#mirrorlist/g' /etc/yum.repos.d/*.repo
sed -i 's;^.*baseurl=http://mirror;baseurl=https://vault;g' /etc/yum.repos.d/*.repo
if [ "${HOSTTYPE}" == "aarch64" ]; then
  sed -i 's;/centos/7/;/altarch/7/;g' /etc/yum.repos.d/*.repo
fi

./tmp/build-manylinux-forge.sh

EOF
