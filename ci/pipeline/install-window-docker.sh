#!/bin/bash

set -ex

mkdir -p /c/tools
wget -P /c/tools https://download.docker.com/win/static/stable/x86_64/docker-17.09.0-ce.zip
unzip /c/tools/docker-17.09.0-ce.zip -d /c/tools
rm /c/tools/docker-17.09.0-ce.zip
mv /c/tools/docker/* /c/bazel/
