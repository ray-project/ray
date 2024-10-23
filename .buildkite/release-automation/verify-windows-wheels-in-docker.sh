#!/bin/bash

set -euo pipefail

set -x

RAY_VERSION="${RAY_VERSION:-2.32.0}"
RAY_COMMIT="${RAY_COMMIT:-607f2f30f5f21543b6a5568ee77ea779eeba30a8}"

# Download winpty
mkdir -p /c/tmp/winpty
which curl
curl -sfL https://github.com/rprichard/winpty/releases/download/0.4.3/winpty-0.4.3-msys2-2.7.0-x64.tar.gz -o /c/tmp/winpty/winpty.tar.gz
tar -xzf /c/tmp/winpty/winpty.tar.gz -C /c/tmp/winpty

# Setup verify context
mkdir -p /c/tmp/verify
cp .buildkite/release-automation/verify-windows-wheels.sh /c/tmp/verify/verify-windows-wheels.sh
cp .buildkite/release-automation/windows.verify.Dockerfile /c/tmp/verify/Dockerfile

mkdir -p /c/tmp/verify/release/util
cp release/util/sanity_check.py /c/tmp/verify/release/util/sanity_check.py

docker build -t rayproject/win-verifier /c/tmp/verify

docker create --name verifier --env RAY_VERSION --env RAY_COMMIT rayproject/win-verifier
docker start verifier
docker logs -f verifier
docker rm -f verifier

# python -c "import time; time.sleep(1000000)"
