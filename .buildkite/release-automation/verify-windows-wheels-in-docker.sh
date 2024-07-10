#!/bin/bash

set -euo pipefail

set -x

RAY_VERSION="${RAY_VERSION:-2.32.0}"
RAY_COMMIT="${RAY_COMMIT:-607f2f30f5f21543b6a5568ee77ea779eeba30a8}"

mkdir -p /c/tmp/verify
cp /c/workdir/.buildkite/release-automation/verify-windows-wheels.sh /c/tmp/verify/verify-windows-wheels.sh
cp /c/workdir/.buildkite/release-automation/windows.verify.Dockerfile /c/tmp/verify/Dockerfile

mkdir -p /c/tmp/verify/release/util
cp /c/workdir/release/util/sanity_check.py /c/tmp/verify/release/util/sanity_check.py

docker build -t rayproject/win-verifier /c/tmp/verify

winpty docker run -it --rm --env RAY_VERSION --env RAY_COMMIT rayproject/win-verifier

# python -c "import time; time.sleep(1000000)"
