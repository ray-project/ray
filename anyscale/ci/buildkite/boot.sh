#!/bin/bash

set -euo pipefail

curl -sfL "https://raw.githubusercontent.com/ray-project/rayci/stable/run_rayci.sh" > /tmp/run_rayci.sh
/bin/bash /tmp/run_rayci.sh -upload -config anyscale/ci/config.yaml