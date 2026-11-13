#!/bin/bash
# Pins pyarrow back to the version the data release images used before #68005,
# to A/B the random_shuffle regression against the pyarrow 25 baseline.

set -exo pipefail

pip3 install --no-cache-dir "pyarrow==23.0.1"
