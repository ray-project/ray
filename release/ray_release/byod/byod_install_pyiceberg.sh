#!/bin/bash
# shellcheck disable=SC2102

set -exo pipefail

pip3 install --no-cache-dir "pyiceberg[glue,s3fs]==0.11.0"
