#!/bin/bash
# This script is used to setup test environment for running core tests.

set -exo pipefail

DL=1 ./ci/env/install-dependencies.sh
