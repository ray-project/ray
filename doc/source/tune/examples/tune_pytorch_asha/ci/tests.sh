#!/usr/bin/env bash
set -euxo pipefail

ipython ci/tune_pytorch_asha.py
