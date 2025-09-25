#!/bin/bash

set -euxo pipefail

uv pip install -r python_depset.lock --system --no-deps --index-strategy unsafe-best-match

jupyter execute e2e_timeseries/01-Distributed-Training.ipynb e2e_timeseries/02-Validation.ipynb e2e_timeseries/03-Serving.ipynb
