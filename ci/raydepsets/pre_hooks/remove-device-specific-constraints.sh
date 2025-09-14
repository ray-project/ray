#!/bin/bash

set -euo pipefail

sed "/[0-9]\+cpu/d;/[0-9]\+pt/d" "python/requirements_compiled.txt" > python/requirements_compiled_gpu.txt
