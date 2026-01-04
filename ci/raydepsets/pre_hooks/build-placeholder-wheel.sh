#!/bin/bash

set -exuo pipefail

export RAY_DEBUG_BUILD=deps-only

uv build --wheel --directory python/ -o ../.whl/
