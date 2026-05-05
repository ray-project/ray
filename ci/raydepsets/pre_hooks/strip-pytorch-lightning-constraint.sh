#!/bin/bash

set -euo pipefail

# Strip pytorch-lightning and numpy from each per-version compiled lock so the
# lightning1 back-compat build can pin pytorch-lightning==1.8.6 (v1) and
# numpy==1.26.4 without conflicting with the v2 / numpy 2.x pins in the lock.
for PY_VERSION in 3.10 3.11 3.12 3.13; do
    CONSTRAINTS_FILE="/tmp/ray-deps/requirements_compiled_py${PY_VERSION}.txt"
    OUTPUT_FILE="/tmp/ray-deps/requirements_compiled_py${PY_VERSION}_no_ptl.txt"
    if [[ -f "$CONSTRAINTS_FILE" ]]; then
        sed -e '/^pytorch-lightning/d' -e '/^numpy/d' "$CONSTRAINTS_FILE" > "$OUTPUT_FILE"
    fi
done

# Strip lightning>=2 from tune-test-requirements so lightning1 back-compat
# build only has pytorch-lightning==1.8.6 (v1), not the v2 lightning package.
TUNE_TEST_REQ="python/requirements/ml/tune-test-requirements.txt"
if [[ -f "$TUNE_TEST_REQ" ]]; then
    sed '/^lightning/d' "$TUNE_TEST_REQ" > /tmp/ray-deps/tune-test-requirements-no-lightning.txt
fi
