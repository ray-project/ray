#!/bin/bash
# Force pandas 3.0 into the test image, overriding the pandas 2.x pinned by
# requirements_compiled. Used by the iter_batches_pandas release test to
# benchmark the Arrow->pandas path under pandas 3.0 (requires Python >= 3.11).

set -exo pipefail

pip3 install --no-cache-dir --upgrade "pandas==3.0.0"
python3 -c "import pandas; print('pandas', pandas.__version__)"
