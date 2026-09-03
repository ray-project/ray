#!/bin/bash

set -exo pipefail

WORKING_DIR=/home/ray/in_image_working_dir

mkdir -p "$WORKING_DIR/in_image_module"

echo -n "baked-into-the-image" > "$WORKING_DIR/marker"

cat > "$WORKING_DIR/in_image_module/__init__.py" <<'EOF'
from in_image_module.value import get_value
EOF

cat > "$WORKING_DIR/in_image_module/value.py" <<'EOF'
def get_value():
    return 42
EOF
