#!/bin/bash

set -exo pipefail

WORKING_DIR=/home/ray/in_image_working_dir
PY_MODULES_DIR=/home/ray/in_image_py_modules

mkdir -p "$WORKING_DIR/in_image_module" "$PY_MODULES_DIR/in_image_py_module"

echo -n "baked-into-the-image" > "$WORKING_DIR/marker"

cat > "$WORKING_DIR/in_image_module/__init__.py" <<'EOF'
from in_image_module.value import get_value
EOF

cat > "$WORKING_DIR/in_image_module/value.py" <<'EOF'
def get_value():
    return 42
EOF

cat > "$PY_MODULES_DIR/in_image_py_module/__init__.py" <<'EOF'
from in_image_py_module.value import get_value
EOF

cat > "$PY_MODULES_DIR/in_image_py_module/value.py" <<'EOF'
def get_value():
    return 7
EOF
