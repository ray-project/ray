#!/bin/bash

check_docstyle() {
    echo "Checking docstyle..."
    violations=$(echo "$@" | xargs grep -E '^[ ]+[a-z_]+ ?\([a-zA-Z]+\): ' | grep -v 'str(' | grep -v noqa || true)
    if [[ -n "$violations" ]]; then
        echo
        echo "=== Found Ray docstyle violations ==="
        echo "$violations"
        echo
        echo "Per the Google pydoc style, omit types from pydoc args as they are redundant: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#code-style "
        echo "If this is a false positive, you can add a '# noqa' comment to the line to ignore."
        exit 1
    fi
    return 0
}

check_docstyle "$@"
