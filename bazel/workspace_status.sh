#!/bin/bash

set -euo pipefail

if [[ "${USER:-}" =~ "@" ]]; then
	echo "ERROR: \$USER ('${USER:-}') contains invalid char '@'" >&2
    exit 1
fi

if [[ "${HOME:-}" =~ "@" ]]; then
	echo "ERROR: \$HOME ('${HOME:-}') contains invalid char '@'" >&2
    exit 1
fi
