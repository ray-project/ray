#!/bin/bash

printBuildkiteHeader() {
    printf -- '--- %s\n' "$@"
}

printInfo() {
    printf '\033[32mINFO:\033[0m %s\n' "$@"
}

printError() {
    printf '\033[31mERROR:\033[0m %s\n' "$@"
}

# Normalize architecture to supported architectures.
# Currently supported architectures: x86_64
normalize_arch() {
    local arch="${1:-$(uname -m)}"
    case "${arch}" in
        x86_64|amd64)
            echo "x86_64"
            ;;
        *)
            printError "Unsupported architecture: $arch"
            exit 1
            ;;
    esac
}

validate_file_exists() {
    local file="$1"
    
    if [[ ! -f "$file" ]]; then
        printError "$file not found"
        exit 1
    fi
}

