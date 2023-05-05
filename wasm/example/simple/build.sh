#!/bin/bash

set -e

export WASI_VERSION=14
export WASI_VERSION_FULL=${WASI_VERSION}.0

# find out the operating system: darwin, linux, etc.
export OS=$(uname -s | tr '[:upper:]' '[:lower:]')
if [ "${OS}" = "darwin" ]; then
    export OS="macos"
fi

function install_sdk() {
    if ! [ -x "$(command -v wget)" ]; then
        echo "Error: wget is not installed." >&2
        exit 1
    fi

    if ! [ -x "$(command -v tar)" ]; then
        echo "Error: tar is not installed." >&2
        exit 1
    fi

    wget https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-${WASI_VERSION}/wasi-sdk-${WASI_VERSION_FULL}-${OS}.tar.gz
    tar xf wasi-sdk-${WASI_VERSION_FULL}-${OS}.tar.gz

}

# /opt/wasi-sdk/bin/clang --target=wasm32-unknown-wasi \
#     --sysroot /opt/wasi-sdk/share/wasi-sysroot/ \
#     -Wl,--export-all \
#     ./simple.c -o simple.wasm

export WASI_SDK_PATH=$(pwd -P)/wasi-sdk-${WASI_VERSION_FULL}

if [ ! -d ${WASI_SDK_PATH} ]; then
    echo "Installing WASI SDK..."
    install_sdk
    if [ ! -d ${WASI_SDK_PATH} ]; then
        echo "Error: WASI SDK not installed." >&2
        exit 1
    fi
fi

export CC="${WASI_SDK_PATH}/bin/clang --sysroot=${WASI_SDK_PATH}/share/wasi-sysroot"

$CC --target=wasm32-unknown-wasi \
    -Wl,--export-all \
    ./simple.c -o simple.wasm -nostdlib


# check if wabt is installed
if ! [ -x "$(command -v wasm2wat)" ]; then
    # warn if not installed
    echo "Warning: wabt is not installed. Please install wabt to generate wat files." >&2
    exit 0
else
    # if installed, generate wat files
    wasm2wat simple.wasm -o simple.wat
    wasm-objdump -dhs simple.wasm > simple.objdump
fi
