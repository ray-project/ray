#!/bin/bash -x

# /opt/wasi-sdk/bin/clang --target=wasm32-unknown-wasi \
#     --sysroot /opt/wasi-sdk/share/wasi-sysroot/ \
#     -Wl,--export-all \
#     ./simple.c -o simple.wasm
/opt/wasi-sdk/bin/clang --target=wasm32-unknown-wasi \
    -Wl,--export-all \
    ./simple.c -o simple.wasm -nostdlib
wasm2wat simple.wasm -o simple.wat
wasm-objdump -dhs simple.wasm > simple.objdump
