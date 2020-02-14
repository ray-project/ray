#!/usr/bin/env bash

set -euo pipefail

install_clang() {
  if [ "${OSTYPE}" = "msys" ]; then
    export MSYS2_ARG_CONV_EXCL="*"  # Don't let MSYS2 attempt to auto-translate arguments that look like paths
    # Ideally we should be able to use the Chocolatey package manager:
    #   choco install --no-progress llvm
    # However, it frequently gives HTTP 503 errors, so we just download and install manually.
    urldir="https://releases.llvm.org"
    arch=64
    if [ "${HOSTTYPE}" = "${HOSTTYPE%64}" ]; then arch=32; fi
    local version
    version="${LLVM_VERSION_WINDOWS}"
    target="./LLVM-${version}-win${arch}.exe"
    curl -s -L -R -o "${target}" "http://releases.llvm.org/${version}/${target##*/}"
    chmod +x "${target}"
    "${target}" /S
    rm -f -- "${target}"
  elif 1>&- command -v pacman; then
    sudo pacman -S --needed --noconfirm --noprogressbar clang
  elif 1>&- command -v apt-get; then
    sudo apt-get -q -y install clang
  fi
}

install_"$@"
