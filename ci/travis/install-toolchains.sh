#!/usr/bin/env bash

set -euxo pipefail

LLVM_VERSION_WINDOWS="9.0.0"

install_clang() {
  if [ "${OSTYPE}" = "msys" ]; then
    export MSYS2_ARG_CONV_EXCL="*"  # Don't let MSYS2 attempt to auto-translate arguments that look like paths
    # Ideally we should be able to use the Chocolatey package manager:
    #   choco install --no-progress llvm
    # However, it frequently gives HTTP 503 errors, so we just download and install manually.
    local target_dir="${PROGRAMFILES}\LLVM"
    if ! command -v clang "${target_dir}/clang" > /dev/null; then
      local urldir="https://releases.llvm.org"
      local arch=64
      if [ "${HOSTTYPE}" = "${HOSTTYPE%64}" ]; then arch=32; fi
      local version="${LLVM_VERSION_WINDOWS}"
      local target="./LLVM-${version}-win${arch}.exe"
      if [ ! -f "${target}" ]; then
        mkdir -p -- "${target%/*}"
        curl -s -L -R -o "${target}" "http://releases.llvm.org/${version}/${target##*/}"
        chmod +x "${target}"
      fi
      if [ "${TRAVIS-}" = true ] || [ -n "${GITHUB_WORKFLOW-}" ]; then
        7z x "${target}" -o"${target_dir}"  # 7-zip is faster than the self-extracting installer; good for CI
      else
        "${target}" /S  # for normal users we should install properly
        rm -f -- "${target}"
      fi
    fi
  elif 1>&- command -v pacman; then
    sudo pacman -S --needed --noconfirm --noprogressbar clang
  elif 1>&- command -v apt-get; then
    sudo apt-get -q -y install clang
  fi
}

install_"$@"
