#!/usr/bin/env bash

set -euxo pipefail

LLVM_VERSION="9.0.0"  # This is not necessarily guaranteed (e.g. we might use the system compiler)

install_clang() {
  local cc="clang++" osversion="" url="" urlbase="https://releases.llvm.org" targetdir="/usr/local"
  case "${OSTYPE}" in
    msys)
      osversion=win
      if [ "${HOSTTYPE}" != "${HOSTTYPE%64}" ]; then
        osversion="${osversion}64"
      else
        osversion="${osversion}32"
      fi
      url="${urlbase}/${LLVM_VERSION}/LLVM-${LLVM_VERSION}-${osversion}.exe"
      cc="clang-cl"
      ;;
    linux-gnu)
      osversion="${OSTYPE}-$(sed -n -e '/^PRETTY_NAME/ { s/^[^=]*="\(.*\)"/\1/g; s/ /-/; s/\([0-9]*\.[0-9]*\)\.[0-9]*/\1/; s/ .*//; p }' /etc/os-release | tr '[:upper:]' '[:lower:]')"
      ;;
    darwin*)
      osversion="darwin-apple"
      ;;
  esac
  if [ -z "${url}" ]; then
    url="${urlbase}/${LLVM_VERSION}/clang+llvm-${LLVM_VERSION}-${HOSTTYPE}-${osversion}.tar.xz"
  fi
  if ! command -v "${cc}"; then
    case "${osversion}" in
      linux-gnu-ubuntu*)
        sudo apt-get install -qq -o=Dpkg::Use-Pty=0 install clang clang-format clang-tidy
        ;;
      *)  # Fallback for all platforms is to download from LLVM's site, but avoided until necessary
        local target="./${url##*/}"
        curl -f -s -L -R --show-error -o "${target}" "${url}"
        if [ "${OSTYPE}" = "msys" ]; then
          mkdir -p -- "${targetdir}"
          7z x -bsp0 -bso0 "${target}" -o"${targetdir}"
          MSYS2_ARG_CONV_EXCL="*" Reg Add "HKLM\SOFTWARE\LLVM\LLVM" /ve /t REG_SZ /f /reg:32 \
            /d "$(cygpath -w -- "${targetdir}")" > /dev/null
          rm -f -- "${target}"
        else
          sudo tar -x -J --strip-components=1 -f "${target}" -C "${targetdir}"
        fi
        ;;
    esac
  fi
  "${cc}" --version
}

install_clang "$@"
