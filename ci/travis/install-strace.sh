#!/usr/bin/env bash

set -euxo pipefail

install_strace() {
  case "${OSTYPE}" in
    linux*)
      if ! strace -qq -k -e trace=exit /bin/true 1> /dev/null 2> /dev/null; then
        { echo "This Linux distribution doesn't appear to support strace -k." "Attempting to build & install a recent version..."; } 2> /dev/null
        git -c advice.detachedHead=false clone -q --depth=1 "https://github.com/strace/strace" -b v5.5 && (
          cd strace &&
          ./bootstrap > /dev/null &&
          CPPFLAGS="-w ${CPPFLAGS-}" ./configure --quiet --with-libunwind --enable-mpers=no &&
          make -s -j"$(getconf _NPROCESSORS_ONLN || echo 1)" &&
          sudo make -s install
        ) > /dev/null
      fi;;
    *) false;;
  esac
}

install_strace "$@"
