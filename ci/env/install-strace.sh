#!/usr/bin/env bash

set -euxo pipefail

install_strace() {
  case "${OSTYPE}" in
    linux*)
      if ! strace -qq -k -e trace=exit /bin/true 2> /dev/null; then
        (
          set +x
          echo "This Linux distribution doesn't appear to support strace -k." \
            "Attempting to build & install a recent version..." 1>&2
          git -c advice.detachedHead=false clone -q --depth=1 -b v5.5 \
            "https://github.com/strace/strace"
          cd strace
          ./bootstrap
          CPPFLAGS="-w ${CPPFLAGS-}" ./configure --quiet --with-libunwind --enable-mpers=no
          make -s -j"$(getconf _NPROCESSORS_ONLN || echo 1)"
          sudo make -s install
        )
      fi > /dev/null;;
    *) false;;
  esac
}

install_strace "$@"
