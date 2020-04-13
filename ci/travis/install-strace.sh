#!/usr/bin/env bash

{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null  # Push caller's shell options (quietly)

set -eo pipefail && if [ -n "${OSTYPE##darwin*}" ]; then set -u; fi  # some options interfere with Travis's RVM on Mac

install_strace() {
  case "${OSTYPE}" in
    linux*)
      if ! strace -qq -k -e trace=exit /bin/true 1> /dev/null 2> /dev/null; then
        { echo "This Linux distribution doesn't appear to support strace -k." "Attempting to build & install a recent version..."; } 2> /dev/null
        git -c advice.detachedHead=false clone -q --depth=1 "https://github.com/strace/strace" -b v5.5 && (
          builtin cd strace &&
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

{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null  # Pop caller's shell options (quietly)
