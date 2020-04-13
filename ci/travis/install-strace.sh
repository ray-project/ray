#!/usr/bin/env bash

set -eu

case "$(uname -s)" in
  Linux*)
    strace -qq -k -e trace=exit /bin/true 1> /dev/null 2> /dev/null || {
      echo "This Linux distribution doesn't appear to support strace -k." "Attempting to build & install a recent version..." 1>&2
      git -c advice.detachedHead=false clone -q --depth=1 "https://github.com/strace/strace" -b v5.5 && (
        cd strace &&
        ./bootstrap > /dev/null &&
        CPPFLAGS="-w ${CPPFLAGS-}" ./configure --quiet --with-libunwind --enable-mpers=no &&
        make -s -j"$(getconf _NPROCESSORS_ONLN || echo 1)" &&
        sudo make -s install
      )
    }
    ;;
  *)
    # Unable to install on other platforms
    false
    ;;
esac
