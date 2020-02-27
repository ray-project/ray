#!/usr/bin/env bash

set -eu

case "$(uname -s)" in
  Linux*)
    strace -qq -k -e trace=exit /bin/true || {
      echo "This Linux distribution doesn't appear to support strace -k." "Attempting to build & install a recent version..." 1>&2
      git clone -q --depth=1 "https://github.com/strace/strace" -b v5.5 && (
        cd strace &&
        ./bootstrap > /dev/null &&
        ./configure --quiet --with-libunwind --enable-mpers=no &&
        make -s -j"$(getconf _NPROCESSORS_ONLN || echo 1)" &&
        sudo make install
      )
    }
    ;;
  *)
    # Unable to install on other platforms
    false
    ;;
esac
