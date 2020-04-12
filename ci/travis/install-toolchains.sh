#!/usr/bin/env bash

{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null  # Push caller's shell options (quietly)

set -eo pipefail && if [ -n "${OSTYPE##darwin*}" ]; then set -u; fi  # some options interfere with Travis's RVM on Mac

LLVM_VERSION="9.0.0"

install_toolchains() {
  local osversion="" url="" urlbase="https://releases.llvm.org" targetdir="/usr/local"
  case "${OSTYPE}" in
    msys)
      export MSYS2_ARG_CONV_EXCL="*"  # Don't let MSYS2 attempt to auto-translate arguments that look like paths
      osversion=win
      if [ "${HOSTTYPE}" != "${HOSTTYPE%64}" ]; then
        osversion="${osversion}64"
      else
        osversion="${osversion}32"
      fi
      url="${urlbase}/${LLVM_VERSION}/LLVM-${LLVM_VERSION}-${osversion}.exe"
      targetdir="${PROGRAMFILES}\LLVM"
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
  curl -s -L -R "${url}" | if [ "${OSTYPE}" = "msys" ]; then
    local target="./${url##*/}"
    install /dev/stdin "${target}"
    7z x -bsp0 -bso0 "${target}" -o"${targetdir}"
    rm -f -- "${target}"
  else
    sudo tar -x -J --strip-components=1 -C "${targetdir}"
    command -V clang 1>&2
  fi
  "${targetdir}"/bin/clang --version 1>&2
}

install_toolchains "$@"

{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null  # Pop caller's shell options (quietly)
