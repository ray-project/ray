#!/usr/bin/env bash

set -euxo pipefail

LLVM_VERSION="9.0.0"

install_toolchains() {
  local osversion="" url="" urlbase="https://releases.llvm.org" targetdir="/usr/local"
  case "${OSTYPE}" in
    msys)
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
    (
      # Add Clang/LLVM binaries to somewhere that's in already PATH
      # (don't change PATH itself, to avoid invalidating Bazel's cache or having to manage environment variables)
      mkdir -p -- ~/bin
      set +x
      local path
      for path in "${targetdir}\\bin"/*.exe; do
        local name="${path##*/}"
        printf "%s\n" "#!/usr/bin/env bash" "exec \"${path}\" \"\$@\"" | install /dev/stdin ~/bin/"${name%.*}"
      done
    )
  else
    sudo tar -x -J --strip-components=1 -C "${targetdir}"
    command -V clang 1>&2
  fi
  "${targetdir}"/bin/clang --version 1>&2
}

install_toolchains "$@"
