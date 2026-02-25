#!/usr/bin/env bash
set -euo pipefail

if ! command -v nm >/dev/null 2>&1; then
  echo "nm not found on PATH"
  exit 1
fi

os="$(uname -s)"
case "${os}" in
  Linux)
    golden="${TEST_SRCDIR}/io_ray/python/ray/tests/raylet_exported_symbols_linux.txt"
    nm_args=(-D --defined-only -g)
    ;;
  Darwin)
    golden="${TEST_SRCDIR}/io_ray/python/ray/tests/raylet_exported_symbols_macos.txt"
    nm_args=(-gUj)
    ;;
  *)
    exit 0
    ;;
esac

raylet_so="${TEST_SRCDIR}/io_ray/python/ray/_raylet.so"
if [[ ! -f "${raylet_so}" ]]; then
  echo "Cannot find _raylet.so in runfiles"
  exit 1
fi

symbols_non_ray="${TEST_TMPDIR}/raylet_exported_symbols.non_ray.txt"
symbols_original="${TEST_TMPDIR}/raylet_exported_symbols.original.txt"
nm "${nm_args[@]}" "${raylet_so}" | awk '{print $NF}' > "${symbols_original}"  # keep symbol names only
grep -E -v '_ZN3ray|_ZNK3ray|_ZTIN3ray|_ZTVN3ray|_ZTSN3ray' "${symbols_original}" > "${symbols_non_ray}"  # drop Ray mangled symbols; check only non-Ray leaks
diff -u "${golden}" "${symbols_non_ray}"
