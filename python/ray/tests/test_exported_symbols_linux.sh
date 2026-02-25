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
    actual="${TEST_TMPDIR}/raylet_exported_symbols_linux.actual.txt"
    ;;
  Darwin)
    golden="${TEST_SRCDIR}/io_ray/python/ray/tests/raylet_exported_symbols_macos.txt"
    nm_args=(-gUj)
    actual="${TEST_TMPDIR}/raylet_exported_symbols_macos.actual.txt"
    ;;
  *)
    exit 0
    ;;
esac

raylet_so=""
for candidate in \
  "${TEST_SRCDIR}/io_ray/python/ray/_raylet.so" \
  "${TEST_SRCDIR}/io_ray/_raylet.so"; do
  if [[ -f "${candidate}" ]]; then
    raylet_so="${candidate}"
    break
  fi
done
if [[ -z "${raylet_so}" ]]; then
  echo "Cannot find _raylet.so in runfiles"
  exit 1
fi

nm "${nm_args[@]}" "${raylet_so}" \
  | awk '{print $NF}' \  # keep symbol name only
  | grep -E -v '_ZN3ray|_ZNK3ray|_ZTIN3ray|_ZTVN3ray|_ZTSN3ray' \  # drop Ray symbols since we want to test the leaks of non-Ray symbols
  > "${actual}"
diff -u "${golden}" "${actual}"
