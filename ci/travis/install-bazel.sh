#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

version="3.2.0"
achitecture="${HOSTTYPE}"
platform="unknown"
case "${OSTYPE}" in
  msys)
    echo "Platform is Windows."
    platform="windows"
    # No installer for Windows
    ;;
  darwin*)
    echo "Platform is Mac OS X."
    platform="darwin"
    ;;
  linux*)
    echo "Platform is Linux (or WSL)."
    platform="linux"
    ;;
  *)
    echo "Unrecognized platform."
    exit 1
esac

# Sanity check: Verify we have symlinks where we expect them, or Bazel can produce weird "missing input file" errors.
# This is most likely to occur on Windows, where symlinks are sometimes disabled by default.
{ git ls-files -s || true; } | {
  missing_symlinks=()
  while read -r mode digest sn path; do
    if [ "${mode}" = 120000 ]; then
      test -L "${path}" || missing_symlinks+=("${paths}")
    fi
  done
  if [ ! 0 -eq "${#missing_symlinks[@]}" ]; then
    echo "error: expected symlink: ${missing_symlinks[@]}" 1>&2
    echo "For a correct build, please run 'git config --local core.symlinks true' and re-run git checkout." 1>&2
    false
  fi
}

if [ "${OSTYPE}" = "msys" ]; then
  target="${MINGW_DIR-/usr}/bin/bazel.exe"
  mkdir -p "${target%/*}"
  curl -f -s -L -R -o "${target}" "https://github.com/bazelbuild/bazel/releases/download/${version}/bazel-${version}-${platform}-${achitecture}.exe"
  tee /etc/profile.d/bazel.sh > /dev/null <<EOF; . /etc/profile.d/bazel.sh
export USE_CLANG_CL=1  # Clang front-end for Visual C++
EOF
else
  target="./install.sh"
  curl -f -s -L -R -o "${target}" "https://github.com/bazelbuild/bazel/releases/download/${version}/bazel-${version}-installer-${platform}-${achitecture}.sh"
  chmod +x "${target}"
  if [ "${CI-}" = true ]; then
    sudo "${target}" > /dev/null  # system-wide install for CI
    command -V bazel 1>&2
  else
    "${target}" --user > /dev/null
  fi
  rm -f "${target}"
fi

for bazel_cfg in ${BAZEL_CONFIG-}; do
  echo "build --config=${bazel_cfg}" >> ~/.bazelrc
done
if [ "${TRAVIS-}" = true ]; then
  echo "build --config=ci-travis" >> ~/.bazelrc

  # If we are in Travis, most of the compilation result will be cached.
  # This means we are I/O bounded. By default, Bazel set the number of concurrent
  # jobs to the the number cores on the machine, which are not efficient for
  # network bounded cache downloading workload. Therefore we increase the number
  # of jobs to 50
  # NOTE: Normally --jobs should be under 'build:ci-travis' in .bazelrc, but we put
  # it under 'build' here avoid conflicts with other --config options.
  echo "build --jobs=50" >> ~/.bazelrc
fi
if [ "${GITHUB_ACTIONS-}" = true ]; then
  echo "build --config=ci-github" >> ~/.bazelrc
fi
if [ "${CI-}" = true ]; then
  echo "build --config=ci" >> ~/.bazelrc
  # If we are in master build, we can write to the cache as well.
  upload=0
  if [ "${TRAVIS_PULL_REQUEST-false}" = false ]; then
    if [ -n "${BAZEL_CACHE_CREDENTIAL_B64:+x}" ]; then
      {
        printf "%s" "${BAZEL_CACHE_CREDENTIAL_B64}" | base64 -d - >> "${HOME}/bazel_cache_credential.json"
      } 2>&-  # avoid printing secrets
      upload=1
    elif [ -n "${encrypted_1c30b31fe1ee_key:+x}" ]; then
      {
        openssl aes-256-cbc -K "${encrypted_1c30b31fe1ee_key}" \
            -iv "${encrypted_1c30b31fe1ee_iv}" \
            -in "${ROOT_DIR}/bazel_cache_credential.json.enc" \
            -out "${HOME}/bazel_cache_credential.json" -d
      } 2>&-  # avoid printing secrets
      if [ 0 -eq $? ]; then
        upload=1
      fi
    fi
  fi
  if [ 0 -ne "${upload}" ]; then
    translated_path=~/bazel_cache_credential.json
    if [ "${OSTYPE}" = msys ]; then  # On Windows, we need path translation
      translated_path="$(cygpath -m -- "${translated_path}")"
    fi
    cat <<EOF >> ~/.bazelrc
build --google_credentials="${translated_path}"
EOF
  else
    echo "Using remote build cache in read-only mode." 1>&2
    cat <<EOF >> ~/.bazelrc
build --remote_upload_local_results=false
EOF
  fi
fi
