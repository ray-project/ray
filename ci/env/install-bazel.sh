#!/usr/bin/env bash

set -exuo pipefail

arg1="${1-}"

BAZELISK_VERSION="v1.16.0"

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

echo "Architecture(HOSTTYPE) is ${HOSTTYPE}"

if [[ "${BAZEL_CONFIG_ONLY-}" != "1" ]]; then
  # Sanity check: Verify we have symlinks where we expect them, or Bazel can produce weird "missing input file" errors.
  # This is most likely to occur on Windows, where symlinks are sometimes disabled by default.
  { git ls-files -s 2>/dev/null || true; } | (
    set +x
    missing_symlinks=()
    while read -r mode _ _ path; do
      if [[ "${mode}" == 120000 ]]; then
        test -L "${path}" || missing_symlinks+=("${path}")
      fi
    done
    if [[ ! 0 -eq "${#missing_symlinks[@]}" ]]; then
      echo "error: expected symlink: ${missing_symlinks[*]}" 1>&2
      echo "For a correct build, please run 'git config --local core.symlinks true' and re-run git checkout." 1>&2
      false
    fi
  )

  if [[ "${OSTYPE}" == "msys" ]]; then
    target="${MINGW_DIR-/usr}/bin/bazel.exe"
    mkdir -p "${target%/*}"
    curl -f -s -L -R -o "${target}" "https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-linux-amd64"
  else
    # Buildkite mac instances
    if [[ -n "${BUILDKITE-}" && "${platform}" == "darwin" ]]; then
      mkdir -p "$HOME/bin"
      # Add bazel to the path.
      # shellcheck disable=SC2016
      printf '\nexport PATH="$HOME/bin:$PATH"\n' >> ~/.zshenv
      # shellcheck disable=SC1090
      source ~/.zshenv
      INSTALL_USER=1
    # Buildkite linux instance
    elif [[ "${CI-}" == true || "${arg1-}" == "--system" ]]; then
      INSTALL_USER=0
    # User
    else
      mkdir -p "$HOME/bin"
      INSTALL_USER=1
      export PATH=$PATH:"$HOME/bin"
    fi

    if [[ "${HOSTTYPE}" == "aarch64" || "${HOSTTYPE}" = "arm64" ]]; then
      # architecture is "aarch64", but the bazel tag is "arm64"
      url="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-${platform}-arm64"
    elif [ "${HOSTTYPE}" = "x86_64" ]; then
      url="https://github.com/bazelbuild/bazelisk/releases/download/${BAZELISK_VERSION}/bazelisk-${platform}-amd64"
    else
      echo "Could not found matching bazelisk URL for platform ${platform} and architecture ${HOSTTYPE}"
      exit 1
    fi

    if [[ "$INSTALL_USER" == "1" ]]; then
      target="$HOME/bin/bazel"
      curl -f -s -L -R -o "${target}" "${url}"
      chmod +x "${target}"
    else
      target="/bin/bazel"
      sudo curl -f -s -L -R -o "${target}" "${url}"
      sudo chmod +x "${target}"
    fi
  fi
fi

bazel --version

# clear bazelrc
echo > ~/.bazelrc

if [[ "${CI-}" == "true" ]]; then
  # Ask bazel to anounounce the config it finds in bazelrcs, which makes
  # understanding how to reproduce bazel easier.
  echo "build --announce_rc" >> ~/.bazelrc
  echo "build --config=ci" >> ~/.bazelrc

  # In Windows CI we want to use this to avoid long path issue
  # https://docs.bazel.build/versions/main/windows.html#avoid-long-path-issues
  if [[ "${OSTYPE}" == msys ]]; then
    echo "startup --output_user_root=c:/tmp" >> ~/.bazelrc
  fi
  
  if [[ "${platform}" == darwin ]]; then
    echo "Using local disk cache on mac"
    echo "build --disk_cache=/tmp/bazel-cache" >> ~/.bazelrc
    echo "build --repository_cache=/tmp/bazel-repo-cache" >> ~/.bazelrc
  elif [[ "${BUILDKITE_BAZEL_CACHE_URL:-}" != "" ]]; then
    echo "build --remote_cache=${BUILDKITE_BAZEL_CACHE_URL}" >> ~/.bazelrc
    if [[ "${BUILDKITE_PULL_REQUEST:-false}" != "false" ]]; then
      echo "build --remote_upload_local_results=false" >> ~/.bazelrc
    fi
  fi
fi
