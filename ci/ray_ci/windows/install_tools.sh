#!/bin/bash

set -ex

# Delete the existing bazel and replace it with bazelisk.
powershell ci/ray_ci/windows/install_bazelisk.ps1

powershell ci/pipeline/fix-windows-container-networking.ps1
pip install awscli

# Install the driver's deps into the agent's Python 3.8 env (strip hashes for pip).
no_hashes="$(mktemp)"
sed 's/ \\$//; s/ --hash[^ ]*//g' python/deplocks/ci/ci_windows_depset.lock > "$no_hashes"
pip install -r "$no_hashes" --no-deps
rm -f "$no_hashes"
