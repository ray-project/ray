#!/bin/bash
# Point this macOS job's pip and uv at the CI package mirror. Source it, do not
# run it: it exports the index variables into the calling shell.
#
# macOS steps run directly on the agent, not in a container, so there is no
# /etc/profile.d being sourced; this entry point exists only to reach the shared
# decision script from the checkout. The probe, the mode selection, and the
# fail-open paths all live there, and an agent that cannot reach the mirror's
# hosted index resolves from public PyPI exactly as before.
_rayci_macos_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
# shellcheck source=ci/pypi_proxy_profile.sh
source "${_rayci_macos_repo_root}/ci/pypi_proxy_profile.sh"
unset _rayci_macos_repo_root
