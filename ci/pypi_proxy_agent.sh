#!/bin/bash
# Configure the PyPI index for the steps that no image can reach. Sourced from
# .buildkite/hooks/pre-command; it exports and must not be run.
#
# The images cover steps that run inside them: forge and manylinux carry
# ci/pypi_proxy_profile.sh as an /etc/profile.d hook. What they cannot cover is
# anything that never enters a container -- the release pipeline's init step, and
# every wanda image build, which run directly on the agent. The decision itself
# lives in the shared profile script; this file only locates it in the checkout.
_rayci_agent_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=ci/pypi_proxy_profile.sh
source "${_rayci_agent_repo_root}/ci/pypi_proxy_profile.sh"
unset _rayci_agent_repo_root
