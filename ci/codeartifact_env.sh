#!/bin/bash
# Point CI's Python package resolution at AWS CodeArtifact instead of pypi.org,
# so that a files.pythonhosted.org 502 cannot fail a build.
#
# CodeArtifact's simple index hands out artifact URLs relative to its own
# endpoint, so both requests pip makes -- the project page and the wheel itself
# -- resolve against CodeArtifact. A path-prefix byte cache only ever gets the
# first one, because PyPI's index body names files.pythonhosted.org absolutely.
#
# This runs on the agent, but the fetches happen two layers in: the step's
# command runs in a container started by the Buildkite docker plugin, whose
# --env list is fixed and carries nothing about the index, and that container
# starts the nested test container. Exporting here therefore cannot reach them.
# What crosses the boundary is the checkout, which is bind-mounted into the step
# container, so the configuration is written there as two files:
#
#   .rayci-codeartifact.env  index settings, read by /etc/profile.d in the image
#   .rayci-netrc             the credential, mode 0600
#
# Both are gitignored and excluded from docker build contexts, so neither is
# committed nor baked into an image layer. The nested container gets the netrc
# bind-mounted by ci/ray_ci/linux_container.py, since its workspace is COPYed
# into an image rather than mounted.
#
# Fail-open throughout: it probes the index and writes nothing unless that probe
# succeeds, so a missing IAM permission, an expired token or an unreachable
# endpoint leaves the build resolving from PyPI exactly as it does today, with
# the reason printed.

_rayci_codeartifact_setup() {
  # The caller runs under `set -x` and this handles a bearer token. Keep it out
  # of the build log.
  local had_xtrace=0
  case "$-" in *x*) had_xtrace=1; set +x ;; esac

  local checkout="${RAYCI_CHECKOUT_DIR:-$PWD}"
  local env_file="${checkout}/.rayci-codeartifact.env"
  local netrc_file="${checkout}/.rayci-netrc"

  _rayci_ca_finish() {
    # Never leave a stale credential behind for the next job on a reused agent.
    if [[ "${1:-keep}" == "clear" ]]; then
      rm -f "${env_file}" "${netrc_file}"
    fi
    [[ "${had_xtrace}" == "1" ]] && set -x
    return 0
  }

  if [[ "${RAYCI_CODEARTIFACT_DISABLE:-0}" == "1" ]]; then
    echo "codeartifact: disabled by RAYCI_CODEARTIFACT_DISABLE, using PyPI"
    _rayci_ca_finish clear; return 0
  fi

  local domain="${RAYCI_CODEARTIFACT_DOMAIN:-ray-ci-scratch}"
  local owner="${RAYCI_CODEARTIFACT_OWNER:-029272617770}"
  local repo="${RAYCI_CODEARTIFACT_REPO:-pypi-store}"
  local region="${RAYCI_CODEARTIFACT_REGION:-us-west-2}"

  if ! command -v aws >/dev/null 2>&1; then
    echo "codeartifact: no aws CLI on this agent, using PyPI"
    _rayci_ca_finish clear; return 0
  fi

  local host="${domain}-${owner}.d.codeartifact.${region}.amazonaws.com"
  local index="https://${host}/pypi/${repo}/simple/"

  local token
  if ! token="$(aws codeartifact get-authorization-token \
        --domain "${domain}" --domain-owner "${owner}" --region "${region}" \
        --query authorizationToken --output text 2>&1)"; then
    echo "codeartifact: could not mint a token, using PyPI"
    # The AWS CLI puts blank lines around its errors; print the first line that
    # actually says something.
    echo "${token}" | grep -m1 -v '^[[:space:]]*$' | sed 's/^/codeartifact: /' || true
    _rayci_ca_finish clear; return 0
  fi

  ( umask 077; printf 'machine %s\nlogin aws\npassword %s\n' "${host}" "${token}" > "${netrc_file}" )
  chmod 600 "${netrc_file}"

  # Fail-open gate: only redirect the build if the index actually answers.
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 20 \
            --netrc-file "${netrc_file}" \
            -H 'Accept: application/vnd.pypi.simple.v1+json' \
            "${index}tabulate/" 2>/dev/null || echo 000)"
  if [[ "${code}" != "200" ]]; then
    echo "codeartifact: index probe returned HTTP ${code}, using PyPI"
    _rayci_ca_finish clear; return 0
  fi

  # rules_python runs whl_library's pip with --isolated, which makes pip ignore
  # every PIP_* variable. Without RULES_PYTHON_PIP_ISOLATED=0 the index setting
  # is silently discarded and the repository rule resolves from PyPI while
  # looking configured.
  #
  # UV_INDEX_URL is deliberately NOT set. It would work -- uv overrides the
  # --index-url embedded in the deplock files, unlike pip -- but it also reaches
  # the uv that Ray spawns for uv-based runtime environments, and those
  # subprocesses do not carry the credential. They then hit an authenticated
  # index anonymously: //python/ray/tests:test_runtime_env_uv{,_run} produced 80
  # 401s and timed out at 915s. Redirecting uv needs the credential to follow it
  # into the runtime env first, which is its own change. Absent the variable, uv
  # follows the index pinned in each lock file, as it does today.
  cat > "${env_file}" <<EOF
PIP_INDEX_URL=${index}
RULES_PYTHON_PIP_ISOLATED=0
EOF
  chmod 644 "${env_file}"

  # Also export on the agent, for the few steps that run outside a container.
  export PIP_INDEX_URL="${index}"
  export RULES_PYTHON_PIP_ISOLATED=0
  export NETRC="${netrc_file}"

  echo "codeartifact: index probe OK, resolving from ${index}"
  _rayci_ca_finish; return 0
}

_rayci_codeartifact_setup
unset -f _rayci_codeartifact_setup _rayci_ca_finish
