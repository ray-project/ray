#!/bin/bash
# Point this job's Python package resolution at AWS CodeArtifact instead of
# pypi.org, so that a files.pythonhosted.org 502 cannot fail a build.
#
# CodeArtifact's simple index hands out artifact URLs relative to its own
# endpoint, so both requests pip makes -- the project page and the wheel itself
# -- resolve against CodeArtifact. A path-prefix byte cache only ever gets the
# first one, because PyPI's index body names files.pythonhosted.org absolutely.
#
# This script must be SOURCED, not executed: its whole purpose is the exports.
#
# It is deliberately fail-open. It probes the index and exports nothing unless
# that probe succeeds, so a missing IAM permission, an expired token or an
# unreachable endpoint leaves the job resolving from PyPI exactly as it does
# today, with the reason printed. Nothing here makes CI depend on CodeArtifact
# being available.

# The caller (.buildkite/hooks/pre-command) runs under `set -x`, and this
# function handles a bearer token. Keep it out of the build log.
_rayci_codeartifact_setup() {
  local had_xtrace=0
  case "$-" in *x*) had_xtrace=1; set +x ;; esac

  _rayci_ca_finish() {
    [[ "${had_xtrace}" == "1" ]] && set -x
    return 0
  }

  if [[ "${RAYCI_CODEARTIFACT_DISABLE:-0}" == "1" ]]; then
    echo "codeartifact: disabled by RAYCI_CODEARTIFACT_DISABLE, using PyPI"
    _rayci_ca_finish; return 0
  fi

  local domain="${RAYCI_CODEARTIFACT_DOMAIN:-ray-ci-scratch}"
  local owner="${RAYCI_CODEARTIFACT_OWNER:-029272617770}"
  local repo="${RAYCI_CODEARTIFACT_REPO:-pypi-store}"
  local region="${RAYCI_CODEARTIFACT_REGION:-us-west-2}"

  if ! command -v aws >/dev/null 2>&1; then
    echo "codeartifact: no aws CLI on this agent, using PyPI"
    _rayci_ca_finish; return 0
  fi

  local host="${domain}-${owner}.d.codeartifact.${region}.amazonaws.com"
  local index="https://${host}/pypi/${repo}/simple/"

  local token
  if ! token="$(aws codeartifact get-authorization-token \
        --domain "${domain}" --domain-owner "${owner}" --region "${region}" \
        --query authorizationToken --output text 2>&1)"; then
    echo "codeartifact: could not mint a token, using PyPI"
    # Print why. The AWS CLI puts blank lines around its errors, so take the
    # first line that actually says something.
    echo "${token}" | grep -m1 -v '^[[:space:]]*$' | sed 's/^/codeartifact: /' || true
    _rayci_ca_finish; return 0
  fi

  # The credential goes in a netrc rather than in the URL. pip and uv both read
  # $NETRC, which keeps the token out of the environment, out of `docker run`
  # arguments and out of anything that echoes a command line. It also avoids
  # depending on $HOME, which differs between the agent and the unprivileged
  # user that Bazel runs as inside the test containers.
  #
  # The URL itself must carry no userinfo at all. A half-credentialled
  # https://aws@host/ is worse than useless: requests derives the netrc lookup
  # key as netloc.split(":")[0], which strips the port but not the userinfo, so
  # it searches for a machine literally named "aws@<host>", misses, and pip then
  # coerces the absent password to "" and authenticates as aws with an empty
  # password -- a 401 that looks exactly like a bad token.
  local dir=/tmp/rayci-codeartifact
  local netrc="${dir}/netrc"
  mkdir -p "${dir}"
  chmod 700 "${dir}"
  ( umask 077; printf 'machine %s\nlogin aws\npassword %s\n' "${host}" "${token}" > "${netrc}" )
  chmod 600 "${netrc}"

  # Fail-open gate: only redirect the job if the index actually answers.
  local code
  code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 20 \
            --netrc-file "${netrc}" \
            -H 'Accept: application/vnd.pypi.simple.v1+json' \
            "${index}tabulate/" 2>/dev/null || echo 000)"
  if [[ "${code}" != "200" ]]; then
    echo "codeartifact: index probe returned HTTP ${code}, using PyPI"
    rm -f "${netrc}"
    _rayci_ca_finish; return 0
  fi

  export NETRC="${netrc}"
  export PIP_INDEX_URL="${index}"
  export UV_INDEX_URL="${index}"
  # rules_python runs whl_library's pip with --isolated, which makes pip ignore
  # every PIP_* variable. Without this the export above is silently discarded
  # and the repository rule resolves from PyPI while looking configured.
  export RULES_PYTHON_PIP_ISOLATED=0

  echo "codeartifact: index probe OK, resolving from ${index}"
  _rayci_ca_finish; return 0
}

_rayci_codeartifact_setup
unset -f _rayci_codeartifact_setup _rayci_ca_finish
