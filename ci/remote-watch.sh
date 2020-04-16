#!/usr/bin/env bash

get_current_ci() {
  local ci=""
  ci="${ci:-${TRAVIS+travis}}"  # 'travis'
  ci="${ci:-${GITHUB_WORKFLOW+github}}"  # 'github'
  printf "%s" "${ci}"
}

# Watches the remote branch for changes, passing the given PID to the provided command-line when a change is detected.
# To bypass this, put a line like "CI_KEEP_ALIVE: travis, github" (or just "CI_KEEP_ALIVE" by itself) in the commit message,
# and keep the commit available on the remote branch.
# Usage: remote_watch PID [kill-command...]
remote_watch() {
  local pid
  pid="$1"
  shift
  local command=("$@") ci
  if ! kill -0 "${pid}" 2> /dev/null; then
    echo "Error: Invalid PID: '${pid}'" 1>&2
    return 1
  fi
  if [ "${#command[@]}" -eq 0 ]; then
    command=("${SHELL}" -c 'set -x && kill -INT "$@" && sleep 5 && kill -TERM "$@" && sleep 15 && kill -KILL "$@";' -)
  fi
  ci="$(get_current_ci)"
  if [ -z "${REMOTE_WATCH_DAEMON_JOB_ID-}" ]; then
    local server_poll_interval=60 remote ref expected_sha="" expected
    ref="$(git for-each-ref --format="%(upstream:short)" "$(git symbolic-ref -q HEAD)")"  # Get remote tracking branch
    if [ -n "${ref}" ]; then
      remote="${ref%%/*}"
      ref="refs/heads/${ref#${remote}/}"
    else
      remote="$(git remote show -n | head -n 1)"
      ref="${TRAVIS_PULL_REQUEST_BRANCH-}"
      if [ -n "${ref}" ]; then
        if [ "${TRAVIS_EVENT_TYPE}" = pull_request ]; then
          ref="refs/pull/${TRAVIS_PULL_REQUEST}/merge"
        else
          ref="refs/heads/${TRAVIS_PULL_REQUEST}"
        fi
        expected_sha="${TRAVIS_COMMIT-}"
      else
        ref="${CI_REF:-}"
      fi
    fi
    if [ -z "${remote}" ]; then
      echo "Error: Invalid remote: '${remote}'" 1>&2
      return 1
    fi
    if [ -z "${ref}" ]; then
      echo "Error: Invalid ref: '${ref}'" 1>&2
      return 1
    fi
    if [ -z "${expected_sha}" ]; then
      expected_sha="$(git rev-parse --verify HEAD)"
    fi
    expected="$(printf "%s\t%s" "${expected_sha}" "${ref}")"
    local process_poll_interval=1 slept_so_far="${server_poll_interval}" should_stay_alive=0
    case "$(git show -s --format="%B" "HEAD^-" | sed -n 's/^[# ]*CI_KEEP_ALIVE\(:\(.*\)\)*$/,\2,/p' | sed "s/[ ]//g")" in
      *,"${ci}",*) should_stay_alive=1;;
      ,,) should_stay_alive=1;;
    esac
    (
      { set +x; } 2> /dev/null
      echo "Launched background job to detect changes in ${ref} every ${server_poll_interval} seconds: ${expected}" 1>&2
      local terminate=0
      while true; do
        if ! kill -0 "${pid}" 2> /dev/null; then
          break
        fi
        local verified_alive=0
        if [ "${server_poll_interval}" -le "${slept_so_far}" ]; then
          if [ "${should_stay_alive}" -eq 0 ]; then
            local status=0 line=""
            line="$(git ls-remote --exit-code "${remote}" "${ref}")" || status=$?
            if [ "${status}" = 0 ]; then
              verified_alive=1
            fi
            if [ "${status}" = 0 ] && [ "${line}" != "${expected}" ] || [ "${status}" = 2 ]; then
              terminate=1
              printf "Terminating job because %s has changed\n  from:\t%s\n  to:  \t%s\n" "${ref}" "${expected}" "${line}" 1>&2
              break
            fi
          fi
          if [ "${verified_alive}" -eq 0 ]; then  # only try fetch if we couldn't verify with ls-remote
            if git fetch "${remote}" "${expected_sha}"; then
              verified_alive=1
            else
              terminate=1
              printf "Terminating job because commit has disappeared from '%s' on '%s', likely due to a force-push:\n\t%s\n" "${ref}" "${expected}" 1>&2
              break
            fi
          fi
          slept_so_far=0
        fi
        sleep "${process_poll_interval}"
        slept_so_far=$((slept_so_far + process_poll_interval))
      done
      if [ "${terminate}" -eq 1 ] && kill -0 "${pid}" 2> /dev/null; then
        "${command[@]}" "${pid}"
      fi
    ) &
    export REMOTE_WATCH_DAEMON_JOB_ID="$!"
  fi
}

remote_watch "$@"
