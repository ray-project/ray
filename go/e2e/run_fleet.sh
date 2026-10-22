#!/usr/bin/env bash
#
# run_fleet.sh -- scripted runner for the Ray Go E2E driver fleet.
#
# Usage:
#   run_fleet.sh [--phase 1|2] [--driver NAME] [--timeout-scale N]
#                [--cluster-address ADDR] [--results-dir DIR] [--dry-run]
#
# Options:
#   --phase 1|2          Which fleet phase to run (default 1). Phase 1 is the
#                        single-language driver set; phase 2 is the
#                        cross-language set (Go<->Python, Go<->C++, runtime env).
#   --driver NAME        Run only the named driver; repeatable. Without it,
#                        every driver in the selected phase runs.
#   --timeout-scale N    Export RAY_GO_E2E_TIMEOUT_SCALE=N for the submissions
#                        (default 1; the drivers ignore values below 1.0).
#   --cluster-address A  Ray cluster address used by `ray job submit` and by the
#                        standalone Python lane; exported as RAY_ADDRESS.
#                        Default: $RAY_ADDRESS when set, otherwise "auto".
#   --results-dir DIR    Where the per-driver logs and the summary are written.
#                        Default: ./fleet-results-<UTC timestamp>.
#   --dry-run            Perform every environment check and print the exact
#                        build, stage and submit commands, submitting nothing.
#                        Checks that depend on build outputs (the userfuncs
#                        plugin, built by this run) are printed as build steps
#                        rather than performed.
#
# Exit status: 0 only when every selected driver passed.
#
# The runner rebuilds //go/e2e/... and //go:ray_go_pkg_zip in the same
# invocation before staging anything, so no staged artifact can come from an
# earlier tree. The raygo/go_runtime.so pair the drivers and workers actually
# load are the copies already synced into the environment (or pinned through
# RAY_GO_RUNTIME_PATH), not the ones the build just produced, so after the build
# the runner also compares that pair against the just-built package and fails
# with the remedy when they differ. Each driver is staged into its own job
# directory together with the userfuncs plugin and any cross-language extra,
# submitted with `ray job submit`, and compared against the criterion its own
# source defines. The drivers do not report uniformly, so the criterion has
# three tiers: count equality against the driver-mode test count for a
# no-argument row whose source carries the "=== Test... PASSED ===" convention;
# presence of a marker the source itself declares (that convention, "✓", "✅" or
# "passed") for every other row; and exit code alone where the source declares
# no marker at all.
#
# Environment knobs read by the drivers (export them before invoking the runner):
#   RAY_GO_E2E_TIMEOUT_SCALE  poll-deadline scale for the drivers that poll
#                             (see --timeout-scale).
#   GSA_ACTOR_STATE_TIMEOUT   actor-state probe deadline, in seconds, for
#                             global_state_accessor_driver.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PLUGIN="$ROOT/bazel-bin/go/e2e/userfuncs/plugin/userfuncs.so"
BUILD_TARGETS=(//go/e2e/... //go:ray_go_pkg_zip)

# CLI state.
PHASE=1
DRY_RUN=0
SCALE=1
RESULTS=""
SELECTED=()
# Default the cluster address to the exported RAY_ADDRESS, else "auto" so the
# direct lane can hand the address straight to ray.init().
CLUSTER_ADDRESS="${RAY_ADDRESS:-auto}"

DRIVER_TABLE=()

# ---------------------------------------------------------------------------
# Driver table (data).
#
# Each row is: exec|phase|driver|extra job-dir files|driver args
#   exec  -- "job" submits the driver with `ray job submit`; "direct" runs it
#            on the head node with the local interpreter (standalone Python lane).
#   extra -- comma-separated job-dir files, each carrying its source:
#            src:PATH resolves under go/e2e/, bin:NAME resolves under
#            bazel-bin/go/e2e/<driver>/.
# Two rows may share a driver name when it declares more than one mode; the
# results row is keyed by <driver><args> so both appear.
#
# The table is loaded after argument parsing because the direct lane row embeds
# the resolved cluster address.
# ---------------------------------------------------------------------------
load_driver_table() {
  DRIVER_TABLE=(
    "job|1|driver_app||"
    "job|1|driver_app||--run-mode=local"
    "job|1|actor_integration_driver||"
    "job|1|driver_parallel_actor||"
    "job|1|runtime_context_driver||"
    "job|1|global_state_accessor_driver||"
    "job|1|driver_placement_group||"
    "job|1|driver_placement_group||--run-mode=local"
    "job|1|driver_gcs_client||"
    "job|1|driver_gcs_client||--run-mode=local"
    "job|1|driver_metrics||"
    "job|1|driver_metrics||--run-mode=local"
    "job|1|zero_copy_benchmark||"
    "job|2|driver_call_python|src:test_go_call_python.py|"
    "job|2|driver_call_cpp|bin:cpp_xlang_worker.so|"
    "job|2|runtime_env_driver||"
    "direct|2|test_python_call_go|src:test_python_call_go.py|--ray-address=$CLUSTER_ADDRESS"
  )
}

die() {
  printf 'error: %s\n' "$1" >&2
  exit 1
}

warn() {
  printf 'warning: %s\n' "$1" >&2
}

usage() {
  # Print the leading comment block (from the first header line to the last
  # consecutive "#" line) so the range tracks the header instead of a hardcoded
  # line count that drifts as the header grows.
  awk 'NR >= 3 { if ($0 !~ /^#/) exit; print }' "${BASH_SOURCE[0]}" \
    | sed 's/^# \{0,1\}//'
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --phase) PHASE="${2:?--phase needs a value}"; shift 2 ;;
      --driver) SELECTED+=("${2:?--driver needs a value}"); shift 2 ;;
      --timeout-scale) SCALE="${2:?--timeout-scale needs a value}"; shift 2 ;;
      --cluster-address)
        CLUSTER_ADDRESS="${2:?--cluster-address needs a value}"
        export RAY_ADDRESS="$CLUSTER_ADDRESS"
        shift 2
        ;;
      --results-dir) RESULTS="${2:?--results-dir needs a value}"; shift 2 ;;
      --dry-run) DRY_RUN=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown argument '$1' (see --help)" ;;
    esac
  done
  [[ "$PHASE" == "1" || "$PHASE" == "2" ]] || die "--phase must be 1 or 2 (got '$PHASE')"
  export RAY_GO_E2E_TIMEOUT_SCALE="$SCALE"
}

# artifact_problem MESSAGE: fatal in a real run, but only a warning under
# --dry-run so the runner can be verified on a machine without a cluster or a
# packaged runtime.
artifact_problem() {
  if [[ $DRY_RUN -eq 1 ]]; then
    warn "$1 (dry-run: not fatal)"
  else
    die "$1"
  fi
}

# probe_dashboard ADDRESS: require the dashboard's /api/version endpoint to
# answer 2xx, retrying a few times before giving up. This is the reachability
# probe for a cluster reached from outside: there the address is a dashboard
# URL (the form `ray job submit` needs), and `ray status` cannot be used
# because the GCS hands the client its own pod IP, which is unroutable.
probe_dashboard() {   # $1=address (http:// or https://)
  local addr="${1%/}" url attempt code
  url="$addr/api/version"
  for attempt in 1 2 3; do
    code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$url" 2>/dev/null || true)"
    if [[ "$code" == 2* ]]; then
      printf 'dashboard reachable: %s -> HTTP %s\n' "$url" "$code"
      return 0
    fi
    if [[ $attempt -lt 3 ]]; then sleep 2; fi
  done
  return 1
}

# python_ray_go_dir prints the ray/go directory of the ray the environment
# imports, or returns non-zero when ray cannot be imported.
python_ray_go_dir() {
  python3 -c 'import ray, os; print(os.path.join(os.path.dirname(ray.__file__), "go"))' 2>/dev/null
}

# direct_row_selected: true when the current phase and selection include a row
# that runs directly on the head node (the standalone Python lane), whose
# ray.init cannot take the dashboard URL that `ray job submit` uses.
direct_row_selected() {
  local row exec_ phase driver extra args want
  for row in "${DRIVER_TABLE[@]}"; do
    IFS='|' read -r exec_ phase driver extra args <<<"$row"
    [[ "$phase" == "$PHASE" && "$exec_" == "direct" ]] || continue
    [[ ${#SELECTED[@]} -eq 0 ]] && return 0
    for want in "${SELECTED[@]}"; do
      [[ "$want" == "$driver" ]] && return 0
    done
  done
  return 1
}

# Phase 0 preflight: the environment the run depends on. Every failure names the
# fix, so a broken setup is reported here instead of inside a driver.
preflight() {
  printf '== preflight (phase %s) ==\n' "$PHASE"

  # 1. ray CLI available
  command -v ray >/dev/null || die "ray CLI not on PATH; activate the Python env with ray installed"

  # 2. the direct lane cannot use a dashboard URL: ray.init needs a GCS address
  #    (host:port) or a ray:// client URL. This is a static check, done before
  #    any probe, so an impossible selection is rejected instead of submitting a
  #    row certain to die at ray.init. Skipped under --dry-run, like the probe.
  if [[ $DRY_RUN -eq 0 ]] && direct_row_selected \
    && [[ "$CLUSTER_ADDRESS" == http://* || "$CLUSTER_ADDRESS" == https://* ]]; then
    die "the direct lane needs a GCS address routable from where it runs, or it must run on a Ray node (see README); '${CLUSTER_ADDRESS}' is a dashboard URL"
  fi

  # 3. cluster reachable (skipped under --dry-run so the script is verifiable without one)
  if [[ $DRY_RUN -eq 0 ]]; then
    if [[ "$CLUSTER_ADDRESS" == http://* || "$CLUSTER_ADDRESS" == https://* ]]; then
      probe_dashboard "$CLUSTER_ADDRESS" \
        || die "dashboard not reachable at ${CLUSTER_ADDRESS%/}/api/version; check the RAY_ADDRESS dashboard URL and the port-forward (curl is required for this probe)"
    else
      ray status >/dev/null 2>&1 || die "no reachable cluster; start one with 'ray start --head' (phase 1) or export RAY_ADDRESS (phase 2)"
    fi
  fi

  # 4. raygo present in the Python env's ray/go
  local pygo=""
  if ! pygo="$(python_ray_go_dir)"; then
    artifact_problem "python3 cannot import ray"
  elif [[ ! -x "$pygo/cmd/raygo" ]]; then
    artifact_problem "raygo missing at $pygo/cmd/raygo; run 'bazel run //go:gen_ray_go_pkg' and sync"
  elif [[ ! -f "$pygo/lib/go_runtime.so" ]]; then
    artifact_problem "go_runtime.so missing at $pygo/lib/go_runtime.so; same fix as raygo"
  fi

  # The userfuncs plugin is deliberately not checked here: //go/e2e/... in
  # BUILD_TARGETS builds it, so on a fresh checkout it does not exist yet and a
  # check at this point would stop the run with a manual build instruction the
  # runner is about to satisfy itself. build_tree checks it after the build.
}

# verify_runtime_same_tree: the raygo/go_runtime.so pair the environment loads
# are the copies previously synced into it (or pinned through
# RAY_GO_RUNTIME_PATH), while the build only rebuilds the package zip and never
# re-extracts it, so a stale runtime would otherwise run silently. Compare the
# loaded pair against the just-built package and fail with the remedy when they
# differ. The zip is the freshly built package (the python/ray/go extraction is
# a manual step that can lag it), so this is the same-tree reference.
verify_runtime_same_tree() {
  local zip="$ROOT/bazel-bin/go/ray_go_pkg.zip" tmp pygo so
  [[ -f "$zip" ]] \
    || { artifact_problem "package zip missing at $zip; run 'bazel build //go:ray_go_pkg_zip'"; return 0; }
  pygo="$(python_ray_go_dir)" || return 0   # preflight already reported this
  tmp="$(mktemp -d)"
  python3 -m zipfile -e "$zip" "$tmp" 2>/dev/null \
    || { rm -rf "$tmp"; artifact_problem "cannot unpack $zip"; return 0; }
  [[ -f "$tmp/ray/go/cmd/raygo" && -f "$tmp/ray/go/lib/go_runtime.so" ]] \
    || { rm -rf "$tmp"; artifact_problem "unexpected layout in $zip"; return 0; }
  # The go_runtime.so the drivers resolve may be pinned outside the env's ray/go.
  so="${RAY_GO_RUNTIME_PATH:-$pygo/lib/go_runtime.so}"
  if ! cmp -s "$tmp/ray/go/cmd/raygo" "$pygo/cmd/raygo"; then
    artifact_problem "raygo at $pygo/cmd/raygo differs from the just-built package; run 'bazel run //go:gen_ray_go_pkg' and sync ray/go/{cmd/raygo,lib/go_runtime.so} into the environment (and every cluster node)"
  fi
  if ! cmp -s "$tmp/ray/go/lib/go_runtime.so" "$so"; then
    artifact_problem "go_runtime.so at $so differs from the just-built package; run 'bazel run //go:gen_ray_go_pkg' and sync ray/go/{cmd/raygo,lib/go_runtime.so} into the environment (and every cluster node), or set RAY_GO_RUNTIME_PATH to the tree's python/ray/go/lib/go_runtime.so"
  fi
  rm -rf "$tmp"
}

# Rebuild the driver fleet and the raygo package in one invocation so nothing
# staged below can come from an earlier tree, then verify the runtime the
# environment loads is that same tree.
build_tree() {
  if [[ $DRY_RUN -eq 1 ]]; then
    printf 'DRY-RUN  build\n'
    printf '    (cd %s && bazel build %s)\n' "$ROOT" "${BUILD_TARGETS[*]}"
    return 0
  fi
  printf 'building %s\n' "${BUILD_TARGETS[*]}"
  ( cd "$ROOT" && bazel build "${BUILD_TARGETS[@]}" ) \
    || die "bazel build ${BUILD_TARGETS[*]} failed; the tree and the raygo package were not rebuilt"
  # //go/e2e/... covers //go/e2e/userfuncs/plugin, so the plugin exists from
  # here on; every staged row copies it, and the Python lane needs it in its job
  # directory because its code_search_path is the relative "./userfuncs.so".
  [[ -f "$PLUGIN" ]] \
    || artifact_problem "plugin missing at $PLUGIN after the build; run 'bazel build //go/e2e/userfuncs/plugin:userfuncs_plugin'"
  verify_runtime_same_tree
}

resolve_extra() {   # $1=driver  $2="src:PATH"|"bin:NAME"  -> prints the source path
  case "$2" in
    src:*) echo "$ROOT/go/e2e/${2#src:}" ;;
    bin:*) echo "$ROOT/bazel-bin/go/e2e/$1/${2#bin:}" ;;
    *)     die "extra file '$2' lacks a src:/bin: prefix" ;;
  esac
}

stage_job_dir() {   # $1=driver  $2=comma-separated extras (may be empty)
  local d="$1" extra="$2" bin="$ROOT/bazel-bin/go/e2e/$1/${1}_/$1"
  local job="$RESULTS/$d.job"; rm -rf "$job"; mkdir -p "$job"
  # Only the driver binary is lane-specific: the standalone Python lane has no
  # Go driver to stage. The plugin is staged for every row, because that lane's
  # code_search_path is the relative "./userfuncs.so" and the Go worker its task
  # lands on opens the plugin from the job directory.
  if [[ "$d" != "test_python_call_go" ]]; then
    [[ -f "$bin" ]] || die "driver binary missing at $bin — did the build step run?"
    cp "$bin" "$job/$d"
  fi
  cp "$PLUGIN" "$job/userfuncs.so"
  local e src
  IFS=',' read -ra EX <<<"$extra"
  for e in ${EX[@]+"${EX[@]}"}; do
    [[ -z "$e" ]] && continue
    src="$(resolve_extra "$d" "$e")" || die "cannot resolve extra '$e' for $d"
    [[ -f "$src" ]] || die "extra file missing at $src (declared as '$e')"
    cp "$src" "$job/$(basename "$src")"
  done
  echo "$job"
}

# log_stem DRIVER ARGS: the per-row log name stem, "<driver><sanitised args>".
# Every character outside [A-Za-z0-9._] becomes '_' so that an arbitrary
# argument string still yields a valid filename -- the direct lane passes a
# dashboard URL, whose "//" and ":" would otherwise split the name into a
# non-existent directory path. '-' is folded too (a superset of the required
# replacement set, which also allows '-'), which keeps the existing names of the
# --run-mode=local rows unchanged. All three log sites go through this helper so
# the .log and .log.exit paths can never diverge.
log_stem() {        # $1=driver  $2=args
  printf '%s%s' "$1" "$(printf '%s' "$2" | sed 's/[^A-Za-z0-9._]/_/g')"
}

# plan_driver prints the staging and submit commands run_driver would execute,
# without touching the filesystem or the cluster.
plan_driver() {     # $1=exec mode  $2=driver  $3=args  $4=extras
  local mode="$1" d="$2" args="$3" extra="$4"
  local job="$RESULTS/$d.job"
  local log
  log="$RESULTS/$(log_stem "$d" "$args").log"
  printf 'DRY-RUN  %s %s%s\n' "$mode" "$d" "${args:+ $args}"
  printf '    mkdir -p %s\n' "$job"
  if [[ "$d" != "test_python_call_go" ]]; then
    printf '    cp %s %s/%s\n' "$ROOT/bazel-bin/go/e2e/$d/${d}_/$d" "$job" "$d"
  fi
  printf '    cp %s %s/userfuncs.so\n' "$PLUGIN" "$job"
  local e src
  IFS=',' read -ra EX <<<"$extra"
  for e in ${EX[@]+"${EX[@]}"}; do
    [[ -z "$e" ]] && continue
    src="$(resolve_extra "$d" "$e")" || die "cannot resolve extra '$e' for $d"
    printf '    cp %s %s/%s\n' "$src" "$job" "$(basename "$src")"
  done
  if [[ "$mode" == "direct" ]]; then
    printf '    (cd %s && python3 ./%s.py%s)\n' "$job" "$d" "${args:+ $args}"
  else
    printf '    (cd %s && RAY_GO_E2E_TIMEOUT_SCALE=%s ray job submit --working-dir . -- ./%s%s)\n' \
      "$job" "$SCALE" "$d" "${args:+ $args}"
  fi
  printf '    -> log %s, exit record %s.exit\n' "$log" "$log"
}

run_driver() {      # $1=exec mode  $2=driver  $3=args  $4=extras
  local mode="$1" d="$2" args="$3" extra="$4"
  if [[ $DRY_RUN -eq 1 ]]; then plan_driver "$mode" "$d" "$args" "$extra"; return 0; fi
  local log
  log="$RESULTS/$(log_stem "$d" "$args").log"
  # A die inside the command substitution only exits the subshell, so the
  # failure must be re-checked here or the caller would submit a bogus job.
  local job; job="$(stage_job_dir "$d" "$extra")" || die "staging failed for $d"
  # $args is a pre-split driver argument list and must word-split into separate
  # argv entries, so the unquoted expansion is deliberate.
  if [[ "$mode" == "direct" ]]; then
    # The standalone Python lane runs on the head node with its interpreter.
    # shellcheck disable=SC2086
    ( cd "$job" && python3 "./$d.py" $args ) >"$log" 2>&1
  else
    # shellcheck disable=SC2086
    ( cd "$job" && RAY_GO_E2E_TIMEOUT_SCALE="$SCALE" ray job submit --working-dir . -- "./$d" $args ) \
      >"$log" 2>&1
  fi
  echo $? > "$log.exit"
}

compare() {         # $1=driver  $2=log  $3=args  prints PASS/FAIL and the reason
  local d="$1" log="$2" args="$3" rc
  # A driver that produced no exit record or no log is a hard failure naming the
  # path, never a silent skip.
  [[ -f "$log.exit" ]] || { echo "FAIL $d no exit record at $log.exit"; return 1; }
  [[ -f "$log" ]] || { echo "FAIL $d no log at $log"; return 1; }
  rc="$(cat "$log.exit")"
  local src="$ROOT/go/e2e/$d/main.go"
  # The standalone Python lane is a script, not a Go driver: its source is the
  # .py file, which declares its own success markers.
  [[ -f "$src" ]] || src="$ROOT/go/e2e/$d.py"
  if [[ "$rc" != "0" ]]; then echo "FAIL $d exit=$rc"; return 1; fi
  local conv='=== Test[A-Za-z]* PASSED ==='
  # Tier 1 -- count equality: driver mode only (a --run-mode=local row runs a
  # different code path that prints a different number of lines), and only
  # where the driver's own source carries the convention.
  if [[ -z "$args" ]] && grep -q "$conv" "$src"; then
    local expect observed local_only
    expect="$(grep -c '^func test[A-Z]' "$src" || true)"
    # The dual-mode drivers (driver_metrics, driver_gcs_client,
    # driver_placement_group) declare their local-only tests as testLocal* and
    # call them only from runLocal, so the driver-mode row never runs them and
    # they must not be counted here.
    local_only="$(grep -c '^func testLocal' "$src" || true)"
    expect=$((expect - local_only))
    observed="$(grep -c "$conv" "$log" || true)"
    if [[ "$observed" != "$expect" ]]; then
      echo "FAIL $d expected $expect PASSED lines, saw $observed"; return 1
    fi
    echo "PASS $d (rc=0, counted=$observed)"; return 0
  fi
  # Tier 2 -- marker presence: the log must carry a marker the SOURCE declares.
  # driver_call_python/cpp land here (they declare func test... but report one
  # descriptive line per check plus a final "tests passed!" line), as do the
  # --run-mode=local rows.
  local markers=() m
  grep -q "$conv"  "$src" && markers+=("$conv")
  grep -q '✓'      "$src" && markers+=('✓')
  grep -q '✅'     "$src" && markers+=('✅')
  grep -q 'passed' "$src" && markers+=('passed')
  # Tier 3 -- exit-code only: the source declares no marker at all
  # (zero_copy_benchmark's success output is a latency table).
  if [[ ${#markers[@]} -eq 0 ]]; then
    echo "PASS $d (rc=0, no marker declared in source)"; return 0
  fi
  for m in "${markers[@]}"; do
    if grep -q "$m" "$log"; then echo "PASS $d (rc=0, marker '$m')"; return 0; fi
  done
  echo "FAIL $d exit=0 but no source-declared success marker in output"; return 1
}

# select_rows prints the table rows for the selected phase and drivers.
select_rows() {
  local row exec_ phase driver extra args want
  for row in "${DRIVER_TABLE[@]}"; do
    IFS='|' read -r exec_ phase driver extra args <<<"$row"
    [[ "$phase" == "$PHASE" ]] || continue
    if [[ ${#SELECTED[@]} -gt 0 ]]; then
      local match=0
      for want in "${SELECTED[@]}"; do
        [[ "$want" == "$driver" ]] && { match=1; break; }
      done
      [[ $match -eq 1 ]] || continue
    fi
    printf '%s\n' "$row"
  done
}

# check_driver_names rejects a --driver name that is not in the fleet table, and
# a name that belongs only to the other phase: select_rows would drop it without
# a word, leaving the run to proceed on the other selections.
check_driver_names() {
  local want row exec_ phase driver extra args
  # ${SELECTED[@]+"${SELECTED[@]}"} keeps the default no --driver invocation
  # working on bash before 4.4, where an empty array expansion is an unbound
  # variable under `set -u` (the script's other array expansions are guarded the
  # same way, or by an explicit length test).
  for want in ${SELECTED[@]+"${SELECTED[@]}"}; do
    local in_table=0 in_phase=0 other_phase=""
    for row in "${DRIVER_TABLE[@]}"; do
      IFS='|' read -r exec_ phase driver extra args <<<"$row"
      [[ "$driver" == "$want" ]] || continue
      in_table=1
      if [[ "$phase" == "$PHASE" ]]; then in_phase=1; else other_phase="$phase"; fi
    done
    [[ $in_table -eq 1 ]] || die "no driver named '$want' in the fleet table"
    [[ $in_phase -eq 1 ]] \
      || die "driver '$want' is a phase $other_phase driver, but phase $PHASE is selected; run it with --phase $other_phase"
  done
}

main() {
  parse_args "$@"
  load_driver_table
  [[ -n "$RESULTS" ]] || RESULTS="./fleet-results-$(date -u +%Y%m%dT%H%M%SZ)"

  check_driver_names

  local -a rows=()
  local row
  while IFS= read -r row; do
    [[ -n "$row" ]] && rows+=("$row")
  done < <(select_rows)
  [[ ${#rows[@]} -gt 0 ]] \
    || die "no drivers selected for phase $PHASE (check --driver names)"

  preflight

  if [[ $DRY_RUN -eq 0 ]]; then
    mkdir -p "$RESULTS"
  fi
  printf 'results dir: %s\n' "$RESULTS"

  build_tree

  local -a results=()
  local exec_ phase driver extra args log verdict
  local failed=0
  for row in "${rows[@]}"; do
    IFS='|' read -r exec_ phase driver extra args <<<"$row"
    printf '\n-- %s %s%s\n' "$exec_" "$driver" "${args:+ $args}"
    run_driver "$exec_" "$driver" "$args" "$extra"
    [[ $DRY_RUN -eq 1 ]] && continue
    log="$RESULTS/$(log_stem "$driver" "$args").log"
    if verdict="$(compare "$driver" "$log" "$args")"; then
      results+=("$verdict")
    else
      results+=("$verdict")
      failed=$((failed + 1))
    fi
    printf '   %s\n' "$verdict"
  done

  if [[ $DRY_RUN -eq 1 ]]; then
    printf '\ndry-run: %d driver run(s) planned for phase %s, nothing submitted\n' \
      "${#rows[@]}" "$PHASE"
    return 0
  fi

  printf '\n===== fleet summary (phase %s) =====\n' "$PHASE"
  printf '%s\n' "${results[@]}" | tee "$RESULTS/summary.txt"
  printf 'logs: %s\n' "$RESULTS"
  if [[ $failed -gt 0 ]]; then
    printf '%d of %d driver run(s) failed\n' "$failed" "${#rows[@]}" >&2
    return 1
  fi
  printf 'all %d driver run(s) passed\n' "${#rows[@]}"
  return 0
}

main "$@"
