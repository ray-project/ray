#!/usr/bin/env python3
"""Run the curated Ray Data test suite against the artifact's Ray.

Two modes:

  --mode record   Run all tests against the current artifact source (which
                  must be unmodified vanilla Ray at this point, since this
                  is called from ``setup_environment.sh``). Record each
                  test's pass/fail into results/optimization_gate_baseline.json.
                  Tests that fail here are considered "pre-existing failures"
                  and are ignored by the gate.

  --mode gate     Run the same tests against the artifact's (possibly
                  modified) Ray source and compare against baseline. Prints
                  JSON with a ``regressed`` list of tests that passed on
                  baseline but fail now.

Ray is pip-installed editable from ``<artifact>/python``, so ``import ray``
resolves to the artifact's source tree directly in every process — driver,
pytest subprocess, and any Ray workers the tests spawn. No path manipulation
or overlay is required.

Per-test granularity is achieved by running pytest with JUnit XML output
and parsing it.
"""

import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile
from xml.etree import ElementTree

# Import the curated test list from the same directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_list import (  # noqa: E402
    FAST_TEST_NODES,
    KNOWN_FLAKY_TESTS,
    TEST_NODES,
)


def _run_pytest_one_file(
    artifact_dir: str,
    test_node: str,
    timeout_per_test: int,
    junit_path: str,
) -> int:
    """Run pytest for a single test node in an isolated subprocess.

    Ray is pip-installed editable from ``<artifact>/python``, so ``import
    ray`` resolves to the artifact's tree directly. That includes
    ``ray.data.tests`` and ``ray.tests``, which are part of the source tree
    and picked up via the editable install.

    We launch each file in its own pytest invocation because Ray tests rely
    on module-scoped fixtures that initialize/teardown Ray — sharing a
    single pytest process across all files causes state leakage
    (``ray_start_regular_shared`` vs ``ray_start_10_cpus_shared`` etc. can
    conflict).

    Pytest runs from a neutral temp cwd so stray ``ray`` subdirs
    (e.g. ``/tmp/ray/session_*``) aren't interpreted as a namespace package.
    """
    env = os.environ.copy()
    env.setdefault("RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE", "1")
    # NOTE: do NOT set RAY_DATA_DISABLE_PROGRESS_BARS here. We suppress
    # progress bars in the benchmark harness for timing hygiene, but in
    # the test gate several test_progress_manager.* tests specifically
    # verify the progress-manager factory resolves to tqdm/rich/logging
    # implementations by default — setting this env var forces them to
    # NoopExecutionProgressManager and makes those tests fail. Leaving
    # the DataContext default (progress bars enabled) lets those tests
    # exercise their intended code path. Progress-bar chatter from other
    # tests shows up in stderr, which we discard.
    # Force Ray to bind to 127.0.0.1 so tests can start Ray from the agent's
    # sandboxed shell, which runs in a network namespace where Ray's default
    # host-IP detection is unreachable.
    env.setdefault("RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER", "0")

    artifact_abs = os.path.abspath(artifact_dir)
    # Convert the test node to an absolute path.
    if "::" in test_node:
        path, rest = test_node.split("::", 1)
        abs_node = os.path.join(artifact_abs, path) + "::" + rest
    else:
        abs_node = os.path.join(artifact_abs, test_node)

    # Prepend the tests directory to PYTHONPATH so that both the pytest
    # driver AND the Ray worker processes can resolve bare-name imports
    # (`import test_stats`, `import test_autoscaler`, ...). Pytest's
    # prepend import-mode loads the test files as top-level modules
    # (name = file basename) rather than dotted paths, so any pickled
    # closure referring to a test-module function captures the reference
    # as `test_stats.<fn>`. Without this PYTHONPATH, workers fail with
    # `ModuleNotFoundError: No module named 'test_stats'` when the
    # closure is deserialized on them. The tests directory we add here
    # is the one containing the test file being invoked (which is the
    # same directory for `python/ray/data/tests/*` and `.../unit/*`).
    test_file_abs = abs_node.split("::", 1)[0]
    tests_dir = os.path.dirname(test_file_abs)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        tests_dir + (os.pathsep + existing_pythonpath if existing_pythonpath else "")
    )

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "--timeout",
        str(timeout_per_test),
        "-q",
        "--no-header",
        f"--junitxml={junit_path}",
        "-p",
        "no:cacheprovider",
        abs_node,
    ]

    # Use a dedicated empty cwd (not /tmp) so pytest/Python doesn't pick up
    # stray ``ray`` subdirectories as a namespace package. Ray's
    # ``ray.init()`` creates /tmp/ray/session_* which is otherwise treated
    # as a namespace package entry for ``ray``.
    clean_cwd = tempfile.mkdtemp(prefix="gated-tests-cwd-")
    # Capture stderr to a tempfile so we can surface pytest collection errors
    # when the JUnit XML ends up empty. stdout is still discarded.
    stderr_fd, stderr_path = tempfile.mkstemp(
        prefix="gated-tests-stderr-", suffix=".log"
    )
    # Clean up any stale Ray sessions before each pytest run. Each
    # ``ray.init()`` creates a ``/tmp/ray/session_*`` directory containing the
    # cluster's GCS address; accumulated sessions confuse Ray's
    # ``canonicalize_bootstrap_address_or_die`` auto-discovery (tests that
    # call ``ray.util.state.*`` raise
    # "Found multiple active Ray instances" and fail). Clearing these between
    # files keeps each pytest subprocess's Ray state isolated. Also unset
    # ``RAY_ADDRESS`` so the test's own ``ray.init()`` picks its own address
    # rather than trying to connect to a prior cluster.
    for session_dir in glob.glob("/tmp/ray/session_*"):
        try:
            shutil.rmtree(session_dir, ignore_errors=True)
        except OSError:
            pass
    session_latest = "/tmp/ray/session_latest"
    if os.path.islink(session_latest) or os.path.exists(session_latest):
        try:
            os.unlink(session_latest)
        except OSError:
            pass
    env.pop("RAY_ADDRESS", None)
    try:
        with open(os.devnull, "w") as devnull, os.fdopen(stderr_fd, "w") as stderr_f:
            proc = subprocess.run(
                cmd,
                cwd=clean_cwd,
                env=env,
                stdout=devnull,
                stderr=stderr_f,
            )
    finally:
        try:
            os.rmdir(clean_cwd)
        except OSError:
            pass
        # After pytest exits, ``ray.init()``-spawned daemons (gcs_server,
        # raylet, dashboard, ray::* workers) sometimes orphan and keep
        # running — ``ray.shutdown()`` during fixture teardown doesn't
        # always reap them, especially on pytest-timeout SIGKILLs. Left
        # alone, they accumulate and cause Ray's auto-discovery to find
        # "multiple active Ray instances" on subsequent tests that call
        # ``ray.util.state.*``. Reap them here so each test file starts
        # clean.
        # pkill's -f uses ERE; alternation must be bare ``|`` (not ``\|``).
        subprocess.run(
            ["pkill", "-9", "-f",
             "gcs_server|raylet|ray::|ray/dashboard/agent.py|ray/dashboard/dashboard.py|runtime_env/agent/main.py"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    return proc.returncode, stderr_path


#: When a test fails on its first run, rerun it this many times individually
#: to characterize its pass rate. Gate regressions are assessed by comparing
#: pass rates (baseline vs current) rather than single-run pass/fail, so
#: flaky tests don't produce false-alarm regressions.
FLAKY_RETRY_COUNT = 10


def _nodeid_to_pytest_node(abs_file: str, nodeid: str) -> str:
    """Rebuild an executable pytest node from an abs file + ``_parse_junit`` nodeid.

    ``_parse_junit`` produces keys like ``test_stats::test_spilled_stats[True]``
    or ``test_progress_manager.TestGetProgressManager::test_tqdm_progress_default``.
    Pytest expects nodes like ``<abs>/test_stats.py::test_spilled_stats[True]``
    or ``<abs>/test_progress_manager.py::TestGetProgressManager::test_tqdm_progress_default``.
    The leading dotted path in the nodeid begins with the file basename
    and may include nested class names; strip the file basename (always
    present) and translate the remaining dots into ``::`` separators.
    """
    classname, _, name = nodeid.partition("::")
    file_base = os.path.splitext(os.path.basename(abs_file))[0]
    parts = classname.split(".") if classname else []
    if parts and parts[0] == file_base:
        parts = parts[1:]
    pieces = parts + ([name] if name else [])
    return f"{abs_file}::{'::'.join(pieces)}" if pieces else abs_file


def _run_single_test_node(
    artifact_dir: str,
    test_file_node: str,
    abs_file: str,
    nodeid: str,
    timeout_per_test: int,
) -> str:
    """Rerun one subtest by its ``_parse_junit`` nodeid. Returns its status."""
    pytest_node_rel = _nodeid_to_pytest_node(abs_file, nodeid)
    # Convert abs path to something _run_pytest_one_file will re-abs-ify
    # correctly. `_run_pytest_one_file` calls os.path.join(artifact_abs, ...)
    # only when the input isn't already absolute; an absolute-file pytest
    # node (with "::") will pass through.
    pytest_node = pytest_node_rel  # already absolute since abs_file is absolute
    # Strip the artifact prefix so _run_pytest_one_file re-joins correctly
    # (it passes abs paths through unchanged, but we already have abs).
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
        junit_path = f.name
    try:
        _, stderr_path = _run_pytest_one_file(
            artifact_dir, pytest_node, timeout_per_test, junit_path
        )
        try:
            os.unlink(stderr_path)
        except OSError:
            pass
        try:
            sub_results = _parse_junit(junit_path, test_file=test_file_node)
        except Exception:
            return "error"
    finally:
        if os.path.exists(junit_path):
            os.unlink(junit_path)
    return sub_results.get(nodeid, "error")


def _run_all_tests(
    artifact_dir: str,
    timeout_per_test: int,
    test_nodes: list = None,
    flaky_retry_count: int = FLAKY_RETRY_COUNT,
) -> dict:
    """Run each test file in its own pytest subprocess; merge results.

    Returns ``{nodeid: {"passed": int, "total": int}}``. For tests that
    pass on the first run, returns ``{"passed": 1, "total": 1}``. For
    tests that fail on the first run, retries the subtest individually
    up to ``flaky_retry_count`` times total and returns
    ``{"passed": X, "total": flaky_retry_count}`` — giving the gate
    enough signal to distinguish a genuinely-broken test from a flaky
    one when comparing baseline vs current runs.
    """
    if test_nodes is None:
        test_nodes = TEST_NODES
    all_results: dict = {}
    for i, test_node in enumerate(test_nodes):
        with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
            junit_path = f.name
        try:
            print(
                f"  [{i+1}/{len(test_nodes)}] {test_node}",
                file=sys.stderr,
            )
            returncode, stderr_path = _run_pytest_one_file(
                artifact_dir, test_node, timeout_per_test, junit_path
            )
            try:
                file_results: dict = {}
                parse_error: str | None = None
                try:
                    file_results = _parse_junit(junit_path, test_file=test_node)
                except ElementTree.ParseError as e:
                    parse_error = f"JUnit XML unparseable: {e}"
                except Exception as e:
                    parse_error = f"JUnit parse crashed: {type(e).__name__}: {e}"

                # Guard against collection errors OR junit-write failures — if
                # no tests were reported, log the node as an error and surface
                # the pytest stderr tail so the gate's caller can see *why*
                # collection / pytest startup failed (the only way to debug
                # sandbox-specific issues like missing loopback or shm sizing).
                if not file_results:
                    file_results[test_node] = "error"
                    msg_parts = [
                        f"    ! pytest produced no usable junit for {test_node}"
                    ]
                    if parse_error:
                        msg_parts.append(f"      parse: {parse_error}")
                    msg_parts.append(f"      pytest exit code: {returncode}")
                    try:
                        with open(stderr_path) as sf:
                            tail = sf.read()[-4000:]
                        if tail.strip():
                            msg_parts.append("      pytest stderr tail:")
                            msg_parts.extend(
                                "        " + ln for ln in tail.splitlines()[-40:]
                            )
                    except OSError:
                        pass
                    print("\n".join(msg_parts), file=sys.stderr)
            finally:
                if os.path.exists(stderr_path):
                    os.unlink(stderr_path)
        finally:
            if os.path.exists(junit_path):
                os.unlink(junit_path)

        # Seed first-run results into the fractional form.
        for nodeid, status in file_results.items():
            if status == "passed" or status == "skipped":
                all_results[nodeid] = {"passed": 1, "total": 1, "status": status}
            else:
                all_results[nodeid] = {
                    "passed": 0,
                    "total": 1,
                    "status": status,
                }

        # Retry any first-run failures individually to characterize flake,
        # AND always retry any KNOWN_FLAKY_TESTS regardless of first-run
        # outcome. The latter keeps the baseline-vs-gate comparison
        # symmetric on tests whose natural pass-rate is <100%; without
        # it, a single-run pass on the baseline against a 10-run-retry on
        # the gate is a retry-policy artifact, not a real regression.
        retry_nodes = [
            nodeid
            for nodeid, status in file_results.items()
            if nodeid != test_node  # exclude the file-level error placeholder
            and (
                status not in ("passed", "skipped")
                or nodeid in KNOWN_FLAKY_TESTS
            )
        ]
        if retry_nodes and flaky_retry_count > 1:
            test_file_part = test_node.split("::", 1)[0]
            abs_file = os.path.join(os.path.abspath(artifact_dir), test_file_part)
            for nodeid in retry_nodes:
                known = nodeid in KNOWN_FLAKY_TESTS
                first_pass = file_results[nodeid] == "passed"
                reason = (
                    "known-flaky (retrying despite first-run pass)"
                    if known and first_pass
                    else "known-flaky"
                    if known
                    else "flaky"
                )
                print(
                    f"    ↻ retrying {reason} {nodeid} "
                    f"up to {flaky_retry_count - 1} more times",
                    file=sys.stderr,
                )
                for attempt in range(flaky_retry_count - 1):
                    status = _run_single_test_node(
                        artifact_dir,
                        test_node,
                        abs_file,
                        nodeid,
                        timeout_per_test,
                    )
                    all_results[nodeid]["total"] += 1
                    if status == "passed":
                        all_results[nodeid]["passed"] += 1
                    # Track the most recent non-passed status for reporting.
                    if status != "passed":
                        all_results[nodeid]["status"] = status
                # Log final fraction for this nodeid.
                r = all_results[nodeid]
                print(
                    f"      → {nodeid}: {r['passed']}/{r['total']} passed",
                    file=sys.stderr,
                )

    return all_results


def _canonical_dotted_path(raw: str, test_file: str) -> str:
    """Strip the filesystem-path-derived prefix from a dotted module path.

    With ``--import-mode=importlib``, pytest derives the testcase ``classname``
    (and for collection errors, the ``name``) from the absolute test-file
    path — e.g. ``workspace.glia.task-repo.worktrees.<agent>.python.ray.data.
    tests.test_foo.TestBar``. That prefix changes between the setup-time run
    (cwd = task repo root) and evaluate-time runs (cwd = a per-agent
    worktree), so the baseline keys wouldn't match current-run keys.
    Normalize by stripping everything before the rightmost segment that
    matches the test file's basename. Safe no-op when the input doesn't
    contain the basename (e.g. normal ``TestClass`` classnames or
    ``test_method[param]`` names).
    """
    file_basename = os.path.splitext(os.path.basename(test_file.split("::")[0]))[0]
    if not file_basename:
        return raw
    parts = raw.split(".")
    try:
        idx = len(parts) - 1 - parts[::-1].index(file_basename)
    except ValueError:
        return raw
    return ".".join(parts[idx:])


# Backward-compat alias (the original name was classname-only).
_canonical_classname = _canonical_dotted_path


def _parse_junit(junit_path: str, test_file: str = "") -> dict:
    """Parse JUnit XML into ``{nodeid: "passed"|"failed"|"error"|"skipped"}``."""
    results: dict = {}
    if not os.path.isfile(junit_path):
        return results
    tree = ElementTree.parse(junit_path)
    root = tree.getroot()
    # JUnit can have <testsuite> as root OR <testsuites><testsuite>...
    suites = root.iter("testcase")
    for case in suites:
        classname = case.attrib.get("classname", "")
        name = case.attrib.get("name", "")
        # On collection errors pytest sometimes emits
        # <testcase classname="" name="module.path">, putting the path in
        # ``name`` instead of ``classname``. Normalize both fields so keys
        # stay stable regardless of cwd.
        if test_file:
            classname = _canonical_dotted_path(classname, test_file)
            name = _canonical_dotted_path(name, test_file)
        nodeid = f"{classname}::{name}" if classname else name

        status = "passed"
        if case.find("failure") is not None:
            status = "failed"
        elif case.find("error") is not None:
            status = "error"
        elif case.find("skipped") is not None:
            status = "skipped"
        results[nodeid] = status
    return results


def cmd_record(artifact_dir: str, baseline_path: str, timeout: int) -> int:
    """Run tests against the artifact source at the time of setup, record
    pass/fail. Must be called before any agent modifications.

    Always records the full TEST_NODES list; the fast subset is a strict
    subset so no separate fast-mode baseline is needed.

    Tests in ``KNOWN_FLAKY_TESTS`` are run at full retry depth
    (``FLAKY_RETRY_COUNT`` = 10) regardless of first-run outcome, so the
    recorded baseline pass-rate is the true rate rather than a single-run
    sample. This keeps the baseline-vs-gate comparison symmetric on
    tests whose natural pass-rate is <100%.
    """
    results = _run_all_tests(artifact_dir, timeout_per_test=timeout)

    os.makedirs(os.path.dirname(baseline_path), exist_ok=True)
    with open(baseline_path, "w") as f:
        json.dump({"results": results}, f, indent=2, sort_keys=True)

    summary = {
        "stable_pass": 0,      # passed all runs
        "stable_fail": 0,      # passed zero runs
        "flaky": 0,            # passed some but not all runs
        "skipped": 0,
    }
    for v in results.values():
        if v.get("status") == "skipped":
            summary["skipped"] += 1
        elif v["passed"] == v["total"]:
            summary["stable_pass"] += 1
        elif v["passed"] == 0:
            summary["stable_fail"] += 1
        else:
            summary["flaky"] += 1

    print(json.dumps({"path": baseline_path, "total": len(results), "summary": summary}))
    return 0


def cmd_gate(artifact_dir: str, baseline_path: str, timeout: int, fast: bool) -> int:
    """Run tests against OVERLAYED ray, detect regressions vs baseline.

    ``fast=True`` runs only FAST_TEST_NODES. Regressions are reported only
    for tests that ran in this mode; tests outside the subset are ignored.
    The full gate (fast=False) runs the complete TEST_NODES list.
    """
    if not os.path.isfile(baseline_path):
        print(json.dumps({
            "regressed": [],
            "error": f"baseline not found at {baseline_path}",
        }))
        return 1

    with open(baseline_path) as f:
        baseline = json.load(f).get("results", {})

    test_nodes = FAST_TEST_NODES if fast else TEST_NODES
    current = _run_all_tests(
        artifact_dir, timeout_per_test=timeout, test_nodes=test_nodes
    )

    # A "regression" is: a test whose pass rate dropped meaningfully
    # between baseline and current. We compare rates rather than raw
    # pass/fail because several Ray 2.55 tests are flaky (a single run
    # can land either way) — strict pass/fail comparison produces
    # false-alarm regressions. The rule:
    #
    #   regression = baseline_rate - current_rate > RATE_TOLERANCE
    #
    # with RATE_TOLERANCE = 0.1 (10% of runs). A stable-pass baseline
    # (rate = 1.0) with a ~90% current is not flagged; a 50% flaky
    # baseline becoming 20% flaky IS flagged. A baseline that failed
    # 100% but now passes is reported in `fixed` (informational).
    RATE_TOLERANCE = 0.1

    def _rate(entry) -> float:
        total = entry.get("total", 1) if isinstance(entry, dict) else 1
        passed = entry.get("passed", 0) if isinstance(entry, dict) else (
            1 if entry == "passed" else 0
        )
        return passed / total if total > 0 else 0.0

    regressed = []
    fixed = []
    unknown = []

    # In fast mode we only check the subset of baseline entries that
    # correspond to files we actually ran, to avoid reporting every other
    # test as "missing".
    if fast:
        fast_file_stems = set()
        for node in FAST_TEST_NODES:
            fast_file_stems.add(
                os.path.splitext(os.path.basename(node.split("::")[0]))[0]
            )
        relevant_baseline = {
            k: v for k, v in baseline.items()
            if any(stem in k for stem in fast_file_stems)
        }
    else:
        relevant_baseline = baseline

    for nodeid, baseline_entry in relevant_baseline.items():
        current_entry = current.get(nodeid)
        baseline_rate = _rate(baseline_entry)
        if current_entry is None:
            # Missing in current. Only a regression if the baseline
            # actually passed at all.
            if baseline_rate > 0:
                regressed.append({
                    "test": nodeid,
                    "baseline": baseline_entry,
                    "current": "missing",
                })
            continue
        current_rate = _rate(current_entry)
        if baseline_rate - current_rate > RATE_TOLERANCE:
            regressed.append({
                "test": nodeid,
                "baseline": baseline_entry,
                "current": current_entry,
            })
        elif current_rate > baseline_rate + RATE_TOLERANCE:
            fixed.append({
                "test": nodeid,
                "baseline": baseline_entry,
                "current": current_entry,
            })

    for nodeid, current_entry in current.items():
        if nodeid not in baseline:
            unknown.append({"test": nodeid, "status": current_entry})

    summary = {
        "baseline_total": len(baseline),
        "current_total": len(current),
        "regressed_count": len(regressed),
        "fixed_count": len(fixed),
        "unknown_count": len(unknown),
    }

    result = {
        "summary": summary,
        "regressed": regressed,
        "fixed": fixed[:50],  # cap to keep output readable
        "unknown": unknown[:50],
    }
    print(json.dumps(result))
    # Exit 0 regardless — caller decides; evaluate script parses the JSON.
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=["record", "gate"],
        required=True,
        help="record: baseline unmodified Ray; gate: check artifact vs baseline",
    )
    parser.add_argument(
        "--artifact-dir",
        default=".",
        help="Artifact root (defaults to CWD).",
    )
    parser.add_argument(
        "--baseline",
        default="glia-bench/results/optimization_gate_baseline.json",
        help="Path to the baseline JSON (relative to artifact-dir or absolute).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=180,
        help="Per-test timeout in seconds.",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help=(
            "Gate mode only: run only FAST_TEST_NODES (~1-2 min), a strict "
            "subset covering the scheduler, backpressure policies, ranker, "
            "and issue detector. Intended for mid-session correctness "
            "pre-checks before running the full gate."
        ),
    )
    args = parser.parse_args()

    artifact_dir = os.path.abspath(args.artifact_dir)
    baseline = args.baseline
    if not os.path.isabs(baseline):
        baseline = os.path.join(artifact_dir, baseline)

    if args.mode == "record":
        if args.fast:
            print(json.dumps({
                "error": "--fast is only valid with --mode gate"
            }))
            return 2
        return cmd_record(artifact_dir, baseline, args.timeout)
    else:
        return cmd_gate(artifact_dir, baseline, args.timeout, fast=args.fast)


if __name__ == "__main__":
    sys.exit(main())
