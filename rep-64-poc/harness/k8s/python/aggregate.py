#!/usr/bin/env python3
"""Reads every *.json in a results directory, emits summary.md.

The summary has two parts:
  ## Tests     — one row per test, glance-friendly status table
  ## Findings  — explicit list of threshold violations and failed tests,
                 each with metric / spec target / observed value / source
                 file pointer.  Empty when every test passed cleanly.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys


# Spec targets keyed by metric name.  op is the comparison the metric must
# satisfy to count as healthy; `target` is the threshold; `desc` is the
# one-line human description shown in the Findings table.
THRESHOLDS: dict[str, tuple[str, object, str]] = {
    "pod_restart_s": ("<=", 30, "head pod recovers within 30s"),
    "state_preserved_pct": (">=", 95, "actor state ≥95% preserved"),
    "new_tasks_ok": ("==", True, "cluster accepts new tasks after restart"),
}


def _key_metric(test: str, metrics: dict) -> str:
    if test == "20-actor-survival":
        c = metrics.get("actors_created", 0)
        r = metrics.get("actors_recovered", 0)
        return f"{r}/{c} actors recovered"
    if test.startswith("30-pod-delete"):
        pr = metrics.get("pod_restart_s", "?")
        sp = metrics.get("state_preserved_pct", "?")
        return f"pod_restart {pr}s, state {sp}%"
    if test == "40-substrate-sweep":
        n = len(metrics.get("by_storage_class", {}))
        return f"{n} StorageClass(es) probed"
    if test == "50-fast-storage":
        ip = metrics.get("inflection_pool_size", "?")
        return f"inflection pool size: {ip}"
    return "(no key metric)"


def _violates(op: str, target: object, val: object) -> bool:
    """True iff val fails the spec relation (op target val)."""
    if op == "<=":
        return not (val <= target)  # type: ignore[operator]
    if op == ">=":
        return not (val >= target)  # type: ignore[operator]
    if op == "==":
        return val != target
    raise ValueError(f"unknown op {op!r}")


def _findings_for(result: dict, source_basename: str) -> list[dict]:
    """Surface threshold violations + status=fail tests as discrete findings."""
    findings: list[dict] = []
    metrics = result.get("metrics") or {}
    test_name = result["test_name"]
    backend = metrics.get("backend") or result.get("backend")

    has_metric_finding = False
    for metric, (op, target, desc) in THRESHOLDS.items():
        if metric not in metrics:
            continue
        val = metrics[metric]
        if _violates(op, target, val):
            has_metric_finding = True
            findings.append(
                {
                    "test": test_name,
                    "backend": backend,
                    "metric": metric,
                    "target": f"{op} {target} ({desc})",
                    "observed": val,
                    "source": source_basename,
                }
            )

    # status=fail without a threshold trigger means the test crashed before
    # producing the metric (verify phase aborted, ray.init failed, etc.).
    if result.get("status") == "fail" and not has_metric_finding:
        findings.append(
            {
                "test": test_name,
                "backend": backend,
                "metric": "(test did not complete)",
                "target": "verify phase exits cleanly",
                "observed": "status=fail",
                "source": source_basename,
            }
        )
    return findings


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("results_dir")
    args = p.parse_args()

    files = sorted(glob.glob(os.path.join(args.results_dir, "*.json")))
    if not files:
        print(f"No JSON results in {args.results_dir}", file=sys.stderr)
        return 1

    # Latest run per test_name (test files are timestamped).  Track the
    # source file so the Findings section can point readers at the raw JSON.
    latest: dict[str, tuple[dict, str]] = {}
    for f in files:
        with open(f) as fh:
            r = json.load(fh)
        latest[r["test_name"]] = (r, os.path.basename(f))

    # Header from any one envelope (cluster facts identical across a run).
    sample, _ = next(iter(latest.values()))
    out = []
    out.append(f"# Test summary — {sample['tier']} tier")
    out.append("")
    out.append("## Cluster")
    out.append(f"- tier:    {sample['tier']}")
    out.append(f"- context: {sample['cluster']['kube_context']}")
    out.append(f"- ns:      {sample['cluster']['namespace']}")
    out.append(f"- image:   {sample['image']}")
    out.append(f"- commit:  {sample['git_commit']}")
    out.append(
        f"- kuberay: {sample['cluster']['kuberay_operator_version']}  "
        f"|  k8s: {sample['cluster']['k8s_server_version']}  "
        f"|  {sample['cluster']['node_count']} nodes"
    )
    out.append("")
    out.append("## Tests")
    out.append("| test                | status | duration | key metric |")
    out.append("|---------------------|--------|----------|------------|")

    all_findings: list[dict] = []
    for name in sorted(latest):
        r, source = latest[name]
        km = _key_metric(name, r.get("metrics") or {})
        if r["status"] == "skipped":
            km = r.get("skip_reason") or "skipped"
        out.append(
            f"| {name:<19} | {r['status']:<6} | {int(r['duration_s']):>4}s "
            f"| {km} |"
        )
        all_findings.extend(_findings_for(r, source))

    out.append("")
    out.append("## Findings")
    if not all_findings:
        out.append("No threshold violations or failed tests in this run.")
    else:
        out.append(
            f"{len(all_findings)} finding(s) across {len(latest)} test(s).  "
            f"Each row below is a metric that fell outside its spec target, "
            f"or a test that did not complete."
        )
        out.append("")
        out.append("| test | backend | metric | spec target | observed | source |")
        out.append("|------|---------|--------|-------------|----------|--------|")
        for f in all_findings:
            out.append(
                f"| {f['test']} "
                f"| {f['backend'] or '-'} "
                f"| {f['metric']} "
                f"| {f['target']} "
                f"| {f['observed']} "
                f"| {f['source']} |"
            )

    summary_path = os.path.join(args.results_dir, "summary.md")
    with open(summary_path, "w") as fh:
        fh.write("\n".join(out) + "\n")
    print(f"wrote {summary_path}")
    if all_findings:
        print(
            f"  -> {len(all_findings)} finding(s) — see ## Findings section",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
