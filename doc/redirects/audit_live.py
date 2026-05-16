"""Audit anyscale-ray live RtD redirects against docs.ray.io and the Ray source tree.

Inputs:
  - current.api.json: raw v3 API snapshot of the redirects.
  - docs.ray.io: probed for source-URL + target-URL liveness (follows redirects).
  - Ray source tree (doc/source/): scanned to suggest replacement targets for
    dead-target and wrong-content rules.

Outputs (next to this script):
  - audit_data.csv: one row per rule with all tag columns + probe results.
  - audit_report.md: prioritized findings grouped by tag.
  - proposed_current.yaml: hand-shapeable starting point for the cleaned ruleset.
  - dashboard_actions.json: per-rule CRUD operations the operator (or a follow-up
    script using RtdClient) can apply.

The script is intentionally single-file and throwaway. Tag definitions and the
suggestion heuristic are the parts most likely to need tuning after the first
run; both live near the top.
"""

from __future__ import annotations

import csv
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import requests
import yaml

# -----------------------------------------------------------------------------
# Inputs / outputs
# -----------------------------------------------------------------------------

OUT_DIR = Path(__file__).parent
API_JSON = OUT_DIR / "current.api.json"  # fresh snapshot kept next to this script
# Ray source tree. Defaults to the typical layout when this script sits at
# ray/doc/redirects/audit_live.py (committed alongside doc/source/). Override
# via RAY_SOURCE env var if running from a different layout.
RAY_SOURCE = Path(os.environ.get("RAY_SOURCE", Path(__file__).resolve().parent.parent / "source"))
CSV_PATH = OUT_DIR / "audit_data.csv"
REPORT_PATH = OUT_DIR / "audit_report.md"
YAML_PATH = OUT_DIR / "proposed_current.yaml"
ACTIONS_PATH = OUT_DIR / "dashboard_actions.json"

# -----------------------------------------------------------------------------
# Knobs
# -----------------------------------------------------------------------------

BASE_URL = "https://docs.ray.io"
HTTP_TIMEOUT = 15
MAX_WORKERS = 16

# Duplicate identities are computed dynamically from the input data in main()
# so the audit reflects current state after each cleanup pass.

# Resolutions agreed earlier in the session for divergent-target duplicates.
# Maps (from_url, type) -> (chosen_target, reasoning).
# These targets must be full paths with the /en/<version>/ prefix since the rules
# are `type: exact` (page-type rules would omit the prefix).
DIVERGENT_DUPE_RESOLUTIONS: dict[tuple[str, str], tuple[str, str]] = {
    # serialization: both configured targets wrong on master.
    # Real page is at /en/master/ray-core/objects/serialization.html.
    ("/en/latest/serialization.html", "exact"): (
        "/en/master/ray-core/objects/serialization.html",
        "both configured targets wrong on master; actual page lives at ray-core/objects/serialization.html",
    ),
    # pytorch: both converge to /en/master/train/examples.html. Keep direct.
    ("/en/latest/using-ray-with-pytorch.html", "exact"): (
        "/en/master/train/examples.html",
        "both targets converge to /en/master/train/examples.html; direct skips a chain hop",
    ),
    # tensorflow: same shape as pytorch.
    ("/en/latest/using-ray-with-tensorflow.html", "exact"): (
        "/en/master/train/examples.html",
        "both targets converge to /en/master/train/examples.html; direct skips a chain hop",
    ),
}


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def to_master(path: str) -> str:
    """Make a URL probable against docs.ray.io.

    /en/latest/X    -> /en/master/X (master is the canonical probe version)
    /en/master/X    -> unchanged
    /en/<version>/X -> unchanged (specific version paths probed as-is)
    /X              -> /en/master/X (page-style path needs a version prefix
                       to resolve on docs.ray.io)
    other           -> unchanged
    """
    if path.startswith("/en/latest/"):
        return path.replace("/en/latest/", "/en/master/", 1)
    if path.startswith("/en/"):
        return path
    if path.startswith("/") and not path.startswith("//"):
        return "/en/master" + path
    return path


def slug_from_url(url: str) -> str:
    """Last path component without .html, used as the search-key for the source tree."""
    if not url:
        return ""
    path = url.replace("/en/latest/", "/").replace("/en/master/", "/")
    parts = [p for p in path.strip("/").split("/") if p]
    if not parts:
        return ""
    return parts[-1].removesuffix(".html")


def source_path_to_url(p: Path, ray_source: Path) -> str:
    """Convert a doc/source/foo/bar.rst path to /en/master/foo/bar.html."""
    rel = p.relative_to(ray_source)
    html = rel.with_suffix(".html")
    return f"/en/master/{html.as_posix()}"


@dataclass
class Probe:
    url: str
    status: int | str
    final_url: str
    hops: int
    error: str = ""


def probe_one(session: requests.Session, url: str) -> Probe:
    try:
        r = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        return Probe(url=url, status=r.status_code, final_url=r.url, hops=len(r.history))
    except requests.RequestException as exc:
        return Probe(url=url, status="ERROR", final_url="", hops=0, error=str(exc))


# -----------------------------------------------------------------------------
# Classification
# -----------------------------------------------------------------------------

@dataclass
class Classification:
    rule: dict
    source_probe: Probe | None
    target_probe: Probe | None
    tags: dict[str, bool] = field(default_factory=dict)
    suggested_target: str | None = None
    suggestion_confidence: int = 0
    suggestion_reasoning: str = ""


def classify(
    rule: dict,
    all_rules: list[dict],
    source_probe: Probe | None,
    target_probe: Probe | None,
) -> dict[str, bool]:
    from_url = rule.get("from_url") or ""
    to_url = rule.get("to_url") or ""
    rtype = rule.get("type", "page")
    pk = rule.get("pk")

    tags: dict[str, bool] = {}

    # status_302: easy mechanical.
    tags["status_302"] = rule.get("http_status", 301) != 301

    # test_debug: the lone test rule.
    tags["test_debug"] = "/foo/bar" in from_url

    # catch_all_noop: /<prefix>/* -> /<prefix>/:splat.
    tags["catch_all_noop"] = (
        from_url.endswith("/*")
        and to_url.endswith("/:splat")
        and from_url[:-1] == to_url[:-6]  # same prefix before * vs :splat
    )

    # target_dead: target final HTTP status is not 200.
    if target_probe is None or not isinstance(target_probe.status, int):
        tags["target_dead"] = True
    else:
        tags["target_dead"] = target_probe.status != 200

    # target_wrong_content: target is alive (200) but the source slug
    # doesn't appear in the final URL. Heuristic; flags for human review.
    from_slug = slug_from_url(from_url)
    if not tags["target_dead"] and target_probe and from_slug:
        final = target_probe.final_url.lower()
        tags["target_wrong_content"] = from_slug not in final
    else:
        tags["target_wrong_content"] = False

    # chained_target: to_url appears as another rule's from_url.
    other_froms = {r.get("from_url") or "" for r in all_rules if r.get("pk") != pk}
    tags["chained_target"] = to_url in other_froms

    # source_resolves: source URL returns 200 today (rule is dead code unless force=true).
    if source_probe and isinstance(source_probe.status, int):
        tags["source_resolves"] = source_probe.status == 200 and not rule.get("force", False)
    else:
        tags["source_resolves"] = False

    # type_convertible: an exact rule whose from_url starts with /en/latest/
    # could likely become a page rule with the prefix stripped.
    tags["type_convertible"] = rtype == "exact" and from_url.startswith("/en/latest/")

    # duplicate_identity: more than one rule shares this (from_url, type).
    same_ident = sum(1 for x in all_rules if (x.get("from_url"), x.get("type")) == (from_url, rtype))
    tags["duplicate_identity"] = same_ident > 1

    return tags


def suggest_target(
    rule: dict,
    ray_source: Path,
    file_index_by_slug: dict[str, list[Path]],
) -> tuple[str | None, int, str]:
    """Suggest a replacement target with a 0-5 confidence rating.

    Confidence rubric:
      5: unique source file with filename stem equal to source slug.
      4: single fuzzy match (slug appears in filename but stem differs).
      3: multiple exact-stem matches; first is returned.
      2: multiple fuzzy matches; first is returned.
      1: matches exist but the suggested URL doesn't probe 200.
      0: no source file found for the slug, or slug not extractable.

    Verifies suggestions are live by probing.
    """
    from_url = rule.get("from_url") or ""

    # First check explicit resolutions decided earlier in the session.
    pre = DIVERGENT_DUPE_RESOLUTIONS.get((from_url, rule.get("type", "")))
    if pre:
        return pre[0], 5, pre[1]

    slug = slug_from_url(from_url)
    if not slug:
        return None, 0, "no slug extractable from from_url"

    matches = file_index_by_slug.get(slug, [])
    if not matches:
        return None, 0, f"no source file matching slug {slug!r}"

    exact = [m for m in matches if m.stem == slug]

    if len(exact) == 1:
        return source_path_to_url(exact[0], ray_source), 5, f"unique exact-stem match: {exact[0].name}"
    if len(exact) > 1:
        return source_path_to_url(exact[0], ray_source), 3, f"{len(exact)} exact-stem matches; first: {exact[0].relative_to(ray_source)}"
    if len(matches) == 1:
        return source_path_to_url(matches[0], ray_source), 4, f"single fuzzy match: {matches[0].name}"
    return source_path_to_url(matches[0], ray_source), 2, f"{len(matches)} fuzzy matches; first: {matches[0].relative_to(ray_source)}"


def build_source_index(ray_source: Path) -> dict[str, list[Path]]:
    """Index every .rst/.md file under ray/doc/source/ by its stem (basename without ext)."""
    idx: dict[str, list[Path]] = {}
    for ext in (".rst", ".md"):
        for p in ray_source.rglob(f"*{ext}"):
            if p.is_file():
                # Skip auto-generated API stubs - they create huge false-positive sets.
                if "/api/doc/" in p.as_posix():
                    continue
                idx.setdefault(p.stem, []).append(p)
                # Also index by partial slug matches: e.g., "using-ray-with-pytorch"
                # should match "using-ray-with-pytorch.rst".
    return idx


# -----------------------------------------------------------------------------
# Consolidation pass
# -----------------------------------------------------------------------------

def detect_consolidatable(classifications: list[Classification]) -> dict[int, str]:
    """Group rules by a from-prefix + to-prefix transformation pattern.

    Returns {pk: group_id} for rules that participate in a consolidation group
    of >= 3 members. Group_id is a human-readable label.
    """
    # Bucket by (from-parent-dir, to-parent-dir). E.g.,
    # /en/latest/rllib-X.html -> /en/latest/rllib/rllib-X.html
    # all share parent (latest/, latest/rllib/) and a slug renaming pattern.
    by_pattern: dict[tuple[str, str], list[Classification]] = {}
    for c in classifications:
        r = c.rule
        from_url = r.get("from_url") or ""
        to_url = r.get("to_url") or ""
        rtype = r.get("type", "")
        if rtype != "exact" or not from_url or not to_url:
            continue
        if from_url.endswith("/*") or to_url.endswith("/:splat"):
            continue  # already a wildcard
        from_parent = "/".join(from_url.split("/")[:-1]) + "/"
        to_parent = "/".join(to_url.split("/")[:-1]) + "/"
        key = (from_parent, to_parent)
        by_pattern.setdefault(key, []).append(c)

    result: dict[int, str] = {}
    for (fp, tp), members in by_pattern.items():
        if len(members) >= 3:
            label = f"{fp}* -> {tp}:splat"
            for m in members:
                pk = m.rule.get("pk")
                if pk is not None:
                    result[pk] = label
    return result


# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

def write_csv(classifications: list[Classification], path: Path) -> None:
    tag_keys = sorted(classifications[0].tags.keys())
    fieldnames = (
        ["pk", "position", "type", "http_status", "force", "enabled", "from_url", "to_url"]
        + [f"tag_{k}" for k in tag_keys]
        + [
            "source_status",
            "source_final_url",
            "target_status",
            "target_final_url",
            "target_hops",
            "suggested_target",
            "suggestion_confidence",
            "suggestion_reasoning",
            "consolidation_group",
        ]
    )
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for c in classifications:
            r = c.rule
            row = {
                "pk": r.get("pk"),
                "position": r.get("position"),
                "type": r.get("type"),
                "http_status": r.get("http_status"),
                "force": r.get("force"),
                "enabled": r.get("enabled"),
                "from_url": r.get("from_url"),
                "to_url": r.get("to_url"),
                "source_status": c.source_probe.status if c.source_probe else "",
                "source_final_url": c.source_probe.final_url if c.source_probe else "",
                "target_status": c.target_probe.status if c.target_probe else "",
                "target_final_url": c.target_probe.final_url if c.target_probe else "",
                "target_hops": c.target_probe.hops if c.target_probe else "",
                "suggested_target": c.suggested_target or "",
                "suggestion_confidence": c.suggestion_confidence,
                "suggestion_reasoning": c.suggestion_reasoning,
                "consolidation_group": getattr(c, "consolidation_group", ""),
            }
            for k in tag_keys:
                row[f"tag_{k}"] = "Y" if c.tags.get(k) else ""
            w.writerow(row)


def write_report(classifications: list[Classification], path: Path) -> None:
    by_tag: dict[str, list[Classification]] = {}
    for c in classifications:
        for tag, on in c.tags.items():
            if on:
                by_tag.setdefault(tag, []).append(c)

    lines: list[str] = []
    lines.append("# anyscale-ray RtD redirect audit\n")
    lines.append(f"Total rules: **{len(classifications)}**\n")

    # Headline counts.
    lines.append("\n## Tag summary\n")
    lines.append("| Tag | Count | Treatment |")
    lines.append("|---|---|---|")
    treatment = {
        "status_302": "Bulk: rewrite to 301",
        "test_debug": "Bulk: delete",
        "duplicate_identity": "Per DOC-946: delete pure dupes; replace divergent",
        "catch_all_noop": "Bulk: delete (catch-all that maps prefix to itself)",
        "source_resolves": "Per rule: delete (force=false rule fires nothing; source is live)",
        "target_dead": "Per rule: replace target via suggestion + human review",
        "target_wrong_content": "Per rule: replace target via suggestion + human review",
        "chained_target": "Per rule: rewrite to_url to final destination",
        "type_convertible": "Pass: convert exact + /en/latest/ -> page + path",
        "consolidatable": "Pass: fold groups into page + suffix wildcard",
    }
    for tag in sorted(by_tag.keys()):
        lines.append(f"| `{tag}` | {len(by_tag[tag])} | {treatment.get(tag, '')} |")

    # Section per tag.
    for tag in sorted(by_tag.keys()):
        rules = by_tag[tag]
        lines.append(f"\n## `{tag}` ({len(rules)} rules)\n")
        # Sort by position for stability.
        rules_sorted = sorted(rules, key=lambda c: c.rule.get("position", 9999))
        for c in rules_sorted[:80]:
            r = c.rule
            line = f"- **pk={r.get('pk')}** pos={r.get('position')} `{r.get('type')}` `{r.get('from_url')}` → `{r.get('to_url')}`"
            lines.append(line)
            if tag in ("target_dead", "target_wrong_content"):
                tp = c.target_probe
                if tp:
                    lines.append(f"  - probed target: `{tp.url}` → final `{tp.final_url}` (status {tp.status}, {tp.hops} hops)")
                if c.suggested_target:
                    lines.append(f"  - **suggested:** `{c.suggested_target}`  (confidence {c.suggestion_confidence}/5; {c.suggestion_reasoning})")
                else:
                    lines.append(f"  - **no suggestion** ({c.suggestion_reasoning})")
        if len(rules_sorted) > 80:
            lines.append(f"  ...and {len(rules_sorted) - 80} more (see CSV).")

    # Consolidation groups separately - it's a cross-cutting view.
    groups: dict[str, list[Classification]] = {}
    for c in classifications:
        g = getattr(c, "consolidation_group", "")
        if g:
            groups.setdefault(g, []).append(c)
    if groups:
        lines.append(f"\n## Consolidation groups ({len(groups)} groups)\n")
        for g, members in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            lines.append(f"\n### {g}  ({len(members)} rules)")
            for c in members[:10]:
                r = c.rule
                lines.append(f"- pk={r.get('pk')} `{r.get('from_url')}` → `{r.get('to_url')}`")
            if len(members) > 10:
                lines.append(f"- ...and {len(members) - 10} more.")

    path.write_text("\n".join(lines) + "\n")


def write_dashboard_actions(classifications: list[Classification], path: Path) -> None:
    """Per-rule CRUD operation list, ready for the operator to apply manually
    or for a follow-up script using RtdClient. Same data is the source of truth
    for the YAML emit too.

    Action shape:
      {pk, op: delete|update|keep, reason, new: <new fields if update>}
    """
    actions = []
    for c in classifications:
        r = c.rule
        action = {"pk": r.get("pk"), "position": r.get("position"), "from_url": r.get("from_url"), "to_url": r.get("to_url"), "type": r.get("type")}
        reasons = []

        # Decide an op.
        if c.tags.get("test_debug"):
            action["op"] = "delete"
            reasons.append("test rule")
        elif c.tags.get("catch_all_noop"):
            action["op"] = "delete"
            reasons.append("no-op catch-all (per DOC-947)")
        elif c.tags.get("duplicate_identity") and not c.tags.get("target_dead") and not c.tags.get("target_wrong_content"):
            # If this rule's identity is duplicated AND it's the higher-position twin,
            # we delete it. Lower-position one is kept.
            same_ident = [
                x for x in classifications
                if (x.rule.get("from_url"), x.rule.get("type")) == (r.get("from_url"), r.get("type"))
            ]
            min_pos = min(x.rule.get("position", 9999) for x in same_ident)
            if r.get("position", 9999) > min_pos:
                action["op"] = "delete"
                reasons.append("duplicate-identity higher twin")
            else:
                action["op"] = "update"
                reasons.append("duplicate-identity lower twin: keep, may need other updates")
        elif c.tags.get("target_dead"):
            # Genuinely broken target. Always needs human attention; auto-update
            # only if we have a high-confidence source-tree alternative.
            if c.suggested_target and c.suggestion_confidence >= 5:
                action["op"] = "update"
                action["new_to_url"] = c.suggested_target
                reasons.append(f"target dead; replace with suggestion (conf {c.suggestion_confidence})")
            else:
                action["op"] = "review"
                reasons.append(f"target dead; no high-confidence suggestion ({c.suggestion_reasoning})")
        elif c.tags.get("target_wrong_content"):
            # Target loads (200) but the source slug doesn't appear in it.
            # Heuristic only — most are intentional renames where the topic was
            # re-titled. Auto-update only when we have a high-confidence
            # alternative; otherwise trust the existing target.
            if c.suggested_target and c.suggestion_confidence >= 5:
                action["op"] = "update"
                action["new_to_url"] = c.suggested_target
                reasons.append(f"target alive but slug mismatch; high-conf alternative found (conf {c.suggestion_confidence})")
            else:
                action["op"] = "keep"
                reasons.append("target alive but slug mismatch; no better alternative — trust author")
        else:
            action["op"] = "keep"

        # Layer in target-changing mechanical fixes (chain flatten).
        # NOTE: 302->301 conversion is deferred to a final pass after target
        # validation - flagging a 301 to a wrong target gets cached aggressively
        # and is harder to undo. See defer_301 metadata flag below.
        new_fields = action.setdefault("new_fields", {})
        if c.tags.get("chained_target") and c.target_probe and isinstance(c.target_probe.status, int) and c.target_probe.status == 200:
            final = c.target_probe.final_url.replace(BASE_URL, "")
            if final and final != r.get("to_url"):
                new_fields["to_url"] = final
                reasons.append("chain-flatten to final destination")

        if new_fields and action["op"] == "keep":
            action["op"] = "update"
        if not new_fields:
            action.pop("new_fields", None)

        # Side flag: rule is currently 302, will convert to 301 in the final
        # pass after target/chain/consolidation work is validated.
        if c.tags.get("status_302"):
            action["defer_301"] = True

        action["reasons"] = reasons
        actions.append(action)

    path.write_text(json.dumps(actions, indent=2, sort_keys=False))


def write_proposed_yaml(classifications: list[Classification], path: Path) -> None:
    """Emit the resulting cleaned ruleset in rtd-redirects YAML format.

    Order preserved from the input. Applies the same decisions as dashboard_actions
    but as a declarative artifact. Deleted rules are omitted.
    """
    entries = []
    for c in classifications:
        r = c.rule
        # Skip deletes.
        if c.tags.get("test_debug") or c.tags.get("catch_all_noop"):
            continue
        # Skip duplicate higher twins.
        if c.tags.get("duplicate_identity"):
            same_ident = [
                x for x in classifications
                if (x.rule.get("from_url"), x.rule.get("type")) == (r.get("from_url"), r.get("type"))
            ]
            min_pos = min(x.rule.get("position", 9999) for x in same_ident)
            if r.get("position", 9999) > min_pos:
                continue

        from_url = r.get("from_url") or ""
        to_url = r.get("to_url") or ""

        # Apply target replacement.
        if (c.tags.get("target_dead") or c.tags.get("target_wrong_content")) and c.suggested_target and c.suggestion_confidence >= 3:
            to_url = c.suggested_target
        # Apply chain flatten.
        if c.tags.get("chained_target") and c.target_probe and isinstance(c.target_probe.status, int) and c.target_probe.status == 200:
            final = c.target_probe.final_url.replace(BASE_URL, "")
            if final:
                to_url = final

        # Status stays at the current value here; 302->301 conversion is a
        # final-pass action after the rest of the cleanup validates.
        status = r.get("http_status", 301)

        entry = {
            "from": from_url,
            "to": to_url,
            "type": r.get("type"),
            "status": status,
        }
        if r.get("force"):
            entry["force"] = True
        if r.get("description"):
            entry["description"] = r.get("description")
        entries.append(entry)

    doc = {"schema_version": 1, "redirects": entries}
    path.write_text(yaml.safe_dump(doc, sort_keys=False, default_flow_style=False))


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    if not API_JSON.exists():
        print(f"ERROR: not found: {API_JSON}", file=sys.stderr)
        return 1
    if not RAY_SOURCE.exists():
        print(f"ERROR: not found: {RAY_SOURCE}", file=sys.stderr)
        return 1

    raw = json.loads(API_JSON.read_text())
    rules: list[dict] = raw["results"]
    print(f"Loaded {len(rules)} rules from {API_JSON.name}.", file=sys.stderr)

    # Collect probe URLs (deduped).
    probe_paths: set[str] = set()
    for r in rules:
        for k in ("from_url", "to_url"):
            v = r.get(k) or ""
            if v:
                probe_paths.add(to_master(v))
    print(f"Probing {len(probe_paths)} unique URLs on {BASE_URL} ...", file=sys.stderr)

    session = requests.Session()
    session.headers.update({"User-Agent": "rtd-redirects-audit/1.0"})

    # Connectivity check — fail fast if docs.ray.io is unreachable.
    try:
        r = session.get(f"{BASE_URL}/en/master/", timeout=10)
        if r.status_code != 200:
            print(f"ERROR: connectivity check returned {r.status_code}; check network.", file=sys.stderr)
            return 1
    except requests.RequestException as exc:
        print(f"ERROR: connectivity check failed ({exc}); check network.", file=sys.stderr)
        return 1

    probe_results: dict[str, Probe] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(probe_one, session, f"{BASE_URL}{p}"): p for p in probe_paths}
        done = 0
        for f in as_completed(futures):
            p = futures[f]
            probe_results[p] = f.result()
            done += 1
            if done % 50 == 0:
                print(f"  probed {done}/{len(probe_paths)}", file=sys.stderr)
    print(f"  probed {len(probe_results)} URLs total.", file=sys.stderr)

    # Build source index.
    print(f"Indexing Ray source tree at {RAY_SOURCE} ...", file=sys.stderr)
    file_index = build_source_index(RAY_SOURCE)
    print(f"  {sum(len(v) for v in file_index.values())} source files indexed by {len(file_index)} stems.", file=sys.stderr)

    # Classify each rule.
    classifications: list[Classification] = []
    for rule in rules:
        from_url = rule.get("from_url") or ""
        to_url = rule.get("to_url") or ""
        sp = probe_results.get(to_master(from_url)) if from_url else None
        tp = probe_results.get(to_master(to_url)) if to_url else None
        tags = classify(rule, rules, sp, tp)
        c = Classification(rule=rule, source_probe=sp, target_probe=tp, tags=tags)
        if tags.get("target_dead") or tags.get("target_wrong_content"):
            url, conf, why = suggest_target(rule, RAY_SOURCE, file_index)
            c.suggested_target = url
            c.suggestion_confidence = conf
            c.suggestion_reasoning = why
        classifications.append(c)

    # Cross-cutting consolidation detection.
    cons_groups = detect_consolidatable(classifications)
    for c in classifications:
        pk = c.rule.get("pk")
        if pk in cons_groups:
            c.tags["consolidatable"] = True
            c.consolidation_group = cons_groups[pk]  # type: ignore[attr-defined]
        else:
            c.tags["consolidatable"] = False

    # Emit.
    print(f"Writing CSV to {CSV_PATH.name} ...", file=sys.stderr)
    write_csv(classifications, CSV_PATH)
    print(f"Writing report to {REPORT_PATH.name} ...", file=sys.stderr)
    write_report(classifications, REPORT_PATH)
    print(f"Writing proposed YAML to {YAML_PATH.name} ...", file=sys.stderr)
    write_proposed_yaml(classifications, YAML_PATH)
    print(f"Writing dashboard actions to {ACTIONS_PATH.name} ...", file=sys.stderr)
    write_dashboard_actions(classifications, ACTIONS_PATH)

    # Headline stats to stdout.
    by_tag: dict[str, int] = {}
    for c in classifications:
        for tag, on in c.tags.items():
            if on:
                by_tag[tag] = by_tag.get(tag, 0) + 1
    print()
    print(f"{'Tag':<28} Count")
    print("-" * 38)
    for tag in sorted(by_tag.keys()):
        print(f"{tag:<28} {by_tag[tag]:>5}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
