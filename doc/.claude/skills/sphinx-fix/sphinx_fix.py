#!/usr/bin/env python
"""Classify Ray Sphinx/MyST doc-build warnings against a rules table.

Read-only diagnostic. Reads a Sphinx warning stream (a file, stdin, or pasted
text -- either the `/rtd-build-logs warnings` output or a raw `sphinx-build`
log / `-w` warnings file), parses each warning into a record, classifies it
against `rules.yaml`, and prints the canonical fix for each. v0 is
human-in-the-loop: this tool proposes fixes and never edits files.

It triages by severity tier (1 fatal/abort, 2 structural/parse, 3 plain
warning), detects a hard-broken build first, segregates known-benign
suppressed classes, and emits every unmatched warning as an explicit
unclassified list so the rules table can grow from real misses.

Stdlib-only. Prefers PyYAML to read the rules table but falls back to a small
restricted-grammar parser so it runs in a bare environment.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_RULES = Path(__file__).resolve().parent / "rules.yaml"


# --------------------------------------------------------------------------- #
# Records
# --------------------------------------------------------------------------- #
@dataclass
class SphinxWarning:
    path: str | None
    line: int | None
    level: str
    message: str
    category: str | None
    raw: str


@dataclass
class Rule:
    id: str
    title: str
    tier: int
    safety: str
    match: str
    categories: list[str]
    signatures: list[re.Pattern]
    cause: str
    fix: str
    fix_template: str | None
    target_extract: re.Pattern | None
    version_sphinx: str | None
    version_myst: str | None
    notes: str | None


@dataclass
class Suppression:
    id: str
    categories: list[str]
    signatures: list[re.Pattern]
    location_contains: str | None
    reason: str
    tracked_by: str | None


@dataclass
class Finding:
    warning: SphinxWarning
    rule_id: str
    tier: int
    fix: str
    target: str | None
    safety: str
    version_ok: bool


@dataclass
class AbortSignal:
    signature: str
    excerpt: list[str]
    warning_tail_present: bool


@dataclass
class BuildState:
    state: str | None = None
    success: bool | None = None
    summary: str | None = None
    declared_warning_count: int | None = None
    exit_code: int | None = None


@dataclass
class Report:
    abort: AbortSignal | None
    findings: list[Finding]
    unclassified: list[SphinxWarning]
    suppressed: list[tuple[SphinxWarning, Suppression]]
    build_state: BuildState
    baseline: dict = field(default_factory=dict)


class RulesError(Exception):
    """Raised when rules.yaml is malformed."""


# --------------------------------------------------------------------------- #
# Restricted-grammar YAML fallback (used only when PyYAML is absent)
# --------------------------------------------------------------------------- #
def _strip_comment(line: str) -> str:
    """Drop a trailing/full-line `#` comment, ignoring `#` inside quotes."""
    out: list[str] = []
    dq = False
    i, n = 0, len(line)
    while i < n:
        c = line[i]
        if dq:
            out.append(c)
            if c == "\\" and i + 1 < n:
                out.append(line[i + 1])
                i += 2
                continue
            if c == '"':
                dq = False
            i += 1
            continue
        if c == '"':
            dq = True
            out.append(c)
            i += 1
            continue
        if c == "#" and (i == 0 or line[i - 1] == " "):
            break
        out.append(c)
        i += 1
    return "".join(out)


def _unescape_dq(s: str) -> str:
    table = {"\\": "\\", '"': '"', "n": "\n", "t": "\t", "r": "\r", "/": "/", "0": "\0"}
    out: list[str] = []
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        if c == "\\" and i + 1 < n:
            out.append(table.get(s[i + 1], s[i + 1]))
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _scalar(s: str):
    s = s.strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        return _unescape_dq(s[1:-1])
    if s in ("", "null", "~"):
        return None
    if s == "true":
        return True
    if s == "false":
        return False
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    return s


_KEY_RE = re.compile(r"^([A-Za-z0-9_]+):(?:\s+(.*))?$")


def _yaml_fallback(text: str):
    items: list[list] = []
    for raw in text.split("\n"):
        content = _strip_comment(raw)
        if not content.strip():
            continue
        indent = len(content) - len(content.lstrip(" "))
        items.append([indent, content.strip()])
    if not items:
        # Empty / comment-only / whitespace-only input. Match PyYAML's
        # safe_load("") -> None so load_rules raises a clean RulesError and
        # _crosscheck_yaml stays in agreement with PyYAML.
        return None
    pos = [0]

    def peek():
        return items[pos[0]] if pos[0] < len(items) else None

    def value_after_key(key_indent: int):
        nxt = peek()
        if nxt is None or nxt[0] <= key_indent:
            return None
        return parse_node(nxt[0])

    def parse_map_keys(d: dict, indent: int) -> None:
        while True:
            cur = peek()
            if cur is None or cur[0] != indent:
                break
            m = _KEY_RE.match(cur[1])
            if not m:
                break
            pos[0] += 1
            key, val = m.group(1), m.group(2)
            if val:
                d[key] = _scalar(val)
            else:
                d[key] = value_after_key(indent)

    def parse_node(indent: int):
        cur = peek()
        if cur[1].startswith("- "):
            seq: list = []
            while True:
                cur = peek()
                if cur is None or cur[0] != indent or not cur[1].startswith("- "):
                    break
                head = cur[1][2:].strip()
                pos[0] += 1
                m = _KEY_RE.match(head)
                if m:
                    d: dict = {}
                    key, val = m.group(1), m.group(2)
                    if val:
                        d[key] = _scalar(val)
                    else:
                        d[key] = value_after_key(indent + 2)
                    parse_map_keys(d, indent + 2)
                    seq.append(d)
                else:
                    seq.append(_scalar(head))
            return seq
        d = {}
        parse_map_keys(d, indent)
        return d

    return parse_node(0)


def _load_yaml_text(text: str):
    try:
        import yaml
    except ImportError:
        return _yaml_fallback(text)
    return yaml.safe_load(text)


# --------------------------------------------------------------------------- #
# Rule loading
# --------------------------------------------------------------------------- #
def _compile_regexes(values, where: str) -> list[re.Pattern]:
    out = []
    for v in values or []:
        try:
            out.append(re.compile(v))
        except re.error as e:
            raise RulesError(f"bad regex in {where}: {v!r} ({e})") from e
    return out


def _compile_rule(raw: dict) -> Rule:
    rid = raw.get("id")
    if not rid:
        raise RulesError(f"rule missing id: {raw!r}")
    categories = raw.get("categories") or []
    signatures = _compile_regexes(raw.get("signatures"), f"rule {rid} signatures")
    if not categories and not signatures:
        raise RulesError(f"rule {rid} has neither categories nor signatures")
    tier = raw.get("tier")
    if tier not in (1, 2, 3):
        raise RulesError(f"rule {rid} has invalid tier {tier!r}")
    safety = raw.get("safety")
    if safety not in ("mechanical", "judgment"):
        raise RulesError(f"rule {rid} has invalid safety {safety!r}")
    tex = raw.get("target_extract")
    target_extract = re.compile(tex) if tex else None
    return Rule(
        id=rid,
        title=raw.get("title", rid),
        tier=tier,
        safety=safety,
        match=raw.get("match", "any"),
        categories=categories,
        signatures=signatures,
        cause=raw.get("cause", ""),
        fix=raw.get("fix", ""),
        fix_template=raw.get("fix_template"),
        target_extract=target_extract,
        version_sphinx=raw.get("version_sphinx"),
        version_myst=raw.get("version_myst"),
        notes=raw.get("notes"),
    )


def _compile_suppression(raw: dict) -> Suppression:
    sid = raw.get("id", "?")
    return Suppression(
        id=sid,
        categories=raw.get("categories") or [],
        signatures=_compile_regexes(raw.get("signatures"), f"suppression {sid}"),
        location_contains=raw.get("location_contains"),
        reason=raw.get("reason", ""),
        tracked_by=raw.get("tracked_by"),
    )


def load_rules(path: Path) -> tuple[list[Rule], list[Suppression], dict]:
    try:
        text = path.read_text()
    except OSError as e:
        raise RulesError(f"cannot read rules file {path}: {e}") from e
    data = _load_yaml_text(text)
    if not isinstance(data, dict):
        raise RulesError(f"rules file {path} did not parse to a mapping")
    rules = [_compile_rule(r) for r in data.get("rules", [])]
    supps = [_compile_suppression(s) for s in data.get("suppressions", [])]
    seen: set[str] = set()
    for r in rules:
        if r.id in seen:
            raise RulesError(f"duplicate rule id: {r.id}")
        seen.add(r.id)
    baseline = {
        "sphinx": data.get("baseline_sphinx"),
        "myst_parser": data.get("baseline_myst"),
    }
    return rules, supps, baseline


# --------------------------------------------------------------------------- #
# Parsing the warning stream
# --------------------------------------------------------------------------- #
# Sphinx warning line: "<path>[:<line>]: LEVEL: <message> [<category>]".
# The line number is optional -- some classes (e.g. misc.copy_overwrite) warn
# on a whole file with no line.
WARN_RE = re.compile(
    r"^(?P<path>.*?):(?:(?P<line>\d+):)?\s+(?P<level>WARNING|ERROR|SEVERE):\s+"
    r"(?P<msg>.*?)(?:\s+\[(?P<category>[\w.]+)\])?$"
)
# Bare Sphinx warning with no path/line (e.g. "WARNING: extension ...").
BARE_RE = re.compile(
    r"^(?P<level>WARNING|ERROR|SEVERE):\s+"
    r"(?P<msg>.*?)(?:\s+\[(?P<category>[\w.]+)\])?$"
)
# Strip the Read the Docs checkout prefix so paths are repo-relative. The RtD
# path contains "/checkouts/" twice (.../checkouts/readthedocs.org/... and
# .../checkouts/<version>/<repo-relative>), so match greedily to the LAST one.
CHECKOUT_RE = re.compile(r"^.*/checkouts/[^/]+/")
STATE_RE = re.compile(r"State:\s+(?P<state>\w+)\s+Success:\s+(?P<success>\w+)")
EXIT_RE = re.compile(r"^Exit code:\s+(?P<code>\d+)")
SUMMARY_RE = re.compile(r"build (?:finished|succeeded)[^\n]*", re.IGNORECASE)
DECLARED_RE = re.compile(r"build finished with problems,\s+(?P<n>\d+)\s+warning", re.I)

ABORT_RES = [
    ("traceback", re.compile(r"^Traceback \(most recent call last\):")),
    ("extension-error", re.compile(r"Extension error")),
    ("import-error", re.compile(r"Could not import extension")),
    ("severe", re.compile(r"(?:^|\s)SEVERE:\s")),
    ("sphinx-error", re.compile(r"^Sphinx error:")),
]


def strip_checkout_prefix(path: str) -> str:
    return CHECKOUT_RE.sub("", path)


def is_noise(line: str) -> bool:
    """True for python-logging lines and JSON log records (not Sphinx warnings)."""
    if "\tWARNING " in line or "\tERROR " in line:
        return True
    s = line.strip()
    return s.startswith("{") and '"levelname"' in s


def parse_warnings(text: str) -> list[SphinxWarning]:
    out: list[SphinxWarning] = []
    for raw in text.splitlines():
        if is_noise(raw):
            continue
        s = raw.strip()
        if not s:
            continue
        m = WARN_RE.match(s)
        if m:
            out.append(
                SphinxWarning(
                    path=strip_checkout_prefix(m.group("path")),
                    line=int(m.group("line")) if m.group("line") else None,
                    level=m.group("level"),
                    message=m.group("msg").strip(),
                    category=m.group("category"),
                    raw=raw,
                )
            )
            continue
        m = BARE_RE.match(s)
        if m:
            out.append(
                SphinxWarning(
                    path=None,
                    line=None,
                    level=m.group("level"),
                    message=m.group("msg").strip(),
                    category=m.group("category"),
                    raw=raw,
                )
            )
    return out


def parse_build_state(text: str) -> BuildState:
    st = BuildState()
    for raw in text.splitlines():
        s = raw.strip()
        m = STATE_RE.search(s)
        if m:
            st.state = m.group("state")
            st.success = {"true": True, "false": False}.get(m.group("success").lower())
        m = EXIT_RE.match(s)
        if m:
            st.exit_code = int(m.group("code"))
        m = SUMMARY_RE.search(s)
        if m:
            st.summary = m.group(0).strip()
        m = DECLARED_RE.search(s)
        if m:
            st.declared_warning_count = int(m.group("n"))
    return st


def detect_abort(text: str, state: BuildState) -> AbortSignal | None:
    """Detect a hard-broken build that aborted before the warning pass.

    Only fires when an abort signature is present AND the build did not
    complete -- i.e. there is no "build finished/succeeded" summary and the
    state does not report success. Guards against false-firing on a healthy
    build whose text merely quotes "Extension error" etc.
    """
    completed = bool(SUMMARY_RE.search(text))
    if completed or state.success is True:
        return None
    lines = text.splitlines()
    for i, raw in enumerate(lines):
        for name, rx in ABORT_RES:
            if rx.search(raw):
                excerpt = [ln.rstrip() for ln in lines[i : i + 5]]
                return AbortSignal(name, excerpt, warning_tail_present=completed)
    return None


# --------------------------------------------------------------------------- #
# Version comparator (no `packaging` dependency)
# --------------------------------------------------------------------------- #
def _parse_ver(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in re.findall(r"\d+", v))


def _tuplecmp(a: tuple[int, ...], b: tuple[int, ...]) -> int:
    n = max(len(a), len(b))
    a = a + (0,) * (n - len(a))
    b = b + (0,) * (n - len(b))
    return (a > b) - (a < b)


def version_in_range(actual: str | None, spec: str | None) -> bool:
    if not spec or not actual:
        return True
    av = _parse_ver(actual)
    for clause in spec.split(","):
        clause = clause.strip()
        m = re.match(r"(>=|<=|==|>|<)\s*(.+)", clause)
        if not m:
            continue
        op, bv = m.group(1), _parse_ver(m.group(2))
        c = _tuplecmp(av, bv)
        ok = {
            ">=": c >= 0,
            ">": c > 0,
            "<=": c <= 0,
            "<": c < 0,
            "==": c == 0,
        }[op]
        if not ok:
            return False
    return True


# --------------------------------------------------------------------------- #
# Classify
# --------------------------------------------------------------------------- #
def _rule_fires(rule: Rule, cat_hit: bool, sig_hit: bool) -> bool:
    if rule.match == "all":
        cat_ok = cat_hit if rule.categories else True
        sig_ok = sig_hit if rule.signatures else True
        return cat_ok and sig_ok
    return cat_hit or sig_hit


def _extract_target(rule: Rule, w: SphinxWarning):
    target = sec = None
    if rule.target_extract:
        m = rule.target_extract.search(w.message)
        if m:
            gd = m.groupdict()
            target = gd.get("target")
            sec = gd.get("sec")
    stem = None
    if target:
        stem = re.sub(r"\.(rst|md|html)$", "", target).split("#")[0]
    return target, stem, sec


def _format_fix(
    rule: Rule, target: str | None, stem: str | None, sec: str | None
) -> str:
    fix = rule.fix
    if rule.fix_template:
        rep = (
            rule.fix_template.replace("{target_stem}", stem or target or "DOC")
            .replace("{target}", target or "DOC")
            .replace("{sec}", sec or "section")
        )
        fix = f"{fix}  Suggested: {rep}"
    return fix


def match_warning(
    w: SphinxWarning, rules: list[Rule], versions: dict | None
) -> Finding | None:
    candidates: list[tuple[Rule, bool, bool]] = []
    for r in rules:
        cat_hit = bool(r.categories) and w.category in r.categories
        sig_hit = any(rx.search(w.message) for rx in r.signatures)
        if _rule_fires(r, cat_hit, sig_hit):
            candidates.append((r, cat_hit, sig_hit))
    if not candidates:
        return None
    # Prefer a signature hit over a category-only hit; stable sort keeps the
    # file's most-specific-first order among equals.
    candidates.sort(key=lambda c: 0 if c[2] else 1)
    rule = candidates[0][0]
    target, stem, sec = _extract_target(rule, w)
    version_ok = True
    if versions:
        version_ok = version_in_range(
            versions.get("sphinx"), rule.version_sphinx
        ) and version_in_range(versions.get("myst_parser"), rule.version_myst)
    return Finding(
        warning=w,
        rule_id=rule.id,
        tier=rule.tier,
        fix=_format_fix(rule, target, stem, sec),
        target=target,
        safety=rule.safety,
        version_ok=version_ok,
    )


def find_suppression(w: SphinxWarning, supps: list[Suppression]) -> Suppression | None:
    for s in supps:
        cat_hit = bool(s.categories) and w.category in s.categories
        sig_hit = any(rx.search(w.message) for rx in s.signatures)
        if not (cat_hit or sig_hit):
            continue
        if s.location_contains:
            if w.path and s.location_contains in w.path:
                return s
            continue
        return s
    return None


def classify(warnings, rules, supps, versions):
    findings: list[Finding] = []
    unclassified: list[SphinxWarning] = []
    suppressed: list[tuple[SphinxWarning, Suppression]] = []
    for w in warnings:
        s = find_suppression(w, supps)
        if s:
            suppressed.append((w, s))
            continue
        f = match_warning(w, rules, versions)
        if f:
            findings.append(f)
        else:
            unclassified.append(w)
    findings.sort(key=lambda f: (f.tier, f.rule_id))
    return findings, unclassified, suppressed


def build_report(text, rules, supps, versions, baseline=None) -> Report:
    state = parse_build_state(text)
    abort = detect_abort(text, state)
    warnings = parse_warnings(text)
    findings, unclassified, suppressed = classify(warnings, rules, supps, versions)
    return Report(abort, findings, unclassified, suppressed, state, baseline or {})


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
TIER_LABEL = {
    1: "fatal/abort",
    2: "structural/parse (fix first; these mask warnings beneath them)",
    3: "warnings",
}


def _loc(w: SphinxWarning) -> str:
    if w.path and w.line is not None:
        return f"{w.path}:{w.line}"
    if w.path:
        return w.path
    return "(no location)"


def exit_code_for(report: Report) -> int:
    if report.abort:
        return 2
    if report.findings:
        return 1
    if report.unclassified:
        return 3
    return 0


def render_human(report: Report) -> str:
    out: list[str] = []
    st = report.build_state

    if report.abort:
        out.append(
            "✗ HARD-BROKEN BUILD — fix this first; the rest of the "
            "log is unreliable."
        )
        out.append(f"  signal: {report.abort.signature}")
        for ln in report.abort.excerpt:
            out.append(f"    {ln}")
        out.append("  The warning list below is incomplete until this is fixed.")
        out.append("")

    bits = []
    if st.state:
        bits.append(f"state={st.state}")
    if st.success is not None:
        bits.append(f"success={st.success}")
    if st.exit_code is not None:
        bits.append(f"exit={st.exit_code}")
    if st.summary:
        bits.append(st.summary)
    if bits:
        out.append("Build: " + "  ".join(bits))
        out.append("")

    header = (
        "Partial findings (warning pass did not complete)"
        if report.abort
        else "Findings"
    )
    out.append(f"{header} ({len(report.findings)}):")
    if not report.findings:
        out.append("  (none)")
    else:
        by_tier: dict[int, list[Finding]] = {}
        for f in report.findings:
            by_tier.setdefault(f.tier, []).append(f)
        for tier in sorted(by_tier):
            out.append(f"  Tier {tier} — {TIER_LABEL[tier]}:")
            for f in by_tier[tier]:
                flag = "" if f.version_ok else "  (unvalidated for this version)"
                out.append(f"    [T{tier}] {f.rule_id}  {_loc(f.warning)}{flag}")
                out.append(f"          msg:    {f.warning.message}")
                out.append(f"          fix:    {f.fix}")
                safety = f.safety
                if safety == "judgment":
                    safety += " (needs your call)"
                out.append(f"          safety: {safety}")
    out.append("")

    out.append(
        f"Unclassified ({len(report.unclassified)}) — no rule matched; "
        "resolve with the user, then file a skill-improvement ticket to add a rule:"
    )
    for w in report.unclassified:
        cat = f" [{w.category}]" if w.category else ""
        out.append(f"  {_loc(w)}: {w.level}: {w.message}{cat}")
    out.append("")

    out.append(f"Suppressed ({len(report.suppressed)}) — known-benign, not actionable:")
    counts: dict[str, tuple[str | None, int]] = {}
    for _w, s in report.suppressed:
        tb, n = counts.get(s.id, (s.tracked_by, 0))
        counts[s.id] = (tb, n + 1)
    for sid in sorted(counts):
        tb, n = counts[sid]
        track = f" ({tb})" if tb else ""
        out.append(f"  {sid}{track}: {n}")
    out.append("")

    out.append(_verdict(report))
    return "\n".join(out)


def _verdict(report: Report) -> str:
    if report.abort:
        return "Next: fix the hard-broken build above, then rebuild and re-run."
    if report.findings:
        top = min(f.tier for f in report.findings)
        if top <= 2:
            return (
                f"Next: fix Tier {top} first (it masks others), then rebuild and "
                "re-run — do not assume one pass is complete."
            )
        return "Next: apply the fixes above, then rebuild and re-run to confirm clean."
    if report.unclassified:
        return (
            "No rule matched the warning(s) above — resolve with the user and "
            "extend rules.yaml."
        )
    return "✓ No actionable warnings."


def render_json(report: Report) -> str:
    def w2d(w: SphinxWarning) -> dict:
        return {
            "path": w.path,
            "line": w.line,
            "level": w.level,
            "message": w.message,
            "category": w.category,
        }

    payload = {
        "schema": 1,
        "abort": (
            None
            if not report.abort
            else {
                "signature": report.abort.signature,
                "excerpt": report.abort.excerpt,
            }
        ),
        "build_state": {
            "state": report.build_state.state,
            "success": report.build_state.success,
            "summary": report.build_state.summary,
            "exit_code": report.build_state.exit_code,
            "declared_warning_count": report.build_state.declared_warning_count,
        },
        "findings": [
            {
                "rule_id": f.rule_id,
                "tier": f.tier,
                "safety": f.safety,
                "fix": f.fix,
                "target": f.target,
                "version_ok": f.version_ok,
                **w2d(f.warning),
            }
            for f in report.findings
        ],
        "unclassified": [w2d(w) for w in report.unclassified],
        "suppressed": [
            {"suppression_id": s.id, "tracked_by": s.tracked_by, **w2d(w)}
            for w, s in report.suppressed
        ],
        "exit_code": exit_code_for(report),
    }
    return json.dumps(payload, indent=2)


def explain(rule_id: str, rules: list[Rule]) -> str:
    for r in rules:
        if r.id == rule_id:
            lines = [
                f"{r.id}  (tier {r.tier}, {r.safety})",
                f"  title:      {r.title}",
                f"  categories: {', '.join(r.categories) or '(none)'}",
                f"  signatures: {', '.join(p.pattern for p in r.signatures) or '(none)'}",
                f"  cause:      {r.cause}",
                f"  fix:        {r.fix}",
            ]
            if r.fix_template:
                lines.append(f"  template:   {r.fix_template}")
            ver = f"sphinx {r.version_sphinx or '*'}, myst {r.version_myst or '*'}"
            lines.append(f"  validated:  {ver}")
            if r.notes:
                lines.append(f"  notes:      {r.notes}")
            return "\n".join(lines)
    return f"No rule with id {rule_id!r}. Known ids: {', '.join(r.id for r in rules)}"


# --------------------------------------------------------------------------- #
# Selftest
# --------------------------------------------------------------------------- #
def _selftest_invariants(rules, supps, baseline) -> list[str]:
    errs: list[str] = []
    if not baseline.get("sphinx") or not baseline.get("myst_parser"):
        errs.append("rules.yaml missing baseline_sphinx/baseline_myst")
    for r in rules:
        if not r.categories and not r.signatures:
            errs.append(f"{r.id}: no matchers")
        if r.tier not in (1, 2, 3):
            errs.append(f"{r.id}: bad tier")
        if r.safety not in ("mechanical", "judgment"):
            errs.append(f"{r.id}: bad safety")
    # version comparator spot checks
    if not version_in_range("8.2.3", ">=8.0,<9"):
        errs.append("version_in_range: 8.2.3 should satisfy >=8.0,<9")
    if version_in_range("9.0.0", ">=8.0,<9"):
        errs.append("version_in_range: 9.0.0 should NOT satisfy >=8.0,<9")
    return errs


def _crosscheck_yaml(rules_path: Path) -> list[str]:
    try:
        import yaml
    except ImportError:
        return []
    text = rules_path.read_text()
    pyyaml = yaml.safe_load(text)
    fallback = _yaml_fallback(text)
    if pyyaml != fallback:
        return [
            "YAML fallback parser disagrees with PyYAML on rules.yaml "
            "(grammar drift). Keep rules.yaml within the documented subset."
        ]
    return []


def run_selftest(rules_path: Path, update_golden: bool) -> int:
    skill_dir = Path(__file__).resolve().parent
    fx_dir = skill_dir / "tests" / "fixtures"
    gold_dir = skill_dir / "tests" / "golden"
    rules, supps, baseline = load_rules(rules_path)

    errs = _selftest_invariants(rules, supps, baseline)
    errs += _crosscheck_yaml(rules_path)

    fixtures = sorted(fx_dir.glob("*.txt")) if fx_dir.is_dir() else []
    if not fixtures:
        errs.append(f"no fixtures found under {fx_dir}")

    for fx in fixtures:
        report = build_report(
            fx.read_text(), rules, supps, versions=None, baseline=baseline
        )
        got = render_human(report) + "\n"
        gold = gold_dir / fx.name
        if update_golden:
            gold_dir.mkdir(parents=True, exist_ok=True)
            gold.write_text(got)
            continue
        if not gold.is_file():
            errs.append(f"missing golden: {gold.name} (run --update-golden)")
            continue
        want = gold.read_text()
        if got != want:
            import difflib

            diff = "".join(
                difflib.unified_diff(
                    want.splitlines(True),
                    got.splitlines(True),
                    fromfile=f"golden/{fx.name}",
                    tofile=f"got/{fx.name}",
                )
            )
            errs.append(f"golden mismatch {fx.name}:\n{diff}")

    if update_golden:
        print(f"Wrote {len(fixtures)} golden file(s).")
        return 0
    if errs:
        print("SELFTEST FAILED:")
        for e in errs:
            print(f"  - {e}")
        return 1
    print(
        f"SELFTEST OK ({len(fixtures)} fixtures, {len(rules)} rules, "
        f"{len(supps)} suppressions)."
    )
    return 0


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def read_stream(args) -> str:
    src = args.file or args.input
    if src and src != "-":
        return Path(src).read_text()
    if sys.stdin.isatty():
        sys.exit(
            "No input. Pipe a warning stream in, pass a file, or use '-' for stdin.\n"
            "  e.g. rtd.py warnings --pr 64135 | sphinx_fix.py"
        )
    return sys.stdin.read()


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sphinx_fix.py", description=__doc__.splitlines()[0]
    )
    p.add_argument("input", nargs="?", help="warnings/log file ('-' or omit for stdin)")
    p.add_argument("--file", help="explicit input file (alternative to positional)")
    p.add_argument("--rules", type=Path, default=DEFAULT_RULES, help="rules.yaml path")
    p.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    p.add_argument("--explain", metavar="RULE_ID", help="print one rule and exit")
    p.add_argument(
        "--sphinx-version", help="declare running Sphinx for the version gate"
    )
    p.add_argument("--myst-version", help="declare running myst-parser")
    p.add_argument("--no-color", action="store_true", help="accepted; output is plain")
    p.add_argument("--selftest", action="store_true", help="run fixtures vs golden")
    p.add_argument(
        "--update-golden",
        action="store_true",
        help="(re)write golden files from current output",
    )
    return p


def main(argv=None) -> int:
    args = build_arg_parser().parse_args(argv)

    if args.selftest or args.update_golden:
        return run_selftest(args.rules, args.update_golden)

    try:
        rules, supps, baseline = load_rules(args.rules)
    except RulesError as e:
        sys.exit(f"rules error: {e}")

    if args.explain:
        print(explain(args.explain, rules))
        return 0

    versions = None
    if args.sphinx_version or args.myst_version:
        versions = {"sphinx": args.sphinx_version, "myst_parser": args.myst_version}

    text = read_stream(args)
    report = build_report(text, rules, supps, versions, baseline)
    print(render_json(report) if args.json else render_human(report))
    return exit_code_for(report)


if __name__ == "__main__":
    sys.exit(main())
