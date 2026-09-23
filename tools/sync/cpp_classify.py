#!/usr/bin/env python3
"""Classify shared C++ bridge files between an internal tree and the open-source tree.

Usage:
    cpp_classify.py <internal_root> <open_source_root>

For every `.cc` / `.h` file that exists under
`src/ray/core_worker/lib/go/` in BOTH trees, emit a TSV line:

    STATUS<TAB>path<TAB>notes

STATUS is one of:

    SAME      the two raw files are byte-identical
    COSMETIC  differ raw, but equal after normalization (comments / includes /
              include guards / whitespace stripped, both sides run through
              clang-format -style=file)
    REAL      still differ after normalization; a semantic gap the migration
              must look at

The normalization pipeline is deliberately conservative: it removes only things
that cannot change program semantics. Anything left is a candidate REAL diff.

Notes (comma separated, may be empty):

    CF_NEEDED          the file only became equal after clang-format ran
    CF_FAIL(int|oss)   clang-format failed on that side; the unformatted
                       normalized text was used instead
    INC_ADD=a;b        header basenames present only in the open-source side
    INC_DEL=a;b        header basenames present only in the internal side
    INC_RAW=...        include spellings differ beyond path normalization

Normalized texts of both sides are also written under --work-dir so the REAL
diffs can be inspected afterwards. Results are cached (keyed by the raw content
hashes) in --cache so re-runs are cheap.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys

SCRIPT_VERSION = "3"

SUBDIR = os.path.join("src", "ray", "core_worker", "lib", "go")
EXTENSIONS = (".cc", ".h")

# ---------------------------------------------------------------------------
# Source normalization
# ---------------------------------------------------------------------------

_RAW_STRING_RE = re.compile(r'R"([A-Za-z0-9_()\\ ]{0,16})\(')


def _strip_comments(text: str) -> str:
    """Remove // and /* */ comments while respecting string literals."""
    out = []
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        if c == '"':
            # Raw string literal?
            m = _RAW_STRING_RE.match(text, i)
            if m:
                delim = m.group(1)
                end = text.find(")" + delim + '"', m.end())
                if end == -1:
                    out.append(text[i:])
                    i = n
                else:
                    end += len(delim) + 2
                    out.append(text[i:end])
                    i = end
                continue
            j = i + 1
            while j < n:
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == '"':
                    j += 1
                    break
                j += 1
            out.append(text[i:j])
            i = j
            continue
        if c == "'":
            j = i + 1
            while j < n:
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == "'":
                    j += 1
                    break
                j += 1
            out.append(text[i:j])
            i = j
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            j = text.find("\n", i)
            if j == -1:
                j = n
            out.append("\n")
            i = j
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            j = text.find("*/", i + 2)
            if j == -1:
                j = n
            else:
                j += 2
            out.append(" ")
            i = j
            continue
        out.append(c)
        i += 1
    return "".join(out)


_INCLUDE_RE = re.compile(r'^\s*#\s*include\s*([<"])([^>"]*)[>"]')
_GUARD_DEF_RE = re.compile(r"^\s*#\s*ifndef\s+([A-Za-z_][A-Za-z0-9_]*)")
_GUARD_DEFINE_RE = re.compile(r"^\s*#\s*define\s+([A-Za-z_][A-Za-z0-9_]*)")
_PRAGMA_ONCE_RE = re.compile(r"^\s*#\s*pragma\s+once\b")
_IF_RE = re.compile(r"^\s*#\s*(if|ifdef|ifndef)\b")
_ENDIF_RE = re.compile(r"^\s*#\s*endif\b")


def _strip_include_guard(lines: list[str]) -> list[str]:
    """Drop a whole-file include guard (#pragma once or #ifndef X/#define X)."""
    first = 0
    while first < len(lines) and not lines[first].strip():
        first += 1
    if first >= len(lines):
        return lines

    head = lines[first]
    if _PRAGMA_ONCE_RE.match(head):
        return lines[:first] + lines[first + 1 :]

    m = _GUARD_DEF_RE.match(head)
    if not m or first + 1 >= len(lines):
        return lines
    if not _GUARD_DEFINE_RE.match(lines[first + 1]):
        return lines
    if _GUARD_DEFINE_RE.match(lines[first + 1]).group(1) != m.group(1):
        return lines

    # Walk the nesting to find the #endif that closes the guard.
    depth = 1
    close = None
    for idx in range(first + 2, len(lines)):
        if _IF_RE.match(lines[idx]):
            depth += 1
        elif _ENDIF_RE.match(lines[idx]):
            depth -= 1
            if depth == 0:
                close = idx
                break
    if close is None:
        return lines
    # The guard ends the file (allow trailing blanks) only if nothing follows.
    if any(l.strip() for l in lines[close + 1 :]):
        return lines
    return lines[:first] + lines[first + 2 : close]


def normalize(text: str) -> tuple[str, set[str], set[str]]:
    """Return (normalized text, include spellings, ...).

    Steps: unify newlines, strip comments, strip include guards, drop #include
    lines, strip each line, drop blank lines.
    """
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _strip_comments(text)

    includes: set[str] = set()
    kept = []
    for line in text.split("\n"):
        m = _INCLUDE_RE.match(line)
        if m:
            includes.add(m.group(2).strip())
            continue
        kept.append(line)

    kept = _strip_include_guard(kept)

    out = [line.strip() for line in kept]
    out = [line for line in out if line]
    return "\n".join(out) + "\n", includes


def include_basenames(includes: set[str]) -> set[str]:
    return {os.path.basename(inc) for inc in includes}


# ---------------------------------------------------------------------------
# clang-format
# ---------------------------------------------------------------------------


def run_clang_format(text: str, assume_filename: str) -> tuple[str | None, str]:
    """Return (formatted_text_or_None, error)."""
    try:
        proc = subprocess.run(
            ["clang-format", "-style=file", f"--assume-filename={assume_filename}"],
            input=text,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except FileNotFoundError:
        return None, "clang-format not found"
    except subprocess.TimeoutExpired:
        return None, "timeout"
    if proc.returncode != 0:
        return None, (proc.stderr or "nonzero exit").strip().splitlines()[0][:200]
    return proc.stdout, ""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def enumerate_files(root: str) -> list[str]:
    base = os.path.join(root, SUBDIR)
    if not os.path.isdir(base):
        sys.exit(f"error: {base} is not a directory")
    files = []
    for name in os.listdir(base):
        if name.endswith(EXTENSIONS) and os.path.isfile(os.path.join(base, name)):
            files.append(name)
    return sorted(files)


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return fh.read()


def classify(internal_path: str, oss_path: str, internal_root: str, oss_root: str,
             work_dir: str, rel: str) -> dict:
    raw_int = read_text(internal_path)
    raw_oss = read_text(oss_path)

    result: dict = {"path": rel, "notes": []}

    if raw_int == raw_oss:
        result["status"] = "SAME"
        return result

    norm_int, inc_int = normalize(raw_int)
    norm_oss, inc_oss = normalize(raw_oss)

    # --- include set comparison (kept separate from the text comparison) ---
    base_int = include_basenames(inc_int)
    base_oss = include_basenames(inc_oss)
    added = sorted(base_oss - base_int)
    removed = sorted(base_int - base_oss)
    if added:
        result["notes"].append("INC_ADD=" + ";".join(added))
    if removed:
        result["notes"].append("INC_DEL=" + ";".join(removed))
    if not added and not removed and inc_int != inc_oss:
        result["notes"].append("INC_RAW=" + ",".join(sorted(inc_oss ^ inc_int)))

    # --- clang-format both normalized sides ---
    fmt_int, err_int = run_clang_format(
        norm_int, os.path.join(internal_root, SUBDIR, rel)
    )
    fmt_oss, err_oss = run_clang_format(norm_oss, os.path.join(oss_root, SUBDIR, rel))
    if err_int:
        result["notes"].append("CF_FAIL(int):" + err_int)
    if err_oss:
        result["notes"].append("CF_FAIL(oss):" + err_oss)

    cmp_int = fmt_int if fmt_int is not None else norm_int
    cmp_oss = fmt_oss if fmt_oss is not None else norm_oss

    if cmp_int == cmp_oss:
        result["status"] = "COSMETIC"
        if norm_int != norm_oss:
            # Only clang-format made them match: pure style difference.
            result["notes"].append("CF_NEEDED")
    else:
        result["status"] = "REAL"
        if fmt_int is None and fmt_oss is None:
            result["notes"].append("CF_UNAVAILABLE")

    # --- stash artifacts for manual diffing ---
    out_dir = os.path.join(work_dir, rel.replace(os.sep, "__"))
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "internal.norm"), "w", encoding="utf-8") as fh:
        fh.write(cmp_int)
    with open(os.path.join(out_dir, "oss.norm"), "w", encoding="utf-8") as fh:
        fh.write(cmp_oss)

    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("internal_root")
    ap.add_argument("oss_root")
    ap.add_argument("--cache", default="/tmp/cpp_classify_cache.json")
    ap.add_argument("--work-dir", default="/tmp/cpp-classify-work")
    args = ap.parse_args()

    internal_root = os.path.abspath(args.internal_root)
    oss_root = os.path.abspath(args.oss_root)

    files_int = set(enumerate_files(internal_root))
    files_oss = set(enumerate_files(oss_root))
    shared = sorted(files_int & files_oss)

    cache: dict = {}
    if os.path.exists(args.cache):
        try:
            with open(args.cache, "r", encoding="utf-8") as fh:
                cache = json.load(fh)
        except (json.JSONDecodeError, OSError):
            cache = {}

    results = []
    for rel in shared:
        internal_path = os.path.join(internal_root, SUBDIR, rel)
        oss_path = os.path.join(oss_root, SUBDIR, rel)
        raw_int = read_text(internal_path)
        raw_oss = read_text(oss_path)
        key = hashlib.sha256(
            (SCRIPT_VERSION + "\0" + rel + "\0").encode()
            + hashlib.sha256(raw_int.encode()).digest()
            + hashlib.sha256(raw_oss.encode()).digest()
        ).hexdigest()

        if key in cache:
            results.append(cache[key])
            continue

        res = classify(internal_path, oss_path, internal_root, oss_root,
                       args.work_dir, rel)
        cache[key] = res
        results.append(res)

    with open(args.cache, "w", encoding="utf-8") as fh:
        json.dump(cache, fh)

    for res in results:
        notes = ",".join(res.get("notes", []))
        print(f"{res['status']}\t{res['path']}\t{notes}")

    # Report non-shared files on stderr so the TSV stays clean.
    only_int = sorted(files_int - files_oss)
    only_oss = sorted(files_oss - files_int)
    if only_int:
        print("# only in internal tree: " + ", ".join(only_int), file=sys.stderr)
    if only_oss:
        print("# only in open-source tree: " + ", ".join(only_oss), file=sys.stderr)


if __name__ == "__main__":
    main()
