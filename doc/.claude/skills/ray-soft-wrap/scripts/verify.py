#!/usr/bin/env python3
"""Verify a soft-wrap pass changed nothing but whitespace.

For each target file, compares the current (transformed) content against a
reference -- either a git ref (the pre-transform state) or a reference
directory -- on three deterministic axes:

  1. Content invariant   non-whitespace bytes identical (catches any lost,
                         added, or reordered content).
  2. Render equality     CommonMark + GFM-table rendered HTML identical after
                         whitespace normalization (catches structural mis-joins:
                         merged paragraphs, list items, headings, lost hard
                         breaks, collapsed pipe tables). Requires markdown-it-py.
  3. Idempotency         re-running the reflow is a no-op (the file is at a
                         stable fixed point and won't churn under later edits).

A check that can't run is not a check that passed. A file with no reference
(new or untracked) or a render check with no markdown-it-py reports NOT VERIFIED
and exits non-zero, the same way a failure does -- a gate that reports success
for work it never inspected is worse than no gate.

Exit code is non-zero if any file fails a check or if any check couldn't run.

Usage:
    verify.py PATH [PATH ...]                  # compare working tree vs HEAD
    verify.py --ref ORIG_SHA PATH ...          # compare vs an explicit git ref
    verify.py --against-dir DIR PATH ...        # compare vs DIR (match basename)

A dir PATH is searched recursively for *.md / *.markdown.
"""
import argparse
import os
import re
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from softwrap import reflow, collect  # noqa: E402

try:
    from markdown_it import MarkdownIt

    # CommonMark plus the GFM table rule. Plain CommonMark has no table
    # extension, so it renders a pipe table and a one-line collapse of that
    # table to identical text and is blind to a destroyed table. Enabling
    # "table" lets render-equality catch a collapsed table (a <table> on one
    # side becomes a <p> on the other).
    _MD = MarkdownIt("commonmark").enable("table")
except Exception:
    _MD = None


def norm(html):
    return re.sub(r"\s+", " ", html).strip()


def render_equal(ref_text, work_text):
    if _MD is None:
        return None  # check unavailable
    return norm(_MD.render(ref_text)) == norm(_MD.render(work_text))


def nows(s):
    return re.sub(r"\s+", "", s)


def get_reference(path, ref, against_dir):
    if against_dir:
        cand = os.path.join(against_dir, os.path.basename(path))
        if not os.path.isfile(cand):
            return None, f"no reference file {cand}"
        return open(cand, encoding="utf-8").read(), None
    d = os.path.dirname(os.path.abspath(path))
    base = os.path.basename(path)
    try:
        out = subprocess.run(
            ["git", "-C", d, "show", f"{ref}:./{base}"],
            capture_output=True,
            text=True,
            check=True,
        )
        return out.stdout, None
    except subprocess.CalledProcessError:
        return None, f"not in git ref {ref} (new file?)"


def main():
    ap = argparse.ArgumentParser(
        description="Verify a soft-wrap pass is whitespace-only."
    )
    ap.add_argument("paths", nargs="+")
    ap.add_argument(
        "--ref",
        default="HEAD",
        help="git ref for the pre-transform state (default HEAD)",
    )
    ap.add_argument(
        "--against-dir",
        help="compare against reference files in this dir (by basename)",
    )
    args = ap.parse_args()

    if _MD is None:
        print(
            "ERROR: markdown-it-py not installed; the render-equality check can't\n"
            "       run, so no file can be reported as verified.\n"
            "       Install with: pip install markdown-it-py\n",
            file=sys.stderr,
        )

    files = collect(args.paths)
    if not files:
        print("no markdown files matched", file=sys.stderr)
        return 2

    fails = 0
    unverified = 0
    for path in files:
        work = open(path, encoding="utf-8").read()
        ref_text, err = get_reference(path, args.ref, args.against_dir)

        idem_ok = reflow(work)[0] == work
        if ref_text is None:
            content_ok = None
            render_ok = None
        else:
            content_ok = nows(ref_text) == nows(work)
            render_ok = render_equal(ref_text, work)

        def mark(v):
            return {True: "ok", False: "FAIL", None: "n/v"}[v]

        if content_ok is False or render_ok is False or not idem_ok:
            fails += 1
            tag = "   <-- FAIL"
        elif content_ok is None or render_ok is None:
            unverified += 1
            reason = err or "markdown-it-py not installed"
            tag = f"   <-- NOT VERIFIED ({reason})"
        else:
            tag = ""
        print(
            f"  content={mark(content_ok):<4} render={mark(render_ok):<4} "
            f"idempotent={mark(idem_ok):<4}  {path}{tag}"
        )

    print()
    if fails or unverified:
        parts = []
        if fails:
            parts.append(f"{fails} file(s) FAILED verification")
        if unverified:
            parts.append(f"{unverified} file(s) NOT VERIFIED")
        print("RESULT: " + ", ".join(parts) + ".")
        return 1
    print(f"RESULT: all {len(files)} file(s) verified.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
