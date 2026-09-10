#!/usr/bin/env python3
r"""Soft-wrap (unwrap) prose in Markdown / MyST files.

Joins each hard-wrapped paragraph and list item into a single logical line so the
editor and renderer handle line wrapping. The change is whitespace-only: it only
ever collapses the newlines *inside* a paragraph or list item to single spaces.

Left byte-for-byte unchanged:
  - YAML front matter (leading --- ... --- block)
  - fenced code blocks, including nested fences, fenced directives
    (```{list-table}, ```{eval-rst}, ```{toctree}, ```{code-cell}, ...), and a
    fence that opens on the same line as a list marker (- ```shell)
  - $$ ... $$ display math and \begin{env} ... \end{env} (amsmath) blocks
  - GFM pipe tables: a row containing a pipe followed by a |---|:--:| delimiter
    row, then the body rows -- every table row keeps its own line
  - colon-fence directive markers/options (:::{note}, :open:, ...) -- the prose
    *inside* a colon directive is still reflowed, only the markers stay put
  - MyST (target)= anchors, ATX headings, thematic breaks, block quotes
  - CommonMark indented code blocks: wherever no paragraph is open, a line
    indented four or more spaces (or a tab) opens a block that runs verbatim to
    the next non-blank line indented less than four
  - raw HTML lines at any indentation, and every line of a multi-line HTML
    comment
  - link reference definitions ([label]: url) and definition-list items (: def)
  - any paragraph containing a hard line break (trailing two spaces or "\")

Single-line runs are emitted unchanged, so the diff contains ONLY genuine joins.

Usage:
    softwrap.py PATH [PATH ...]      # reflow files / dirs / globs in place
    softwrap.py --check PATH ...     # report what would change; exit 1 if any
    softwrap.py --quiet PATH ...     # only print files that changed

A dir argument is searched recursively for *.md and *.markdown.
"""
import argparse
import glob
import os
import re
import sys

H_RE = re.compile(r"^\s{0,3}#{1,6}(\s|$)")
TARGET_RE = re.compile(r"^\(([\w.\-]+)\)=\s*$")
COLON_RE = re.compile(r"^\s*:{3,}")
FENCE_OPEN_RE = re.compile(r"^(\s*)(`{3,}|~{3,})(.*)$")
FENCE_CLOSE_RE = re.compile(r"^(\s*)(`{3,}|~{3,})\s*$")
TBREAK_RE = re.compile(r"^\s{0,3}([-*_])([ \t]*\1){2,}[ \t]*$")
OPT_RE = re.compile(r"^\s*:[A-Za-z0-9_][A-Za-z0-9_+-]*:(\s.*)?$")
BQ_RE = re.compile(r"^\s{0,3}>")
MARKER_RE = re.compile(r"^(\s*)([-*+]|\d+[.)])\s")
BLANK_RE = re.compile(r"^\s*$")
# Raw HTML lines, at any indentation. CommonMark only opens an HTML block within
# 3 leading spaces, but indentation is not a reliable signal here: a raw HTML
# block nested in a directive body carries the body's indentation, and some Ray
# directives (query-param-ref) re-parse their content with docutils, where an
# indented ``.. raw:: html`` block is raw HTML rather than an indented code
# block. Joining those lines isn't this pass's business either way, so treat a
# raw HTML line as a boundary wherever it sits.
HTML_RE = re.compile(r"^\s*<(/?[A-Za-z][\w-]*|!--)")
HTML_COMMENT_OPEN_RE = re.compile(r"^\s*<!--")
HTML_COMMENT_CLOSE_RE = re.compile(r"-->")
MATH_FENCE_RE = re.compile(r"^\s*\$\$\s*$")
DOLLAR_SPAN_RE = re.compile(r"\$\$")
AMS_BEGIN_RE = re.compile(r"^\s*\\begin\{[A-Za-z*]+\}")
AMS_END_RE = re.compile(r"^\s*\\end\{[A-Za-z*]+\}")
DEFLIST_RE = re.compile(r"^\s{0,3}:\s")
LINKDEF_RE = re.compile(r"^\s{0,3}\[[^\]]+\]:\s")
MARKER_FENCE_RE = re.compile(r"^(\s*([-*+]|\d+[.)])\s+)(`{3,}|~{3,})")
# sphinx-design card separators: ^^^ ends the card header, +++ starts the footer.
# sphinx-design matches each with an anchored ``^\^{3,}\s*$`` / ``^\+{3,}\s*$``,
# so the marker only works on a line of its own. Joining it into the surrounding
# prose silently drops the card header -- and the CommonMark render oracle can't
# see it, because sphinx-design directives aren't part of the oracle's grammar.
CARD_SEP_RE = re.compile(r"^\s*(\^{3,}|\+{3,})\s*$")
HARDBREAK_RE = re.compile(r"(\S  +|\\)$")
# CommonMark indented code block: four spaces (or a tab) opens a code block
# anywhere a paragraph isn't already open, and it runs until the next non-blank
# line indented less than four. Joining those lines leaves a block that still
# renders as code and still holds every non-whitespace byte, but whose contents
# no longer run --
# ``pip install ray`` and ``ray start --head`` become one command. No check in
# verify.py can see it: the content invariant and the CommonMark render oracle
# both collapse whitespace without exempting ``<pre>``, and the joined form is a
# stable fixed point, so idempotency holds too. Same for the render diff in the
# rst-to-myst skill, which normalizes the serialized ``<article>`` the same way.
# The cost is that anything else sitting four spaces deep with no paragraph open
# -- a nested list, a list-item continuation paragraph -- is left wrapped
# instead of joined. That's the under-reflow direction, which is the safe one.
INDENT_CODE_RE = re.compile(r"^(?: {4,}|\t)")

MARKDOWN_EXTS = (".md", ".markdown")


def has_hard_break(run):
    """True if any line in the run ends with a Markdown hard line break."""
    return any(HARDBREAK_RE.search(line) for line in run)


def is_pipe_delim(line):
    """True if line is a GFM table delimiter row, e.g. ``|---|:--:|---|``.

    A delimiter row contains only pipes, hyphens, colons, and whitespace, with
    at least one hyphen and at least one pipe. Its presence on the line right
    after a row that contains a pipe is what marks a GFM pipe table -- the same
    signal the CommonMark/GFM table parser uses.
    """
    s = line.strip()
    return bool(s) and all(c in "|-: \t" for c in s) and "-" in s and "|" in s


def join_run(run):
    """Collapse a run of wrapped prose lines into one line.

    A single-line run, or any run containing a hard line break, is returned
    verbatim so rendering is preserved exactly.
    """
    if len(run) == 1 or has_hard_break(run):
        return list(run)
    first = run[0]
    lead = first[: len(first) - len(first.lstrip())]
    parts = [p.strip() for p in run]
    parts = [p for p in parts if p]
    return [lead + " ".join(parts)]


def reflow(text):
    """Return (new_text, joins) where joins is the number of multi-line runs joined."""
    lines = text.split("\n")
    out = []
    run = []
    joins = 0

    def flush():
        nonlocal joins
        if run:
            joined = join_run(run)
            if len(joined) < len(run):
                joins += 1
            out.extend(joined)
            run.clear()

    n = len(lines)
    i = 0
    # Front matter: only when the very first line is a --- fence.
    if n > 0 and lines[0].strip() == "---":
        out.append(lines[0])
        i = 1
        while i < n:
            out.append(lines[i])
            closed = lines[i].strip() == "---"
            i += 1
            if closed:
                break

    in_fence = False
    fence_char = ""
    fence_len = 0
    in_math = False  # $$ ... $$ display-math block
    in_amsmath = False  # \begin{env} ... \end{env}
    in_html_comment = False  # <!-- ... --> spanning lines
    while i < n:
        ln = lines[i]
        if in_html_comment:
            out.append(ln)
            if HTML_COMMENT_CLOSE_RE.search(ln):
                in_html_comment = False
            i += 1
            continue
        if in_fence:
            out.append(ln)
            m = FENCE_CLOSE_RE.match(ln)
            if m and m.group(2)[0] == fence_char and len(m.group(2)) >= fence_len:
                in_fence = False
            i += 1
            continue
        if in_math:
            out.append(ln)
            if MATH_FENCE_RE.match(ln):
                in_math = False
            i += 1
            continue
        if in_amsmath:
            out.append(ln)
            if AMS_END_RE.match(ln):
                in_amsmath = False
            i += 1
            continue
        # An indented code block, recognized the way CommonMark recognizes one:
        # the only thing it can't interrupt is a paragraph. A blank line isn't
        # required -- indented code opens immediately after a heading, a closing
        # fence, or a thematic break -- so the condition is an empty ``run``,
        # which is precisely "no paragraph is open right now". Checked ahead of
        # the fence rule because four spaces beat a fence marker in CommonMark
        # too.
        if INDENT_CODE_RE.match(ln) and not run:
            flush()
            while i < n and (
                BLANK_RE.match(lines[i]) or INDENT_CODE_RE.match(lines[i])
            ):
                out.append(lines[i])
                i += 1
            continue
        mo = FENCE_OPEN_RE.match(ln)
        if mo:
            flush()
            fence_char = mo.group(2)[0]
            fence_len = len(mo.group(2))
            in_fence = True
            out.append(ln)
            i += 1
            continue
        if MATH_FENCE_RE.match(ln):  # lone $$ opens a display-math block
            flush()
            in_math = True
            out.append(ln)
            i += 1
            continue
        # An HTML comment that opens without closing on the same line runs
        # verbatim to its --> so a comment's interior lines stay as the author
        # wrote them.
        if HTML_COMMENT_OPEN_RE.match(ln) and not HTML_COMMENT_CLOSE_RE.search(ln):
            flush()
            in_html_comment = True
            out.append(ln)
            i += 1
            continue
        if CARD_SEP_RE.match(ln):  # sphinx-design ^^^ / +++ card separator
            flush()
            out.append(ln)
            i += 1
            continue
        if AMS_BEGIN_RE.match(ln):  # \begin{equation} ... \end{equation}
            flush()
            in_amsmath = True
            out.append(ln)
            i += 1
            continue
        # GFM pipe table: a row containing a pipe immediately followed by a
        # delimiter row. Emit header, delimiter, and the body rows verbatim so
        # every row keeps its own line -- joining rows would destroy the table.
        if "|" in ln and i + 1 < n and is_pipe_delim(lines[i + 1]):
            flush()
            out.append(ln)  # header row
            out.append(lines[i + 1])  # delimiter row
            i += 2
            while i < n and lines[i].strip() and "|" in lines[i]:
                out.append(lines[i])  # body row
                i += 1
            continue
        if (
            BLANK_RE.match(ln)
            or BQ_RE.match(ln)
            or H_RE.match(ln)
            or HTML_RE.match(ln)
            or TARGET_RE.match(ln)
            or COLON_RE.match(ln)
            or TBREAK_RE.match(ln)
            or OPT_RE.match(ln)
            or DEFLIST_RE.match(ln)
            or LINKDEF_RE.match(ln)
            or DOLLAR_SPAN_RE.search(ln)
        ):
            flush()
            out.append(ln)
            i += 1
            continue
        mf = MARKER_FENCE_RE.match(ln)
        if mf:  # a fenced code block opens on the same line as a list marker
            flush()
            fence_char = mf.group(3)[0]
            fence_len = len(mf.group(3))
            in_fence = True
            out.append(ln)
            i += 1
            continue
        if MARKER_RE.match(ln):
            flush()
            run.append(ln)
            i += 1
            continue
        run.append(ln)
        i += 1
    flush()
    return "\n".join(out), joins


def collect(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            for root, _, names in os.walk(p):
                for nm in sorted(names):
                    if nm.endswith(MARKDOWN_EXTS):
                        files.append(os.path.join(root, nm))
        elif os.path.isfile(p):
            if p.endswith(MARKDOWN_EXTS):
                files.append(p)
        else:
            files.extend(g for g in sorted(glob.glob(p)) if g.endswith(MARKDOWN_EXTS))
    # de-dupe, keep order
    seen = set()
    uniq = []
    for f in files:
        rp = os.path.realpath(f)
        if rp not in seen:
            seen.add(rp)
            uniq.append(f)
    return uniq


# Each case is (name, input, expected output). Cases assert the boundaries that the
# CommonMark render oracle in verify.py cannot see, so a regression here would
# otherwise pass all three of that script's checks.
SELFTEST_CASES = [
    (
        "plain prose joins",
        "One line\nand another.\n",
        "One line and another.\n",
    ),
    (
        "sphinx-design card separators stay on their own line",
        ":::{grid-item-card}\n**Title**\n^^^\nBody text\nwrapped here.\n\n+++\nFooter.\n:::\n",
        ":::{grid-item-card}\n**Title**\n^^^\nBody text wrapped here.\n\n+++\nFooter.\n:::\n",
    ),
    (
        "RST simple table inside eval-rst is untouched",
        "```{eval-rst}\n====== ======\nCol A  Col B\n====== ======\na      b\n====== ======\n```\n",
        "```{eval-rst}\n====== ======\nCol A  Col B\n====== ======\na      b\n====== ======\n```\n",
    ),
    (
        "hard break suppresses the join",
        "Line one  \nline two.\n",
        "Line one  \nline two.\n",
    ),
    (
        "pipe table rows keep their lines",
        "| a | b |\n|---|---|\n| 1 | 2 |\n",
        "| a | b |\n|---|---|\n| 1 | 2 |\n",
    ),
    (
        "indented raw HTML inside a directive body keeps its lines",
        ":::{query-param-ref} ray-overview/examples\n:parameters: ?tags=llm\n\n.. raw:: html\n\n        <svg width='24' height='24'>\n            <g>\n                <path d='M15 9Z'> </path>\n            </g>\n        </svg>Explore the examples\n:::\n",
        ":::{query-param-ref} ray-overview/examples\n:parameters: ?tags=llm\n\n.. raw:: html\n\n        <svg width='24' height='24'>\n            <g>\n                <path d='M15 9Z'> </path>\n            </g>\n        </svg>Explore the examples\n:::\n",
    ),
    (
        "an indented code block keeps its lines",
        "Install it:\n\n    pip install ray\n    ray start --head\n\nThen open\nthe dashboard.\n",
        "Install it:\n\n    pip install ray\n    ray start --head\n\nThen open the dashboard.\n",
    ),
    (
        "an indented line can't open a code block mid-paragraph",
        "Prose that\n    keeps going.\n",
        "Prose that keeps going.\n",
    ),
    (
        "an indented code block opens with no blank line after a heading",
        "## Install\n    pip install ray\n    ray start --head\n",
        "## Install\n    pip install ray\n    ray start --head\n",
    ),
    (
        "an indented code block opens with no blank line after a closing fence",
        "```python\nx = 1\n```\n    pip install ray\n    ray start --head\n",
        "```python\nx = 1\n```\n    pip install ray\n    ray start --head\n",
    ),
    (
        "a multi-line HTML comment keeps its lines",
        "<!-- DJS: this note\nspans two lines. -->\n\nProse that\njoins.\n",
        "<!-- DJS: this note\nspans two lines. -->\n\nProse that joins.\n",
    ),
]


def selftest():
    """Run the built-in regression cases. Returns a process exit code."""
    failures = 0
    for name, src, want in SELFTEST_CASES:
        got, _ = reflow(src)
        if got != want:
            failures += 1
            print(f"FAIL  {name}\n  want: {want!r}\n  got:  {got!r}")
        else:
            print(f"ok    {name}")
        # Idempotency: a second pass must be a no-op.
        again, _ = reflow(got)
        if again != got:
            failures += 1
            print(f"FAIL  {name} (not idempotent)\n  second pass: {again!r}")
    print(f"\n{len(SELFTEST_CASES)} case(s), {failures} failure(s).")
    return 1 if failures else 0


def main():
    ap = argparse.ArgumentParser(description="Soft-wrap prose in Markdown/MyST files.")
    ap.add_argument("paths", nargs="*", help="files, directories, or globs")
    ap.add_argument(
        "--selftest", action="store_true", help="run built-in regression cases and exit"
    )
    ap.add_argument(
        "--check",
        action="store_true",
        help="don't write; exit 1 if any file would change",
    )
    ap.add_argument("--quiet", action="store_true", help="only list changed files")
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if not args.paths:
        ap.error("provide at least one path, or --selftest")

    files = collect(args.paths)
    if not files:
        print("no markdown files matched", file=sys.stderr)
        return 2

    any_change = False
    bad = 0
    for path in files:
        original = open(path, encoding="utf-8").read()
        new, joins = reflow(original)
        # Hard safety gate: non-whitespace content must be byte-identical.
        invariant_ok = re.sub(r"\s+", "", original) == re.sub(r"\s+", "", new)
        changed = new != original
        any_change = any_change or changed
        if not invariant_ok:
            bad += 1
        if not args.check and changed and invariant_ok:
            open(path, "w", encoding="utf-8").write(new)
        if args.quiet:
            if changed:
                print(path)
        else:
            if changed:
                verb = "would change" if args.check else "changed"
            else:
                verb = "unchanged"
            flag = "" if invariant_ok else "  !! CONTENT-CHANGED, NOT WRITTEN !!"
            print(f"{verb:<13} joins={joins:<4} {path}{flag}")

    if bad:
        print(
            f"\n{bad} file(s) failed the content invariant; refusing to trust output.",
            file=sys.stderr,
        )
        return 3
    if args.check and any_change:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
