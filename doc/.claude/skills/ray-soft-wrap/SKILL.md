---
name: ray-soft-wrap
description: Soft-wrap (unwrap) hard-wrapped prose in Markdown/MyST files in the ray-project/ray repo so each paragraph and list item is one line, with deterministic content-invariant, render-equality, and idempotency checks. Use when asked to soft-wrap, unwrap, or reflow Ray docs (.md/.markdown), or to normalize line wrapping before style and grammar edits. Markdown only — rST files are converted to MyST first by a separate effort.
---

# Ray soft-wrap

Convert hard-wrapped prose in Ray Markdown/MyST docs to one line per paragraph and
list item, leaving line wrapping to the editor and renderer. The change is
**whitespace-only**: it only collapses the newlines inside a paragraph or list item
to single spaces. It never edits words, and it leaves code, directives, tables, math,
and front matter untouched.

The heavy lifting is two deterministic Python scripts (no agents, no tokens). This
skill is the workflow around them: pick scope, run the transform, run the checks,
review, and ship a PR under Ray's OSS conventions.

## When to invoke

- "Soft-wrap / unwrap / reflow the Ray `<area>` docs."
- "Normalize line wrapping in these `.md` files before I do style edits."
- Prepping a directory so a later grammar/style pass produces clean, reviewable diffs.

## Scope

- **Markdown only** (`.md`, `.markdown`). rST is out of scope here — the Ray docs
  effort converts rST to MyST first, then this skill applies.
- The whole `doc/source` tree is hard-wrapped (~200+ files). **Don't do it in one
  PR.** Batch by area (one PR per top-level subtree, e.g. `doc/source/serve`,
  `doc/source/cluster`) so reviews stay manageable. Confirm the batch with the user.

## Prerequisites

- `markdown-it-py` for the render-equality check: `pip install markdown-it-py`. If
  absent, `verify.py` skips that check and warns — install it; the render check is
  the one that catches structural mistakes.
- The scripts live beside this file, at `doc/.claude/skills/ray-soft-wrap/scripts/`.
  Run them from the repo root.

## Workflow

1. **Pick the batch** with the user (a directory or explicit file list).

1. **Work on a branch off current `master`:**
   ```bash
   git fetch origin --tags -q
   git switch -c soft-wrap-<area> origin/master
   ```

1. **Run the transform** on the batch (writes in place):
   ```bash
   python3 <skill>/scripts/softwrap.py doc/source/<area>
   ```
   `softwrap.py` self-checks the content invariant on every file and refuses to write
   any file whose non-whitespace content would change. A `--check` dry run lists what
   would change without writing.

1. **Verify** against the pre-transform state (this is the gate):
   ```bash
   python3 <skill>/scripts/verify.py doc/source/<area>   # compares working tree vs HEAD
   ```
   Every file must report `content=ok render=ok idempotent=ok`. If any file FAILs,
   restore just that file (`git checkout -- <file>`), note the construct, and leave it
   for manual handling — never commit a file that fails verification. (A failure means
   the transform mis-joined something the engine doesn't yet model; capture it and
   consider `/skill-improve`.)

1. **Spot-review the diff** (`git diff`). The diff should be only line joins. Skim a
   couple of files, especially around admonitions, lists, and tables.

1. **Commit** under the repo's contribution conventions (see the root `AGENTS.md`):
   - `[doc]` subject prefix; describe it as a whitespace-only soft-wrap.
   - **DCO sign-off is required**: `git commit --signoff`.

1. **Push to your fork and open the PR** (never push to `upstream`):
   ```bash
   git push -u origin soft-wrap-<area>
   gh pr create --repo ray-project/ray --base master \
     --head <fork-user>:soft-wrap-<area> \
     --title "[doc] Soft-wrap prose in <area>" \
     --body-file <scratch>/pr-body.md
   ```
   Write the PR body to a gitignored path so it can't be accidentally committed, and
   pass it with `--body-file` to avoid heredoc-in-`$()` breakage on bodies containing
   backticks.

## Changing the engine

`softwrap.py` carries built-in regression cases covering the boundaries `verify.py`'s
oracle can't see. Run them after any edit to the engine:

```bash
python3 doc/.claude/skills/ray-soft-wrap/scripts/softwrap.py --selftest
```

Each case asserts both the expected output and idempotency. Add one whenever you
teach the engine a new verbatim boundary.

## Why this is safe

`softwrap.py` only ever deletes newlines inside a paragraph or list item. `verify.py`
proves the pass was whitespace-only on three independent axes:

- **Content invariant** — non-whitespace bytes are byte-for-byte identical. Catches
  any lost, added, or reordered text.
- **Render-equality** — CommonMark + GFM-table rendered HTML is identical after
  whitespace normalization. Catches structural mis-joins (merged paragraphs, merged
  list items, a heading folded into prose, a lost hard break, a collapsed pipe table)
  that the content invariant can't.
- **Idempotency** — re-running the transform is a no-op, so wrapped files sit at a
  stable fixed point and won't churn under later edits.

The full engine was validated across every `.md` file in Ray's `doc/source`
(content invariant and render-equality held on all of them).

## What's preserved (and what isn't)

Left byte-for-byte unchanged by construction: front matter; fenced code blocks
(including nested fences and fenced directives like `{list-table}`, `{eval-rst}`,
`{toctree}`, `{code-cell}`); `$$ … $$` and `\begin{…} … \end{…}` math; GFM pipe
tables (a header row, its `|---|` delimiter row, and the body rows, each kept on its
own line); colon-fence directive markers and options; sphinx-design `^^^` and `+++`
card separators; MyST `(target)=` anchors; ATX headings; thematic breaks; block
quotes; raw HTML at any indentation, including a `.. raw:: html` block nested in a
directive body, and every line of a multi-line HTML comment; link reference
definitions; and definition-list items.

The render-equality oracle is CommonMark plus the GFM table extension, so it sees
paragraphs, lists, headings, block quotes, code, links — and pipe tables. It still
does not "see" the remaining MyST-only constructs (directives, roles, dollar-math);
those are protected by construction instead, not by the oracle — which is why the
engine treats them as verbatim boundaries.

**Take that limitation literally: `render=ok` is not evidence for anything the
oracle can't parse.** The `^^^` protection above exists because it was missing.
Ray's first native MyST card grids landed in the RST-to-MyST conversion, and on
those pages the engine joined `**Title**` / `^^^` / body into a single line. That
silently destroys the card header, because sphinx-design only matches `^^^` on a
line of its own — and all three checks still reported `content=ok render=ok
idempotent=ok`, since the non-whitespace bytes were intact and CommonMark has no
concept of a card. When you soft-wrap a page using a MyST-only construct the oracle
doesn't model, add a structural spot-check of your own; for card grids, assert that
the `{grid-item-card}`, `^^^`, and `+++` counts still match.

The indented-raw-HTML protection has the same provenance. `use-cases.md` carries an
SVG icon inside a `.. raw:: html` block nested in a `{query-param-ref}` body, whose
content Sphinx re-parses with docutils. Indented 8 spaces, it fell outside
CommonMark's 3-space window for opening an HTML block, so the engine collapsed the
whole icon onto one 3,000-character line and reported `content=ok render=ok
idempotent=ok` — the render oracle treats an HTML block as opaque passthrough and
normalizes whitespace inside it. Nothing broke visually that time. It's still not
this pass's business to rewrite raw HTML, so read a long joined line in the diff as
a signal to check what the engine thought it was reflowing.

**Known under-reflow (safe):** prose inside `$$`/amsmath/deflist blocks and inside
backtick-fenced directives (e.g. a colon-less `{note}`) is left wrapped, and any
paragraph containing a hard line break is left wrapped. These are deliberate: they
preserve rendering exactly. If a batch needs those reflowed, do it by hand and
re-run `verify.py`.
