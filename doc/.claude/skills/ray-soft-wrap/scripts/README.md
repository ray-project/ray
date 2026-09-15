# `ray-soft-wrap` scripts

Deterministic helpers for the `ray-soft-wrap` skill. Both are plain Python 3, take
file/dir/glob arguments, and have no hardcoded paths. A directory argument is
searched recursively for `*.md` and `*.markdown`.

| Script | Purpose | How to run |
|--------|---------|------------|
| `softwrap.py` | Reflow (unwrap) hard-wrapped prose so each paragraph and list item is one line. Whitespace-only by construction; refuses to write any file whose non-whitespace content would change. | `python3 softwrap.py PATH [PATH ...]` to write in place; `--check` to dry-run (exit 1 if any file would change, 3 on an invariant failure); `--quiet` to list only changed files. |
| `verify.py` | Confirm a soft-wrap pass changed nothing but whitespace, on three axes: content invariant, CommonMark render-equality, and idempotency. | `python3 verify.py PATH ...` compares the working tree against git `HEAD`; `--ref SHA` against another ref; `--against-dir DIR` against reference files matched by basename (for flat test corpora). Exit non-zero on any failure, and on any file it couldn't fully check (`NOT VERIFIED`). |

## What `softwrap.py` leaves untouched

Front matter; fenced code blocks including nested fences and fenced directives
(` ```{list-table} `, ` ```{eval-rst} `, ` ```{toctree} `, ` ```{code-cell} `);
`$$ ... $$` math and `\begin{env} ... \end{env}` amsmath blocks; colon-fence
directive markers and options (`:::{note}`, `:open:`); sphinx-design `^^^` and
`+++` card separators; MyST `(target)=` anchors;
ATX headings; thematic breaks; block quotes; CommonMark indented code blocks (four
spaces or a tab wherever no paragraph is open, to the next non-blank line indented
less than four); raw HTML at any indentation, including
a `.. raw:: html` block nested in a directive body, and every line of a multi-line
HTML comment; link
reference definitions (`[label]: url`); definition-list items; and any paragraph
containing a Markdown hard line break (trailing `  ` or `\`). Prose *inside* a
colon directive is still reflowed — only the markers stay put.

## Dependencies

- `softwrap.py`: standard library only.
- `verify.py`: `markdown-it-py` for the render-equality check
  (`pip install markdown-it-py`). Required — without it the render check can't run,
  so every file reports `NOT VERIFIED` and the script exits non-zero.

## Typical use

```bash
# from inside a Ray worktree, on a clean branch:
python3 softwrap.py doc/source/ray-contribute
python3 verify.py   doc/source/ray-contribute   # compares against HEAD
```

`verify.py` is the correctness gate: it catches structural mis-joins (merged
paragraphs, list items, headings, lost hard breaks) that the content invariant
alone cannot. Any file it flags should be restored (`git checkout -- FILE`) and
investigated rather than committed.
