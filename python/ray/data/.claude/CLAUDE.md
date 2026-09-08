<!-- Loaded on-demand when Claude works on Ray Data files. -->
<!-- Keep under 50 lines. Multi-step procedures → skills. Code style → rules/. -->

# Ray Data

## Key Modules
<!-- Entry points, important abstractions, non-obvious dependencies -->

## Gotchas
<!-- Non-obvious behaviors, common mistakes, things that break silently -->

## Project docs (local-only, gitignored)

Long-running investigations keep their notes in a directory under `python/ray/data/`
excluded via `.git/info/exclude` — currently `arrow_rs_docs/` (the arrow-rs Parquet
reader migration, draft PR #65117). **Start at that directory's `README.md`**: it holds
the read order, the doc index with a staleness column, the ratio conventions, and the
rules below. Do not read the other docs front to back; arrive at them from a link.

## Doc rules for those directories

Each rule exists because its absence already cost something real.

1. **No markdown file over 1000 lines.** The moment one crosses it, move a whole section
   out into an existing doc if one owns the topic, else a new file  leaving a stub that
   says where it went and keeping section numbers unchanged so old cross-references still
   resolve. Do it in the same edit that crosses the line. Two docs reached 1910 lines and
   became unreadable and un-editable at once; splitting after the fact cost far more.
2. **A measured number is a row in `findings.md`** — one tabular registry with permanent
   IDs, a status column (LIVE / RE-MEASURE / DEAD / RETRACTED / OPEN) and a caveat column,
   so a new confound is recorded by editing one row. Never renumber, never delete a row: a
   wrong finding becomes RETRACTED with the reason, because the wrong version is what
   older docs still say. The prose that qualifies a number goes in the topic doc, never in
   the plan — a plan that quotes numbers goes stale silently.
3. **Closing a work item splits it three ways, in the same edit:** numbers → `findings.md`,
   prose → the topic doc, and the *decision* (one row: disposition, why, and for parked
   items what would revive it) → `todo_archive.md` under its original number, which is
   never reused. It leaves `TODO.md` entirely, which holds open work only. **Before
   archiving, check the closed item for open work hiding inside it** and promote that —
   this has now bitten four times, including a P0 that had already shipped and a stale
   docstring nobody could see.
4. **Verify against the tree before writing a doc claim.** Env vars, defaults, file paths
   and which branch a script lives on all drift; each audit of these docs has found
   documented knobs that do not exist and shipped defaults documented three values stale.
5. After every message, check if there is anything to be updated/added/removed in any doc 
   and make the apt change. 
