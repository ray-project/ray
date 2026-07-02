---
name: sphinx-fix
description: Diagnose a failing Ray Sphinx / Read the Docs documentation build. Parses the Sphinx warning stream (an RtD build log, a local build, or pasted text), classifies each warning against a rules table, and proposes the canonical fix in severity-tier order. Detects a hard-broken build, segregates known-benign suppressed classes, and lists every unclassified warning. Use when a `docs/readthedocs.com:anyscale-ray` check fails, when asked "why is the docs build failing?" or "what warning is breaking this PR?", or to turn a Sphinx warning dump into an ordered fix list.
user-invocable: true
argument-hint: <warnings file | pasted Sphinx log | '-' for stdin>
---

# Diagnose and fix Ray Sphinx doc-build warnings

The Ray docs build with `fail_on_warning: true` (`.readthedocs.yaml`), so **any single Sphinx warning fails the build**. The expensive part of a docs change is usually not the edit — it's *finding* the warning. This skill turns a warning stream into a compact, severity-ordered list of findings, each with its canonical fix.

It is **deterministic and read-only**: a stdlib-only script parses and classifies; it never edits files. v0 is **human-in-the-loop** — you (or the agent) apply each fix and rebuild. Nothing is auto-applied.

Where it sits among the doc-build skills:

- **`sphinx-fix`** (this skill) — *diagnose build output*: classify warnings, propose fixes.
- **`rst-to-myst`** — *convert source*: migrate `.rst` pages to MyST `.md`. It shares one rules source with this skill (`rules.yaml` below).
- **`rtd_doctor.py`** (`doc/rtd_doctor.py`) — *preflight the environment*: assert the local toolchain matches Read the Docs before you build.

---

## When to use

- A `docs/readthedocs.com:anyscale-ray` PR check failed and you need to know why and how to fix it.
- You have a Sphinx warning dump (from RtD, a local build, or a paste) and want it classified.
- You're iterating on a docs PR and want to confirm the build is clean before pushing.

**Not for:** authoring or converting pages (use `rst-to-myst`), setting up the build environment (use `rtd_doctor.py` + `make rtd`), or fetching the rendered HTML (open the preview URL).

---

## Input modes

Both modes feed the same parser and rules table; only the log source differs. The parser tolerates surrounding log noise, so you can pipe a raw build log in without pre-filtering.

1. **A warning stream you already have** — paste it to a file, or pipe it. This includes the output of the team's Read the Docs log tooling (the private `/rtd-build-logs` skill's `warnings` subcommand), the RtD build page's raw log, or any captured Sphinx output. This is the cheapest mode and needs no local build.

2. **A local build** — capture stderr (and, with `-W -w warnings.txt`, a warnings file) from a full `make rtd` / `sphinx-build` run and feed it in. Use this for a tight local loop. A local build must be a **full** build for cross-reference checks to be faithful — an incremental `make local` pulls a cache and misses cross-file breakage (a rename that breaks a link in an *unchanged* file).

---

## Running the engine

```bash
# From a file (or '-'/omit for stdin)
python doc/.claude/skills/sphinx-fix/sphinx_fix.py warnings.txt

# Piped (e.g. a local build, or the team's RtD log tool)
python -m sphinx -T -W --keep-going -b html . _build/html 2>&1 \
  | python doc/.claude/skills/sphinx-fix/sphinx_fix.py

# JSON for programmatic use
python doc/.claude/skills/sphinx-fix/sphinx_fix.py warnings.txt --json

# Read one rule's cause/fix/validated-versions
python doc/.claude/skills/sphinx-fix/sphinx_fix.py --explain myst-xref-missing-cross-extension
```

Stdlib-only — no install. It prefers PyYAML to read `rules.yaml` but falls back to a bundled parser, so it runs in a bare environment. Optionally pass `--sphinx-version`/`--myst-version` to gate findings against each rule's validated range.

**Exit codes:** `0` clean · `1` classified findings to fix · `2` hard-broken build (abort) · `3` only unclassified warnings (no rule matched). Precedence `2 > 1 > 3 > 0`.

---

## Reading the output

The report has five parts, in priority order:

1. **Abort banner** (only if present) — a hard-broken build (a `conf.py`/extension error, a traceback, or `SEVERE:` with no completion summary) aborted before the warning pass. **Fix this first; the rest of the log is unreliable.** For an autosummary import abort, the banner adds a `hint:` — the named module is usually a decoy for an unrelated module whose import chain broke (often a dep mocked by `autodoc_mock_imports` but used at import time); trace it and make the eager import lazy.
2. **Root-cause groups** (only if present) — a single structural root (an ungenerated `autosummary` stub for a module or class) masks a flood of downstream `py:* reference target not found` warnings for the *same* objects. Rather than list the flood as N equal-looking rows, the report collapses it: `ROOT [T2] ... — <prefix>.* (N stubs not generated, masking M downstream references)`, the root's fix once, then the masked references collapsed under it (capped in the rendered view, with the dropped count shown). **Fix the root, rebuild, and the masked references clear together** — don't chase the individual references. The full flat list is always in `--json`.
3. **Findings**, grouped by tier (1 → 2 → 3), for everything not absorbed into a root-cause group. Each row is `[T2] <rule-id> <path:line>` plus the warning message, the canonical `fix:` (with a `Suggested:` rewrite for mechanical rules), and a `safety:` flag. `judgment` findings need your decision.
4. **Unclassified** — warnings no rule matched. **Never silently dropped.** Resolve each with the user, then file a skill-improvement ticket so `rules.yaml` gains a rule (see below).
5. **Suppressed** — known-benign classes the build already filters; not actionable.

---

## Severity tiers and the iterate loop

`-W --keep-going` lists every warning **of a build that completes**. Two things break the "one pass finds everything" assumption, so the skill triages by tier and you must iterate:

- **Tier 1 — fatal/abort.** The build died before the warning pass. `--keep-going` does not save you. Fix it, rebuild, re-run.
- **Tier 2 — structural/parse.** A broken `toctree`, a duplicate label, or a failed `literalinclude` corrupts the doc graph and **masks the warnings beneath it**. Fix all tier-2 findings, then rebuild — a fresh batch of tier-3 warnings often appears.
- **Tier 3 — plain warnings.** These mask nothing; fix them in any order.

**Always fix the highest tier first, then re-acquire and re-parse.** Never report "all clear" after a single pass when a tier-1 or tier-2 condition was present — the report's verdict line says so explicitly.

---

## Applying fixes (human-in-the-loop)

The engine proposes; you apply. For each finding:

- **Mechanical** fixes (link rewrites, fragment links, toctree entries, `literalinclude` ranges, image paths) — apply the `Suggested:` rewrite, confirming the target. For the MyST link rewrites, cross-check **`rst-to-myst` Hard rule 2** (the `{doc}`-role and `#fragment` rules are shared).
- **Judgment** fixes — *you decide*: which of two duplicate labels to keep (preserve the name external `{ref}`/`:ref:` callers use), or whether an orphan page should join a toctree or be marked `:orphan:`.

A rule graduates to auto-apply only once there is evidence it is always safe — not in v0.

---

## Unclassified warnings → grow the rules table

The seed table is incomplete by construction (the Sphinx 8→9 march and contributors new to MyST will surface classes no historical log shows). When the report lists an unclassified warning:

1. Resolve it with the user — determine the cause and the canonical fix.
2. Prompt the user to file a **skill-improvement ticket** capturing the new category/signature and its fix.
3. Add a rule to `rules.yaml` (and, if it's a link class, keep it consistent with `rst-to-myst`).

This feedback loop is the intended way the table grows — from real misses, not a one-time corpus.

---

## conf.py suppression awareness

The build already suppresses or filters known-benign classes in `doc/source/conf.py` (e.g. `misc.copy_overwrite`, and docutils footnote noise from fetched `_collections` content). The engine segregates these into the **Suppressed** bucket and never proposes a fix — do not "fix" them. They are tracked for removal under their own tickets ([DOC-1257](https://anyscale1.atlassian.net/browse/DOC-1257), [DOC-1258](https://anyscale1.atlassian.net/browse/DOC-1258)); when a suppression is lifted and its warning resurfaces, convert its `suppressions:` entry in `rules.yaml` into a rule.

---

## Version-bump re-validation

`rules.yaml` records a `baseline` (Sphinx/myst versions) and a per-rule validated range. MyST and Sphinx bumps rename or retire `[category]` tags (the tag default itself flipped at Sphinx 8.0), so matching also keys on message **signatures** as forward-compatibility insurance. After a dependency bump, re-confirm the category strings and signatures against a real build; any finding the engine flags `(unvalidated for this version)` is the re-validation worklist. Some core-Sphinx rules are signature-only today pending confirmation of their exact `[category]` on Sphinx 8.2.3 — confirm and add the category as you encounter them.

---

## Done

A change is verified when a full `make rtd` (or the green RtD PR preview) shows no warnings, and a regression check against `/en/master` shows the rendered content unchanged except where you intended (per `doc/.claude/CLAUDE.md`).

---

## Maintaining this skill

- `rules.yaml` is the single source of truth and stays within a restricted YAML subset so the stdlib fallback can parse it (see the header comment in the file). Don't introduce flow collections or block scalars.
- After editing `rules.yaml` or `sphinx_fix.py`, run the selftest: `python doc/.claude/skills/sphinx-fix/sphinx_fix.py --selftest`. It checks schema invariants, the fixture goldens, and that the fallback parser agrees with PyYAML. Regenerate goldens after an intentional output change with `--update-golden` (and eyeball the diff).
