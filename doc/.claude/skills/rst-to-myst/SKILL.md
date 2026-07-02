---
name: rst-to-myst
description: Convert Ray documentation pages from reStructuredText (.rst) to MyST Markdown (.md). Use when migrating existing files under doc/source/ to MyST, finishing a partial MyST migration of a directory, or when asked to convert/migrate a doc page to markdown. Covers the RST-to-MyST directive mapping, label and cross-reference preservation, sphinx-design tabs/dropdowns, doctest/testcode handling, the doc/BUILD.bazel doctest exclusions, and the build and doctest verification needed to land a clean docs PR.
user-invocable: true
argument-hint: <file(s) or directory under doc/source to convert>
---

# Convert RST to MyST Markdown

MyST Markdown is the standard for new Ray doc pages — `doc/.claude/CLAUDE.md` declares it, and a lint check rejects newly-added `.rst`. This skill converts an **existing** `.rst` page (or a batch) to MyST `.md` **faithfully**: format only, preserving the rendered HTML and any test coverage.

The Ray docs build with `fail_on_warning: true` (`.readthedocs.yaml`), so a sloppy conversion doesn't render wrong — it **fails the build**. Most of this skill is about the handful of constructs that break the build or silently drop test coverage if mishandled.

---

## When to use this skill

**Use when:**

- Migrating one or more existing `doc/source/**/*.rst` files to MyST `.md`.
- Finishing a partial MyST migration of a directory.

**Not for:**

- Authoring a brand-new page — just write `.md` directly (no conversion needed).
- Editing `.rst` content you're not converting (edits to existing `.rst` aren't lint-flagged).
- Notebooks (`.ipynb`) — different workflow.
- Bundling unrelated content rewrites — keep the diff a pure format conversion (see Golden rule).

---

## Golden rule: faithful conversion

Convert the **format**, not the content. The rendered HTML should be byte-equivalent to the pre-conversion page, **except** for deliberate, called-out light cleanup (a dead link, a stale version ref). No restructuring, no rewording of sound content, no heading-level "fixes."

Why: the decisive regression check compares the PR's Read the Docs preview against `/en/master` (per `doc/.claude/CLAUDE.md`). A faithful conversion makes that diff empty and the PR trivially reviewable. Capitalization nits ("github"→"GitHub"), heading-case changes, and rewraps all add noise and invite scope debates — leave them unless explicitly asked.

**Faithful does not mean byte-copying links.** A few RST link forms render fine in RST but are *wrong* in MyST and fail `fail_on_warning` (see Hard rule 2). Translate them; don't transcribe them.

---

## Procedure

### 1. Read the source(s) and the two style models

Read every `.rst` you're converting **in full**. Also read the canonical MyST examples in the same tree for house style: `doc/source/ray-contribute/docs.md` and `doc/source/ray-contribute/agent-development.md` (frontmatter, `(label)=`, `{contents}`, admonition and image conventions).

### 2. Pre-flight — verify every reference resolves *before* converting

A stale `literalinclude` path, autodoc symbol, or `{ref}` target turns into a build failure under `fail_on_warning`. Confirm each up front:

- **Labels this file defines** — `grep -nE '^\.\. _.*:' file.rst`. You must preserve **every** one (Hard rule 1). Note them.
- **External callers of those labels** — `grep -rn '<label-name>' doc/source python rllib`. Confirms they're load-bearing (and that you must not rename them).
- **`literalinclude` targets** — the file exists; `:lines:`/`:start-after:`/`:end-before:` markers still resolve.
- **autodoc targets** — every `.. autofunction::`/`.. autoclass::` symbol imports.
- **Who references THIS file** — grep the **bare filename** across all of `doc/`, e.g. `grep -rn 'getting-involved' doc/source`. Do **not** grep only the `dir/stem.rst` path: siblings link relatively (`[text](./getting-involved.rst)`, `(getting-involved.rst)`), and those break silently when you rename the file. Classify each hit (see "Reference updates"); most are no-ops, but `doc/BUILD.bazel`, `{include}`, and any relative `.rst` link from another page are not.

### 3. Convert using the mapping

Apply the table below construct-by-construct. Preserve prose line-wrapping verbatim (keeps the diff line-aligned). Then apply the Hard rules and Construct notes.

### 4. Update references that actually need it

Most don't (see checklist). The ones that do go **in the same PR** as the file they track.

### 5. Verify

Static checks → build (RtD) → doctest (if the file is doctest-tested) → regression vs `/en/master`. See "Verification".

### 6. Ship

`git rm` the `.rst`, add the `.md`. Commit, push, PR. For the OSS PR conventions (branch base, DCO sign-off, no internal ticket keys, etc.) follow the project's docs-PR workflow.

---

## The mapping

| RST | MyST Markdown |
|---|---|
| `.. meta::` / `:description:` | YAML frontmatter `myst:\n  html_meta:\n    description: "…"` |
| `.. _label:` above a heading | `(label)=` on its own line, blank line, then the heading |
| `====` / `----` underline | `#` / `##` … — **level by order of appearance, see Hard rule 3** |
| `` ``literal`` `` (double backtick) | `` `code` `` (single backtick) |
| `` `text` `` (single backtick) | `` `code` `` — faithful, since `default_role = "code"` |
| `` `text <url>`_ `` / `` `text <url>`__ `` | `[text](url)` |
| bare URL `https://…` | `<https://…>` (angle-bracket autolink — **`linkify` is off**) |
| same-page section link `` `text <page.html#sec>`_ `` | `[text](#sec)` (fragment) — **never keep the `.html#` URL; see Hard rule 2** |
| `` :ref:`text <label>` `` | `` {ref}`text <label>` `` |
| `` :doc:`text <path>` `` | `` {doc}`text <path>` `` |
| `.. note::` / `.. tip::` / `.. warning::` | `:::{note}` / `:::{tip}` / `:::{warning}` (colon fence) |
| `.. code-block:: LANG` / `.. code:: LANG` | fenced ` ```LANG ` |
| `.. tab-set::` / `.. tab-item:: T` | `::::{tab-set}` / `:::{tab-item} T` (colon fences — see Construct notes) |
| `.. dropdown:: T` (`:open:`) | `:::{dropdown} T` with `:open:` on the next line |
| `.. testcode::` / `.. testoutput::` / `.. doctest::` | ` ```{testcode} ` / `{testoutput}` / `{doctest}` — **only for real, executed blocks; see Hard rule 4** |
| `.. literalinclude:: P` (+opts) | ` ```{literalinclude} P ` with each option as a `:key: val` line |
| `.. autofunction::` / `.. autoclass::` | wrap in ` ```{eval-rst} ` … ` ``` ` (keep any adjacent `.. _label:` inside the same block) |
| `.. list-table::` (+opts) | ` ```{list-table} ` (keep the `* -` / `  -` body; don't reflow to a Markdown table) |
| `.. contents::` `:local:` | ` ```{contents} ` with `:local:` |
| `.. toctree::` | ` ```{toctree} ` — entries stay **extensionless** |
| `.. include:: f.rst` (you're converting `f`) | ` ```{include} f.md ` (convert the included file in the same PR) |
| `.. include:: _shared.rst` (shared partial, stays `.rst`) | ` ```{include} _shared.rst ` with `:parser: rst` (don't convert a shared `_includes/` partial) |
| `.. image:: URL` | `![](URL)` (match the `![alt](path)` style in `docs.md`) |
| `::` literal block | a plain ` ``` ` fence (no language) — see Construct notes |
| auto-lettered list `a.` / `b.` / `c.` | numbered `1.` / `2.` / `3.` — **MyST/CommonMark has no alpha lists** |

---

## Hard rules (get these wrong → broken build or lost test coverage)

1. **Preserve every label name exactly.** `.. _name:` → `(name)=` (own line, blank line, then the heading it labeled). External `{ref}`/`:ref:` callers resolve by **name** and are format-agnostic, so an unchanged label keeps working from `.rst` and `.md` callers alike. A renamed or dropped label breaks every caller. Labels sitting directly above an autodoc directive stay **inside** the `{eval-rst}` block as RST (`.. _name:` next to `.. autofunction::`); targets created inside `eval-rst` still register globally. A label directly above a non-heading directive (e.g. a `.. warning::`) becomes `(name)=` immediately before the converted `:::{warning}` — it still anchors.

2. **Links — translate, don't transcribe.** Three RST link forms need real translation; left as-is they emit a `myst.xref_*` warning (→ build failure):
   - **Whole-doc links** should use the `` {doc}`text <doc>` `` role — it resolves to the document and is never ambiguous. A bare `[text](sibling.rst)` (or `[text](sibling.md)` pointing at an `.rst` source) emits `myst.xref_missing`. An **extensionless** `[text](sibling)` works *only if* the target doc has no same-named label; if it does (e.g. a page carrying both the doc name `getting-involved` and a `(getting-involved)=` label), the bare link is ambiguous and emits `myst.xref_ambiguous`. So just use `{doc}`. This bites in *both* directions: a converted file linking to a still-`.rst` sibling, **and** an already-`.md` sibling whose link to the file you renamed now points at a dead `.rst`. (Re-check the bare-stem grep from pre-flight.)
   - **Same-page section links** written as a raw `page.html#section` URL must become a `#section` fragment (`[text](#section)`), resolved via `myst_heading_anchors`. The `.html#` URL renders in RST but MyST treats it as a cross-reference target and can't find it.
   - **`{ref}`** links are exempt (resolve by label, not path). **Extensionless** links and toctree entries are exempt (Sphinx resolves to whichever source exists).
   - These `myst.xref_*` classes and their fixes are also encoded as machine-readable rules in [`sphinx-fix/rules.yaml`](../sphinx-fix/rules.yaml) — the canonical category→fix table the `sphinx-fix` skill uses to diagnose a failing build. It's one shared source; keep the two in sync.

3. **Heading levels are assigned by ORDER OF FIRST APPEARANCE of each underline style — not by the character.** The same `-` underline can be `##` in one file and `###` in another, depending on what appeared before it. Overline+underline is a distinct style from underline-only. Walk the file top to bottom, assign level 1 to the first style seen, level 2 to the next new style, and so on; reproduce that exactly. Do **not** "fix" surprising nesting (e.g. a section that lands one level too deep) — that's restructuring and changes anchors. When unsure, check the live `/en/master` render of the page and match it.

4. **doctest/testcode: literal-vs-executed.** A meta-doc that *demonstrates* testcode often contains two kinds of blocks:
   - **Illustrative** — shown as syntax to copy. In RST they follow a `::` and are indented (a `literal_block`). Convert to a **plain ` ``` ` fence** (no language). These render but are **never executed**. Leaving the RST directive text (`.. testcode::`) as literal content inside the fence is correct and faithful.
   - **Real** — actually run and rendered. In RST they're column-0 `.. testcode::` / `.. doctest::` directives. Convert to `{testcode}` / `{doctest}` / `{testoutput}` fences.
   Decide **per block**. An illustrative block converted to a directive will execute and fail; a real block left as a plain fence silently loses CI coverage. After converting, count the executed directives and confirm the number matches the original's real blocks. (Note: a `{testcode}` in a doctest-*excluded* file still renders but doesn't run — see Hard rule 5.)

5. **`doc/BUILD.bazel` doctest exclusions.** The main `doctest(` rule globs `source/**/*.md` **and** `source/**/*.rst` with a per-file `exclude` list. If a file you convert is named in that exclude list, **rewrite its entry from `.rst` to `.md` in the same PR.** Otherwise the `*.md` glob pulls the newly-converted file **into** doctest, and blocks that were excluded for a reason (e.g. `ray.init(...)` with no `import ray`) execute and fail. Conversely, a file that is *included* (not excluded) stays tested as `.md` — that's when Hard rule 4 matters most.

---

## Construct notes

- **`default_role = "code"`** (`doc/source/conf.py`): an RST single-backtick already renders as inline code, so single-backtick → single-backtick is byte-faithful, not a rendering change.
- **Admonitions**: prefer colon fences `:::{note}` … `:::` (the `colon_fence` MyST extension is on). They nest a ` ``` ` code fence cleanly without backtick-counting. Backtick ` ```{note} ` also works for simple admonitions with no nested fence. A one-line RST admonition (`.. note:: text`) becomes `:::{note}` / `text` / `:::`.
- **sphinx-design `tab-set` / `tab-item` / `dropdown`**: use **colon fences**, not backtick fences — `::::{tab-set}` › `:::{tab-item} Label` › ` ```code ``` `. The outer fence needs **more colons** than the one it contains (4 vs 3), and colon fences nest cleanly around backtick code fences, so you avoid backtick-counting entirely. Put directive options (`:open:`, `:sync:`, …) on their own line right after the opener. (Confirmed against Ray's RtD build.)
- **`linkify` is OFF** (not in `myst_enable_extensions`). A bare URL will **not** autolink — wrap it as `<https://…>` to preserve the hyperlink. This includes URLs in parentheses like `Bazel 7.5.0 (https://…)` → `(<https://…>)`.
- **The `::` literal-block marker**: docutils drops `" ::"` when it's preceded by whitespace (`"…sessions. ::"` → `"…sessions."`) and replaces `"x::"` (no space) with `"x:"`. Reproduce the resulting prose, then put the block in a plain ` ``` ` fence.
- **`list-table`**: keep the directive (` ```{list-table} `), move options to `:key: val` lines, and de-indent the `* -` / `  -` body to column 0. Don't convert it into a native Markdown table.
- **Nested fences**: an outer fence must use **more backticks** than any fence it contains (or use a `:::` colon fence as the outer). Inside an ordered-list item, indent a nested ` ``` ` fence to the item's content column (3 spaces under `1. `).
- **`{eval-rst}` for autodoc** is the safe default; native `{autofunction}` is a fallback only if the build is verified clean. Keep the option indentation the RST used.
- **Include-only content partials** (a file that exists only to be `.. include::`d, like `involvement.rst`): give the `.md` **no frontmatter and no title** — it's spliced into its includer, and frontmatter would render mid-page there. These files don't orphan-warn even though they're not in any toctree (Sphinx doesn't treat included files as standalone docs). Inline any named-reference link targets in the partial, so they don't collide with the same target defined in the includer (RST tolerated the duplicate; inlining sidesteps it).
- **No alpha-enumerated lists**: MyST/CommonMark ordered lists are numeric only. Convert `a.`/`b.`/`c.` sub-lists to `1.`/`2.`/`3.` (a forced, minor rendering change — call it out in the PR). A column-0 code block between numbered items breaks the list, but explicit numbers still render correctly (faithful to the RST).

---

## Reference updates — what changes, what doesn't

**No-ops (don't touch / don't scope-creep):**

- **Toctree entries** — already extensionless; Sphinx resolves to whichever source exists.
- **Extensionless links** to the converted doc (e.g. `[text](page)` with no extension) — resolve fine.
- **`.html` URL references** — `doc/redirects/current.yaml`, `CONTRIBUTING.rst`, semgrep/lint scripts, and **absolute** `https://docs.ray.io/…/page.html#sec` links point at HTML output, which is identical regardless of source format. (Only *relative* `page.html#sec` self-links need fixing — Hard rule 2.)
- **External `{ref}`/`:ref:` callers** — safe as long as labels are preserved (Hard rule 1).

**Do change (same PR as the file):**

- **`doc/BUILD.bazel`** — doctest `exclude` entries (Hard rule 5) and any explicit doc-code test target naming the `.rst`.
- **`.. include::` / `{include}`** directives pointing at a file you're converting (convert both).
- **Relative `.rst` links from sibling pages** to the file you're renaming — found via the bare-stem grep. Point them at the new doc (extensionless or `{doc}`).
- **`.claude/` path mentions** of the file (e.g. `CLAUDE.md`, skill/rule files referencing `…/development.rst`). Re-grep `.claude/` for the stem. These are tiny string edits and `.claude/` isn't in `.buildkite/test.rules.txt`, so they don't pull extra CI suites.

---

## Verification

1. **Static checks** on each new `.md` — frontmatter parses (skip this for include-only partials, which have none), backtick **and** `:::` colon fences balance, no residual RST leaked outside fences, every label present, executed-directive counts match. Sketch:

   ```python
   import re, yaml
   for f in FILES:  # the new .md paths
       t = open(f).read()
       if t.startswith('---\n'):  # partials have no frontmatter
           assert yaml.safe_load(t.split('---\n', 2)[1])['myst']['html_meta']['description']
       L = t.splitlines()
       assert sum(ln.lstrip().startswith('```') for ln in L) % 2 == 0, f"unbalanced ``` {f}"
       assert sum(bool(re.match(r'^:{3,}\{', ln)) for ln in L) == sum(bool(re.match(r'^:{3,}\s*$', ln)) for ln in L), f"unbalanced colon fences {f}"
       infence = False                       # residual RST outside ``` fences
       for i, ln in enumerate(L, 1):
           if ln.lstrip().startswith('```'): infence = not infence; continue
           if infence: continue
           for pat in (r':(ref|doc):`', r'^\.\. ', r'`[^`]*<[^>]*>`_', r'[^:]::\s*$'):
               if re.search(pat, ln): print(f"RESIDUAL {f}:{i}: {ln!r}")
   ```

   Also `grep -c '^```{testcode}'` (etc.) and confirm the count equals the original's real blocks; grep for every label you noted in pre-flight; and `grep -n '\.html#\|](.*\.rst)'` to catch any link form Hard rule 2 forbids.

2. **Build (decisive parse check)** — `pre-commit run --files <changed>` is effectively a no-op for most `doc/source/**/*.md` (vale is scoped to `doc/source/data/`, prettier to js/ts/html/css), so the real check is Sphinx. A full local build is heavy; the practical signal is the **Read the Docs PR preview** (`docs/readthedocs.com:anyscale-ray`). With `fail_on_warning`, a green RtD build proves every label, `{ref}`, link, toctree entry, `{literalinclude}`, `{eval-rst}`, `{list-table}`, and sphinx-design directive resolved. **Read the raw RtD log on failure**: the build page lazy-loads, so fetch `https://app.readthedocs.com/api/v2/build/<BUILD_ID>.txt` and grep for `WARNING:`/`ERROR:`. `-W --keep-going` lists all warnings of a build that *completes* — but a hard-broken build (a `conf.py`/extension error, a traceback, or `SEVERE:`) aborts before producing that list, and a parse error or broken `toctree` masks the xref/orphan warnings beneath it. So if a failed build's log is empty or short, or fixing one error reveals new ones, fix the highest-severity error first and **re-run** — don't trust a single pass to be complete.

3. **Doctest (only for files the doctest rule includes)** — the RtD html builder does **not** execute testcode/doctest; that runs in the Buildkite doctest target (surfaces under `buildkite/microcheck` for a changed doc file). Confirm the real executed blocks pass and the illustrative ones don't run. MyST `{testcode}`/`{doctest}` in `.md` *is* exercised — `getting-started.md` and `configure-manage-dashboard.md` are tested `.md` precedents.

4. **Regression** — compare the RtD preview against **`/en/master`** (not `/en/latest`). Rendered content should match except where light cleanup intentionally changed it.

---

## Verified Ray-specific facts (as of mid-2026)

- `doc/source/conf.py`: `default_role = "code"`; `myst_enable_extensions` includes `colon_fence` but **not** `linkify`; `myst_heading_anchors = 3` (so `[text](#slug)` resolves to any h1–h3 heading).
- `doc/BUILD.bazel` main `doctest(` rule globs `source/**/*.md` + `source/**/*.rst`, with a per-file `exclude` list (e.g. `ray-contribute/getting-involved.md`, `ray-contribute/testing-tips.md`) and whole-subtree excludes for `ray-core/`, `data/`, `rllib/`, `serve/`, `train/`, `tune/` (which have their own `doctest` rules).
- `pre-commit` has no hook that lints `doc/source/**/*.md` outside `doc/source/data/` (vale) — so pre-commit passing is not evidence the page is correct; the Sphinx build is.
- The `ray-contribute/` directory was the first batch fully migrated (precedent for every pattern above, including sphinx-design tabs/dropdowns in `development.md` and the shared-include + partial handling in `getting-involved.md` / `involvement.md`).

---

## Gotchas

- **Heading level surprises are usually faithful, not bugs.** If a section renders one level deeper than feels right, the RST adornment order put it there. Reproduce it; don't fix it in a conversion PR.
- **Two `#` (H1) headings in one page** is fine when the RST had two top-level (`=`) sections.
- **Trailing whitespace inside code blocks** can be dropped (invisible, no linter on these files) — don't preserve it deliberately.
- **A converted file in the doctest exclude list but you forgot to update BUILD.bazel** is a likely silent break: the build stays green, but the doctest target starts running blocks that were never meant to run. Always re-grep `doc/BUILD.bazel` for the stem.
- **A sibling's *relative* link to the file you're renaming** (`[text](./page.rst)`) is the easiest reference to miss — it has no `dir/` prefix, so a path-scoped grep won't catch it. Grep the bare stem across all of `doc/`.
- **`.html#anchor` self-links and bare `.md`→`.rst` links** are faithful-but-wrong: they render in RST but fail `fail_on_warning` in MyST. Translate them (Hard rule 2).
- **An extensionless link to a doc that also carries a same-named label is ambiguous** (`myst.xref_ambiguous`), not missing — MyST can't tell the doc from the label. Use `{doc}` for whole-doc links so it always resolves to the document.
- **Don't trust pre-commit's silence** as a quality signal for these `.md` files — it skips them.
