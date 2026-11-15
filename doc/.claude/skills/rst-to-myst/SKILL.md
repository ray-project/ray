---
name: rst-to-myst
description: Convert Ray documentation pages from reStructuredText (.rst) to MyST Markdown (.md). Use when migrating existing files under doc/source/ to MyST, finishing a partial MyST migration of a directory, or when asked to convert/migrate a doc page to markdown. Covers the RST-to-MyST directive mapping, label and cross-reference preservation, sphinx-design tabs/dropdowns/card grids, doctest/testcode handling, the doc/BUILD.bazel doctest lists, and the build and doctest verification needed to land a clean docs PR. Ships a converter (scripts/rst2myst.py) that does the mechanical pass and flags what it can't convert.
user-invocable: true
argument-hint: <file(s) or directory under doc/source to convert>
---

# Convert RST to MyST Markdown

MyST Markdown is the standard for new Ray doc pages — `doc/AGENTS.md` declares it, and a lint check rejects newly-added `.rst`. This skill converts an **existing** `.rst` page (or a batch) to MyST `.md` **faithfully**: format only, preserving the rendered HTML and any test coverage.

The Ray docs build with `fail_on_warning: true` (`.readthedocs.yaml`), so most of a sloppy conversion doesn't render wrong — it **fails the build**. Most of this skill is about the handful of constructs that break the build or silently drop test coverage if mishandled.

**A green build is necessary and not sufficient.** A second, smaller class of mistake renders wrong *and* builds clean, with no warning anywhere: a lost page title, an image that changes markup, a directive whose nested RST degrades to visible text. Nothing in steps 1–3 of Verification can see any of it, because they all look at source or at reference resolution. Only the rendered diff in step 4 can. Run it.

## Status and layout

As of 2026-10-30, the only `.rst` sources left under `doc/source/` are the API reference pages under `*/api/`, the `_templates/autosummary/` Jinja templates, `_includes/rllib/new_api_stack.rst`, and `ray-observability/reference/cli.rst`. Use this skill for those, or for an `.rst` page that a long-lived branch brings back.

Paths in this skill that start with `doc/` are relative to the repo root. Run every command from there. In the commands below, `$SKILL` is `doc/.claude/skills/rst-to-myst`, and the scripts are under `$SKILL/scripts/`: `rst2myst.py`, `doctest_parity.py`, and `render_diff.py`. Two sibling skills share work with this one:

- **`ray-soft-wrap`** (`doc/.claude/skills/ray-soft-wrap/`) does the reflow commit.
- **`sphinx-fix/rules.yaml`** (`doc/.claude/skills/sphinx-fix/`) is the machine-readable table of `myst.xref_*` fixes that Hard rule 2 shares. When you change a link rule here, update the matching entry there.

The load-bearing Ray rules for a conversion PR are the root `AGENTS.md` contribution policy, DCO sign-off, and the `.md`/`.rst` cross-reference note in `doc/AGENTS.md`.

## Prefer native MyST over `{eval-rst}`

Convert every construct to native MyST when a native form exists, even when `{eval-rst}` would be quicker to write. An `{eval-rst}` block is RST embedded in Markdown: it's no better for readers of the source, for agents, or for the later style pass than the `.rst` it replaced. Reach for `{eval-rst}` only when there's no native path:

- **autodoc directives** (`autofunction`, `autoclass`, and the rest), where `{eval-rst}` is the established safe default. The `*/api/` reference trees are mostly autodoc, which is why they were left as `.rst` when the prose pages converted.
- **Third-party directives that `nested_parse` their content as RST**, such as `sphinxcontrib-openapi`, which degrade to literal text in native MyST. See Construct notes.

RST tables are not on that list. Convert them to `{list-table}`. This supersedes the earlier practice of wrapping RLlib's grid tables in `{eval-rst}`. The render diff decides whether a `{list-table}` reproduces the RST table closely enough. Three Ray pages have cleared it. `data/shuffling-data` (ray-project/ray#66375) and `ray-core/cross-language` (ray-project/ray#66410) converted RST tables, and `ray-core/resource-isolation-with-cgroupv2` (#66410) converted an existing `list-table`. In each, the `{list-table}` rendered the same table markup as the RST, so a top-level table's expected diff is empty. The diffs that did appear came from what surrounds a table: the backtick-caption and leading-`%` traps, and the `<dl>` class of a table nested in a definition list. All three are in Construct notes.

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

Why: the decisive regression check compares the PR's Read the Docs preview against `/en/master` (per `doc/AGENTS.md`). A faithful conversion makes that diff empty and the PR trivially reviewable. Capitalization nits ("github"→"GitHub"), heading-case changes, and rewraps all add noise and invite scope debates — leave them unless explicitly asked.

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
- **Open PRs that edit the file.** A conversion renames the file, so any open PR that edits the `.rst` has to move its change onto the `.md`. List them before converting: `gh pr list -R ray-project/ray --state open --limit 400 --json number,updatedAt,files -q '.[] | select([.files[].path] | any(test("<dir>/")))'`. The campaign converts anyway and lets the other PR rebase, as it did for #66368, #65974, #66402, #64137, and #65979 in batch 2. Name each one in the PR description.
- **Who references THIS file** — grep the **bare filename** across all of `doc/`, e.g. `grep -rn 'getting-involved' doc/source`. Do **not** grep only the `dir/stem.rst` path: siblings link relatively (`[text](./getting-involved.rst)`, `(getting-involved.rst)`), and those break silently when you rename the file. Classify each hit (see "Reference updates"); most are no-ops, but `doc/BUILD.bazel`, `{include}`, and any relative `.rst` link from another page are not. **Widen the grep past `doc/` for a landing or index page** — a page can be consumed by a file outside the Sphinx build that a `doc/`-scoped grep never sees. `rllib/index.rst` was `.. include::`d by `rllib/README.rst` (a package README, not a doc page and unable to hold Markdown); converting the index left three stale include paths in an `.rst` file, found only by grepping the package dir and repo root (`grep -rn '<stem>' rllib python .` plus a scan for `.. include::`).

### 3. Convert using the mapping

Start with the converter, which applies the mechanical part of the table below and reports everything it can't translate faithfully:

```bash
python3 $SKILL/scripts/rst2myst.py doc/source/<path>.rst > doc/source/<path>.md
```

It writes the `.md` to stdout and prints one `FLAG` line to stderr per construct a human has to resolve, such as a relative `.html` link, a substitution, an RST table, or an autodoc directive. Resolve every flag by hand before committing. It never guesses at a construct it doesn't model, so a page with no flags still gets the full static checks and render diff. Run `python3 $SKILL/scripts/rst2myst.py --selftest` after editing the converter, and add a case for each new construct.

Whether you use the converter or not, apply the table below construct-by-construct. **Keep the source's prose line-wrapping in this pass**, verbatim — it keeps the conversion diff line-aligned with the `.rst`, which is the only thing that lets a reviewer confirm at a glance that the words didn't change. Then apply the Hard rules and Construct notes.

Ray's `.md` prose is soft-wrapped, one line per paragraph and per list item, so a converted page shouldn't stay hard-wrapped. Reflow it as a **second, whitespace-only commit** in the same PR, using Ray's [`ray-soft-wrap`](https://github.com/ray-project/ray/blob/master/doc/.claude/skills/ray-soft-wrap/SKILL.md) skill, from the repo root:

```bash
python3 doc/.claude/skills/ray-soft-wrap/scripts/softwrap.py <the new .md files>
python3 doc/.claude/skills/ray-soft-wrap/scripts/verify.py   <the new .md files>
```

Splitting it into two commits gets both properties: the conversion commit stays reviewable line-by-line against the `.rst`, and the reflow commit is one a reviewer can skim in seconds because `verify.py` proves it changed nothing but whitespace — non-whitespace bytes byte-identical, rendered HTML identical, transform idempotent.

**`verify.py`'s render check is CommonMark plus GFM tables, so it cannot see MyST-only constructs.** It will pass a card grid whose `^^^` header separator got folded into the prose. `softwrap.py` protects `^^^` and `+++` by construction, but when a page leans on a construct the oracle doesn't model, add a structural assertion of your own — for card grids, that the `{grid-item-card}`, `^^^`, and `+++` counts still match. The step-4 render diff is the backstop either way.

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
| `` `text` `` (single backtick) | `` `code` `` — see Construct notes; the rendered `<code>` loses a `code` class that carries no styling |
| `` `text <url>`_ `` / `` `text <url>`__ `` | `[text](url)` |
| bare URL `https://…` | `<https://…>` (angle-bracket autolink — **`linkify` is off**) |
| same-page section link `` `text <page.html#sec>`_ `` | `[text](#sec)` (fragment) — **never keep the `.html#` URL; see Hard rule 2** |
| other-page link `` `text <other.html>`__ `` / `` `text <other.html#sec>`__ `` | `` {doc}`text <other>` `` / `` {ref}`text <label-on-that-section>` `` — **see Hard rule 2** |
| `` :ref:`text <label>` `` | `` {ref}`text <label>` `` |
| `` :doc:`text <path>` `` | `` {doc}`text <path>` `` |
| `.. note::` / `.. tip::` / `.. warning::` | `:::{note}` / `:::{tip}` / `:::{warning}` (colon fence) |
| `.. code-block:: LANG` / `.. code:: LANG` | fenced ` ```LANG ` |
| `.. tab-set::` / `.. tab-item:: T` | `::::{tab-set}` / `:::{tab-item} T` (colon fences — see Construct notes) |
| `.. dropdown:: T` (`:open:`) | `:::{dropdown} T` with `:open:` on the next line |
| `.. grid:: 1 2 2 2` (+opts) | `::::{grid} 1 2 2 2` (colon fence, **more colons than the cards it holds**) |
| `.. grid-item-card::` / `.. grid-item::` | `:::{grid-item-card}` / `:::{grid-item}` — keep `^^^` and `+++` on their own lines |
| `.. button-ref:: target` / `.. button-link:: url` | ` ```{button-ref} target ` / ` ```{button-link} url `, options as `:key: val`, blank line, then the label |
| `.. div:: classes` | `:::{div} classes` (sphinx-design; a bare `.. div::` takes no argument) |
| `.. testcode::` / `.. testoutput::` | ` ```{testcode} ` / `{testoutput}` — **only for real, executed blocks; see Hard rule 4** |
| `.. doctest::`, or `>>>` prompts in any block | ` ```{testcode} ` plus `{testoutput}`, never `{doctest}` — **see Hard rule 4** |
| `.. literalinclude:: P` (+opts) | ` ```{literalinclude} P ` with each option as a `:key: val` line |
| `.. autofunction::` / `.. autoclass::` | wrap in ` ```{eval-rst} ` … ` ``` ` (keep any adjacent `.. _label:` inside the same block) |
| `.. list-table::` (+opts) | ` ```{list-table} ` (keep the `* -` / `  -` body; don't reflow to a Markdown table) |
| RST grid table (`+---+`) or simple table (`=== ===`) | ` ```{list-table} ` with `:header-rows:` matching the RST header — **native, not `{eval-rst}`; see Prefer native MyST** The converter builds the `{list-table}` for a grid table whose cells each fit on one line, and passes any other grid table through for hand conversion. **Don't add `:widths:`**: a grid table renders no `<colgroup>`, and `:widths:` adds one (ray-project/ray#66669) |
| line block (`\| line`) | lines joined by a trailing `\` hard break — **no MyST line block; see Construct notes** |
| indented block quote (a paragraph or list indented under prose) | `> ` quote — **only when it isn't a list-item continuation; see Construct notes** |
| `.. contents::` `:local:` | ` ```{contents} ` with `:local:` |
| `.. toctree::` | ` ```{toctree} ` — entries stay **extensionless** |
| `.. include:: f.rst` (you're converting `f`) | ` ```{include} f.md ` (convert the included file in the same PR) |
| `.. include:: _shared.rst` (shared partial, stays `.rst`) | **problematic — see Hard rule 8.** MyST surfaces an included `.rst` partial's directives and comments as literal text, not parsed RST. If the partial renders nothing (all-comment), drop the include. |
| `.. image:: URL` | ` ```{image} URL ` — **not** `![](URL)`; see Hard rule 5 |
| `.. figure:: P` (+ caption) | ` ```{figure} P ` with options as `:key: val` lines, blank line, then the caption |
| `.. title:: T` | **no MyST equivalent** — see Hard rule 5 |
| `::` literal block | a plain ` ``` ` fence (no language) — see Construct notes |
| auto-lettered list `a.` / `b.` / `c.` | numbered `1.` / `2.` / `3.` — **MyST/CommonMark has no alpha lists** |
| auto-numbered list `#.` | `1.` on every item — **CommonMark renders `#.` as text**; the converter applies this |

---

## Hard rules (get these wrong → broken build or lost test coverage)

1. **Preserve every label name exactly.** `.. _name:` → `(name)=` (own line, blank line, then the heading it labeled). External `{ref}`/`:ref:` callers resolve by **name** and are format-agnostic, so an unchanged label keeps working from `.rst` and `.md` callers alike. A renamed or dropped label breaks every caller. Labels sitting directly above an autodoc directive stay **inside** the `{eval-rst}` block as RST (`.. _name:` next to `.. autofunction::`); targets created inside `eval-rst` still register globally. A label directly above a non-heading directive (e.g. a `.. warning::`) becomes `(name)=` immediately before the converted `:::{warning}` — it still anchors.

2. **Links — translate, don't transcribe.** Four RST link forms need real translation; left as-is they emit a `myst.xref_*` warning (→ build failure):
   - **Whole-doc links** should use the `` {doc}`text <doc>` `` role — it resolves to the document and is never ambiguous. A bare `[text](sibling.rst)` (or `[text](sibling.md)` pointing at an `.rst` source) emits `myst.xref_missing`. An **extensionless** `[text](sibling)` works *only if* the target doc has no same-named label; if it does (e.g. a page carrying both the doc name `getting-involved` and a `(getting-involved)=` label), the bare link is ambiguous and emits `myst.xref_ambiguous`. So just use `{doc}`. This bites in *both* directions: a converted file linking to a still-`.rst` sibling, **and** an already-`.md` sibling whose link to the file you renamed now points at a dead `.rst`. (Re-check the bare-stem grep from pre-flight.)
   - **Same-page section links** written as a raw `page.html#section` URL must become a `#section` fragment (`[text](#section)`), resolved via `myst_heading_anchors`. The `.html#` URL renders in RST but MyST treats it as a cross-reference target and can't find it.
   - **Relative links to another page** written as `` `text <other.html>`__ `` or `` `text <other.html#section>`__ ``. MyST reads the scheme-less target as a cross-reference and emits `myst.xref_missing`, like the same-page case. A whole-page link becomes `` {doc}`text <other>` ``. A section link becomes `` {ref}`text <label>` `` to the explicit label on that section. Check the target page for the label: in `namespaces`, `configure.html#driver-options` became `` {ref}`Driver Options <java-driver-options>` ``, the label sitting on that same "Driver options" section. Both render as internal references instead of external links, and a `{ref}` href uses the label's id rather than the heading slug. That's a known, explainable render diff. If the section has no label, add one to the target page in the same PR rather than leaving a raw `.html#` link.
   - **Scheme-less bare-domain targets** — an RST link whose target has no URL scheme (`` `PyTorch <pytorch.org>`__ ``, `` `gymnasium <gymnasium.farama.org>`__ ``). Transcribed faithfully to `[PyTorch](pytorch.org)`, MyST reads the scheme-less target as a **cross-reference**, not a URL, and emits `myst.xref_missing`. RST rendered it as a (relative, usually broken) link, so the page *looked* fine on the old site — the MyST build fails. Add the scheme: `[PyTorch](https://pytorch.org)`. This one transcribes cleanly and slips through review: it was latent in an already-merged conversion (`multi-agent-envs.md`, ray-project/ray#66062) and silently red-built the stack until the RtD preview caught it.
   - **`{ref}`** links are exempt (resolve by label, not path). **Extensionless** links and toctree entries are exempt (Sphinx resolves to whichever source exists).
   - These `myst.xref_*` classes and their fixes are also encoded as machine-readable rules in [`sphinx-fix/rules.yaml`](https://github.com/ray-project/ray/blob/master/doc/.claude/skills/sphinx-fix/rules.yaml) in Ray — the canonical category→fix table the `sphinx-fix` skill uses to diagnose a failing build. It's one shared source; keep the two in sync.

3. **Heading levels are assigned by ORDER OF FIRST APPEARANCE of each underline style — not by the character.** The same `-` underline can be `##` in one file and `###` in another, depending on what appeared before it. Overline+underline is a distinct style from underline-only. Walk the file top to bottom, assign level 1 to the first style seen, level 2 to the next new style, and so on; reproduce that exactly. Do **not** "fix" surprising nesting (e.g. a section that lands one level too deep) — that's restructuring and changes anchors. When unsure, check the live `/en/master` render of the page and match it.

4. **doctest/testcode: literal-vs-executed.** A meta-doc that *demonstrates* testcode often contains two kinds of blocks:
   - **Illustrative** — shown as syntax to copy. In RST they follow a `::` and are indented (a `literal_block`). Convert to a **plain ` ``` ` fence** (no language). These render but are **never executed**. Leaving the RST directive text (`.. testcode::`) as literal content inside the fence is correct and faithful.
   - **Real** — actually run and rendered. In RST they're column-0 `.. testcode::` / `.. testoutput::` / `.. doctest::` directives. Convert to `{testcode}` and `{testoutput}` fences.
   Decide **per block**. An illustrative block converted to a directive will execute and fail; a real block left as a plain fence silently loses CI coverage. (Note: a `{testcode}` on a page no doctest target lists still renders but doesn't run — see Hard rule 6.)

   **`>>>` examples belong in docstrings, not on pages.** Don't write a `{doctest}` fence or leave `>>>` prompts on a converted page. Rewrite a real `.. doctest::` block as `{testcode}` plus `{testoutput}`: the statements without their `>>>` and `...` prompts go in the `{testcode}`, and the expected output goes in the `{testoutput}`. Rewrite a `>>>` transcript that can't run in CI, such as one that needs a live cluster, as plain `python` and `text` blocks. This keeps every executed block on one runner, `pytest-sphinx`, which the parity check in Verification step 3 covers. The only pages that keep `>>>` are the two that teach the docstring format, `ray-contribute/writing-code-snippets.md` and `ray-contribute/getting-involved.md`. ray-project/ray#67432 converted the last five pages that had prompts, including `data/working-with-tensors` and `ray-references/glossary`. The rewrite drops the prompts and moves the output into its own block, so it's an explainable render diff. Call it out on the PR.

   **Prove it with the plugin's parser, not a grep.** Run `scripts/doctest_parity.py` on every converted page that a doctest target includes (see Verification step 3). A directive count is not enough, because what counts as collected is decided by the parser, and it differs between the two syntaxes:
   - **The RST regex matches `.. testcode::` at any indentation, including inside a `::` literal block or an RST comment.** So a block that looks illustrative may in fact have been executing, and converting it to a plain fence silently drops coverage. The parity check catches it. Hard rule 4's classification alone doesn't. A commented-out block (`.. ` followed by an indented `.. testcode::`) renders nothing on `/en/master` yet still executes, and it becomes an HTML comment that MyST never collects. If the block only defines names, as in `train/user-guides/experiment-tracking` (1 to 0 on ray-project/ray#66668), accept the drop and explain it on the PR.
   - **The MyST regex matches only a fence of exactly three backticks**, and a section ends at the first line that is exactly three backticks. A four-backtick ` ````{testcode} ` renders fine and is silently never collected.
   - **A `:hide:`, `:options:`, or `:skipif:` line must be followed by a blank line**, or collection raises and the whole file's doctest item errors.
   - **A `>>>` left on a converted `.md` gets a second runner that the `.rst` never had.** This is why the rule above removes them. The macro also passes `--doctest-glob='*.md'`, so pytest's stock doctest parser reads the raw Markdown and runs every `>>>` prompt in it, in any fence. pytest-sphinx builds examples only from `testcode`, and the stock glob doesn't match `.rst`. So a `.. doctest::` block, or a `>>>` in a code block, first executes when its page becomes `.md`. The stock parser reads expected output up to the next blank line, so a closing fence directly under an output line becomes part of that output and fails the example. Output that went stale while nothing ran it fails too. `data/working-with-tensors` failed 2 out of 2 on the fence in the ray-project/ray#66375 stack. The converter still emits a `{doctest}` fence for `.. doctest::`, ends any fence holding `>>>` with a blank line so the build stays green, and flags the page for the rewrite. The parity check fails on a swallowed fence.

5. **Page identity — the title and the images.** Three constructs change the rendered page while leaving the build green and emitting no warning. All three were caught by the render diff (Verification step 4) *after* a clean `fail_on_warning` build, not before it.
   - **`.. title::` has no MyST equivalent, and it does not work inside `{eval-rst}`.** The docutils directive sets `document['title']`, which `TitleCollector` reads for the `<title>` tag; under MyST that assignment does not reach the real document. A page whose title came from `.. title::` silently renders as `<no title>`. If the page has a heading, delete the directive and let the heading carry the title. If it has none, add an H1 with the same text: `env.titles` ends up identical, and a page with no heading is almost always one whose body a custom template overrides anyway, so the H1 never renders. Check the template before assuming that.
   - **`.. image::` is not `![]()`.** An RST `.. image::` with no `:alt:` takes its alt text from the URI and emits a bare `<img>` at block level. Markdown `![](path)` emits `alt=""` wrapped in a `<p>`. Use ` ```{image} path ` to keep both. `![alt](path)` is right only when you're supplying real alt text, which is a content change — call it out.
   - **A caption-less `.. figure::` is still a `<figure>`.** Converting it to an image of either form drops the `<figure>` wrapper and its alignment class. Keep ` ```{figure} `.

6. **`doc/BUILD.bazel` doctest lists.** Since ray-project/ray#66809 and #67003, prose doctests are `doctest_each()` calls over explicit page lists, one target per page (`//doc:source/<path>`, extension dropped, so conversion keeps the target name). No glob covers prose. If the `.rst` you convert is listed there, **rewrite its `files =` entry from `.rst` to `.md` in the same PR**, or the page silently stops being tested. If it's an `UNTESTED` key in `doc/test_prose_doctest_targets.py` (repo-relative, `doc/source/<path>.rst`), rewrite that too. The `doc_prose_doctest_targets` lint (`python doc/test_prose_doctest_targets.py`) fails on a stale or missing entry, and on a listed page left with nothing runnable. A listed page stays tested as `.md` — that's when Hard rule 4 matters most.

7. **An apostrophe in a heading silently changes its anchor.** docutils slugifies `What's Ray Core?` to `what-s-ray-core`; MyST drops the apostrophe and produces `whats-ray-core`. The build stays green, nothing warns, and any external link to the old anchor dies. Roughly 17 headings across 15 of the still-unconverted files are affected. The rule: **if the heading already carries an explicit label, that label is the anchor callers should be using, and you add nothing.** Only when the heading is bare do you add a compat target carrying the old docutils slug — `(what-s-next)=` above `## What's next?`. Never put two targets on one heading. **Grep for the slug first: `(name)=` is a global Sphinx label, not a page-local anchor.** A generic slug is often already taken. On ray-project/ray#66380, `(what-s-next)=` on `ray-core/starting-ray` failed the build with `duplicate label what-s-next`, because `cluster/getting-started.md` defines it. If the slug is taken, don't add the target. Accept the anchor change and call it out on the PR. Either way the section id and the headerlink href still change, so treat this as a known, explainable render diff rather than a regression to chase.

8. **Shared includes, substitutions, and raw-HTML images — three traps that build green and render wrong.**
   - **A shared `.rst` partial does not include cleanly into MyST.** MyST's `{include}` of a `.rst` file surfaces the raw content: RST directives and comments render as **literal visible text**, not parsed RST. A bare `{include}` and `:parser: rst` both do it (the latter as a code block), through a green build — so a commented-out partial, which renders nothing on the RST pages, dumps its raw text onto every including page. If the partial renders nothing, **drop the include** (the page loses nothing); if it carries active content, convert it to `.md` and include the `.md`, or inline it. Verified against `_includes/rllib/new_api_stack.rst` (myst-parser 5.1.0). **A Markdown copy that lives beside an `.rst` partial still in use needs a different stem.** Sphinx treats `_help.md` and `_help.rst` as one docname, `_includes/_help`, and warns `multiple files found for the document`, which fails the build. ray-project/ray#66417 added `_includes/_help-links.md` next to `_includes/_help.rst`, which three RST pages still include.
   - **RST substitutions (`|name|`) have no MyST equivalent here — the `substitution` extension is off.** A `.. |name| image::` definition plus a `|name|` use does **not** resolve, and wrapping both in one `{eval-rst}` block does not save it: docutils raises `Undefined substitution referenced` (a build error), because eval-rst's nested parse never runs the substitution transform, even with the definition in the same block. Drop the substitution and inline each use as an `<img>` tag (the `html_image` extension is on).
   - **A raw `<img>` is only processed inside MyST-parsed content.** In prose or a `{list-table}` cell, Sphinx processes the tag, copies the image to `_images/`, and rewrites the `src`. In a **raw HTML block** — a hand-written `<table>` you reached for to get `colspan` — the `<img src>` passes through verbatim and 404s, again through a green build. So a substitution-driven icon/sigil table becomes a `{list-table}` with `<img>` cells (accepting that list-table can't `colspan`), never a raw HTML `<table>`.

---

## Construct notes

- **`default_role = "code"`** (`doc/source/conf.py`): an RST single-backtick already renders as inline code, so single-backtick → single-backtick is the right conversion. It is *not* byte-identical, though: the RST form emits `<code class="code docutils literal notranslate">` and the Markdown form drops the `code` class. That class carries no styling in Ray's CSS or in `pydata-sphinx-theme`, and every already-converted page in the tree renders without it, so plain backticks are the house choice and `render_diff.py` filters this difference by default. Use the `` {code}`x` `` role only if you need a byte-identical diff for some other reason.
- **Backslash escapes: RST processes them where Markdown doesn't.** The default role processes backslash escapes, so `` `task_id=\<task_id\>` `` renders `task_id=<task_id>`. A Markdown code span keeps every backslash, so drop them inside the span. RST also removes an escaped space and renders an escaped letter as the letter, so `` ``ObjectRef``\s `` renders `ObjectRefs` and `(\ ``x``\ )` renders `(x)`. Markdown shows `\s` and `\ ` literally, so write `` `ObjectRef`s `` and `` (`x`) ``. All three build green and show stray backslashes. `fault_tolerance/tasks`, `internals/task-lifecycle`, `direct-transport/direct-transport`, and `actors/out-of-band-communication` had them. The converter applies these.
- **RST inline markup doesn't nest, so backticks inside bold are literal.** `` **`legacy`** `` renders a bold `` `legacy` `` with visible backticks on `/en/master`, and `` **No ``collate_fn`` in iterator** `` shows the double backticks. MyST nests inline code inside bold, so escape the backticks to keep the render: `` **\`legacy\`** ``. The converter applies this. Whether the RST author meant inline code is a content question, so leave it for the style pass.
- **Admonitions**: prefer colon fences `:::{note}` … `:::` (the `colon_fence` MyST extension is on). They nest a ` ``` ` code fence cleanly without backtick-counting. Backtick ` ```{note} ` also works for simple admonitions with no nested fence. A one-line RST admonition (`.. note:: text`) becomes `:::{note}` / `text` / `:::`. When the argument wraps, as in `.. tip:: If you have suggestions,` with an indented line directly below it and no blank line between, the second line continues the same paragraph. Keep the two lines together, with no blank line between them, or the sentence splits into two paragraphs through a green build. Batch 5 had 7 of these, ray-project/ray#66669. The converter handles this.
- **sphinx-design `tab-set` / `tab-item` / `dropdown`**: use **colon fences**, not backtick fences — `::::{tab-set}` › `:::{tab-item} Label` › ` ```code ``` `. The outer fence needs **more colons** than the one it contains (4 vs 3), and colon fences nest cleanly around backtick code fences, so you avoid backtick-counting entirely. Put directive options (`:open:`, `:sync:`, …) on their own line right after the opener. (Confirmed against Ray's RtD build.)
- **`linkify` is OFF** (not in `myst_enable_extensions`). A bare URL will **not** autolink — wrap it as `<https://…>` to preserve the hyperlink. This includes URLs in parentheses like `Bazel 7.5.0 (https://…)` → `(<https://…>)`.
- **Placeholder text in angle brackets is HTML to Markdown.** RST has no inline HTML, so `<REGION>` renders as text. CommonMark reads anything shaped like a tag, a letter-led name of letters, digits, and hyphens, as raw HTML. The tag swallows the rest of the paragraph, through a green build. `<AWS_ACCOUNT_ID>` survives only because `_` isn't legal in a tag name. Escape it as `\<REGION>`. `train/examples/aws-trainium/llama3` had one, ray-project/ray#66668. The converter escapes these outside code spans and autolinks.
- **The `::` literal-block marker**: docutils drops `" ::"` when it's preceded by whitespace (`"…sessions. ::"` → `"…sessions."`) and replaces `"x::"` (no space) with `"x:"`. Reproduce the resulting prose, then put the block in a plain ` ``` ` fence.
- **sphinx-design card grids convert to native MyST — and everything nested inside them has to convert too.** A `grid` of `grid-item-card`s becomes colon fences, widest on the outside: `::::{grid} 1 2 2 2` › `:::{grid-item-card}` › a ` ```{button-ref} ` backtick fence. Add a colon level for each extra layer (`ray-libraries.md` runs `:::::{grid}` › `::::{grid-item-card}` › `:::{div}`). The `^^^` header and `+++` footer separators need no translation at all: sphinx-design matches them with `REGEX_HEADER`/`REGEX_FOOTER` and splits them out of the raw content lines *before* anything parses them, so they're format-agnostic.

  **The trap is `nested_parse`.** `GridDirective`, `GridItemCardDirective`, `div`, and Ray's own `callout`/`annotations` all call `self.state.nested_parse`, which under MyST parses their content as **Markdown**. RST left inside a native card doesn't error — it renders as literal text, through a green `fail_on_warning` build. So a card's nested `button-ref`, `button-link`, `image`, and `figure` all have to become fences in the same pass, and the render diff (Verification step 4) is the only check that will catch it if one doesn't. This is the `nested_parse` degradation referenced in step 4.

  Four already-Markdown pages predate this and wrap their whole grid in `{eval-rst}` (`cluster/vms/index.md`, `kuberay/index.md`, `ray-overview/index.md`, `serve/index.md`). Don't copy that pattern into a new conversion; native is the house choice as of batch 1.

- **`list-table`**: keep the directive (` ```{list-table} `), move options to `:key: val` lines, and de-indent the `* -` / `  -` body to column 0. Don't convert it into a native Markdown table.
- **A backtick-fenced directive whose argument contains a backtick doesn't open the fence.** CommonMark forbids backticks in a ` ``` ` fence's info string, so a ` ```{list-table} ` opener whose caption contains inline code (for example a `map_batches` mention) is parsed as a paragraph, not a directive. The table and every construct after it then degrade to literal text, through a green `fail_on_warning` build. Use a **colon fence** for any directive whose caption or argument carries a backtick: `:::{list-table} …`. Colon-fence arguments allow backticks, and `:::{list-table}` with a caption is already used in the tree. The static fence-balance check still passes, so only the render diff catches this.
- **A directive option value that MyST's option parser would rewrite has to be quoted.** MyST reads `:key: value` lines with a YAML-style parser, and docutils takes the value verbatim. In myst-parser 5.1.0, `:start-after: # Head service` parses as an empty value plus a comment, which fails the build with `myst.directive_option`. A ` #` later in the value is dropped the same way, a leading quote is stripped, and a leading `|` or `>`, or an unterminated quote, is a parse error. Double-quote the value, escaping `\` and `"`: `:start-after: "# Head service"`. `community/yarn` had three, in ray-project/ray#66678. Values without those characters parse back unchanged, including `if __name__ == "__main__"` and `1#2`, so leave them bare. Options inside `{eval-rst}` stay RST and aren't quoted. The converter applies this.
- **A `{list-table}` cell whose content starts with `%` is eaten as a MyST comment.** MyST treats a line beginning with `%` as a block comment, and `{list-table}` re-parses each cell's content as its own block, so a header cell written `  - % of baseline` renders as an empty `<th>`, through a green build. Escape the leading marker: `  - \% of baseline` renders `%` and isn't a comment. A `%` anywhere but the start of a cell (for example a `100%` data cell) is unaffected. Only the render diff catches the missing cell, so verify a converted table's headers and cells all rendered.

- **An RST table indented under a list item with no blank line is a definition list, and the table is its definition.** docutils reads a line indented past a list item's text, with no blank line between them, as the definition of a term. So `- Primitive data types` followed directly by an indented simple table renders `<li><dl><dt>Primitive data types</dt><dd><table>`. Under prose it also sits in a `<blockquote>`. `ray-core/cross-language` has two of these. The `deflist` MyST extension is on, so keep the structure natively: the item's text is the term, and a `: ` line opens the definition, holding the `{list-table}` indented to the definition's content column. In a block quote, every line carries the `> ` prefix:

  ````text
  > - Primitive data types
  >   : ```{list-table}
  >     :header-rows: 1
  >
  >     * - MessagePack
  >       - Python
  >     ```
  ````

  The render diff on ray-project/ray#66410 confirmed it: the block quote, list, `<dt>`, and table markup match `/en/master`. The one difference is that MyST's `<dl>` carries `class="simple myst"` where docutils emits no class or `simple`. The class has no styling. For a construct you haven't seen before, a scratch Sphinx project pinned to `doc/requirements-doc.txt`'s `sphinx` and `myst-parser`, with `conf.py`'s `myst_enable_extensions`, compares candidate syntax against the RST in seconds, before you push. The converter flags both the indented table and the definition list inside the item, but it doesn't build this structure, so write it by hand. A plain bullet item with the table as an indented continuation drops the `<dl>` and moves the table out of the definition.
- **A one-line paragraph followed by an indented line is a definition list, whatever the indented line holds.** `Ray system memory: …` followed directly by `  - **GCS**: …` renders `<dl><dt>Ray system memory: …</dt><dd><ul>` on `/en/master` (`scheduling/memory-management`). So does `**Label Filtering**` over an indented paragraph (`internals/metric-exporter`). Open the definition with `: ` and keep the rest of it at the 2-space content column. A blank line and more 2-space-indented content stay inside the definition:

  ```text
  Ray system memory: this is memory used internally by Ray
  : - **GCS**: memory used for storing the list of nodes and actors.
    - **Raylet**: memory used by the C++ raylet process.
  ```

  Escaping the dash instead, as for an inline pseudo-list, renders the items as inline text. The converter builds this structure for a one-line term at column 0 over a 2-space indent, and flags other shapes. A definition list inside a list item, such as `* System-level errors include:` over a 3-space-indented bullet list in `direct-transport/direct-transport`, takes the same `: ` form at the item's content column, written by hand.
- **A line indented less than a list item's content column ends the list.** In `objects/serialization`, a `testoutput` sits 2 spaces in under an item whose content starts at column 3, after that item's `testcode`. docutils closes the list at the shallower line, renders the `testoutput` in a block quote, and restarts the list at 2. That's an RST indentation slip, not intended structure, so keep both blocks inside the item and call out the render diff, as for any `/en/master` breakage you decline to reproduce. Moving a doctest block into a `> ` quote also stops `pytest-sphinx` collecting it, which the parity check catches. The converter reproduces the RST structure and flags it.
- **A block indented past a list item's content column is a block quote inside the item.** After a blank line, docutils treats a continuation indented deeper than the item's text as a block quote, whatever it holds. A `.. code-block::` indented 4 spaces under `- ` or `1. ` renders `<li><p>…</p><blockquote>` on `/en/master`. Keep the quote with a `> ` prefix at the item's content column: `  > ```yaml` under `- `, `   > ```yaml` under `1. `. `vms/references/ray-cluster-configuration` had five, in ray-project/ray#66682. A block at exactly the content column is a plain continuation, as in Nested fences below. A doctest block in the quote won't be collected by pytest-sphinx, so the converter flags it, as it does for other quotes. The converter applies this.
- **Grep a table census for code blocks.** A count of `===` border lines also matches `=====` underlines inside a ` ```text ` ASCII diagram. `ray-core/direct-transport/custom-tensor-transport` was counted as having 2 RST tables, but both hits are diagram headers inside `code-block:: text`, and the page has no tables. Confirm a table hit is outside a code block before planning hand work for it.
- **RST named hyperlink targets (`` `text`_ `` plus a `.. _`text`: url` definition) have no MyST equivalent.** Inline each one as `[text](url)` at the point of use and delete the definition block. The exception is a target referenced from inside an `{eval-rst}` block — an RST simple or grid table whose cells carry `` `text`_ `` references. Keep those definitions as RST, in the same `{eval-rst}` block as the table that uses them, so they resolve without duplicating a target name elsewhere in the document (`ray-overview/installation.md` does this for the nightly-wheel table). That's the pattern for pages already converted with a wrapped table. A new conversion turns the table into a `{list-table}`, so its `` `text`_ `` references get inlined like any other.
- **A section id can change when a link's text matches a heading.** An RST link with a single trailing underscore, `` `Plasma object store <url>`_ ``, also creates a named target from its text. When that text matches a heading on the same page, the heading loses its implicit name, and docutils falls back to the heading's explicit label for the section id, plus a stray `id1`. `[text](url)` creates no target, so under MyST the section takes its heading slug and the label becomes a `<span>` anchor inside it. `objects/serialization` shows this on ray-project/ray#66415: the section id moves from `plasma-store` to `plasma-object-store`, and `#plasma-store` still resolves. It's an explainable render diff, not a regression.
- **RST comments become HTML comments.** An RST comment — a `.. ` line that isn't a valid directive (`.. TODO: …`, `.. todo (sven): …`, a commented-out block) — renders nothing. Convert it to an HTML comment (`<!-- … -->`) to keep the author's note in the source; the merged pages do this (`advanced-api.md`, `fault-tolerance.md`). Expect the step-4 render diff to flag it as a benign addition: RST emits no node at all, so the `<!-- … -->` shows up as a diff even though it's invisible to readers. Preserve a multi-line commented block as one `<!-- … -->` (a CommonMark comment block runs until `-->`); don't reflow its contents. **The exception is a comment that contains `-->`.** A Mermaid diagram's arrows end the HTML comment early, and the rest of the comment renders as visible text and a code block, through a green build. `internals/object-spilling` had five. Use MyST `%` line comments instead, prefixing every line: they render nothing at all, like the RST comment. The converter does this whenever a comment contains `-->`. **Two placements need care.** A comment inside a list item has to be a `%` comment, because an HTML comment is a raw node and docutils drops the list's `simple` class for it, which changes the item spacing. The `%` comment is invisible, as the RST comment was. The converter does this. And a comment directly under a label takes the label's id with it: `(tune-tutorial)=` above `<!-- TODO -->` above the H1 rendered no `tune-tutorial` anchor at all, through a green build. Put the comment above the label. The converter flags it.
- **Nested fences**: an outer fence must use **more backticks** than any fence it contains (or use a `:::` colon fence as the outer). Inside an ordered-list item, indent a nested ` ``` ` fence to the item's content column (3 spaces under `1. `).
- **Tabs expand to 8-column stops before parsing.** docutils expands every tab in the source to the next multiple of 8 columns, then dedents. A code block indented with tabs therefore renders with spaces, and a tab inside a line becomes padding to the next stop. Expand tabs on the raw line before dedenting, or the columns come out wrong. A tab-indented block also defeated the converter's block detection, which emitted an empty fence followed by the block as escaped prose (`job-submission/quickstart`, ray-project/ray#66679). The converter now expands tabs first, the way docutils does.
- **`{eval-rst}` for autodoc** is the safe default; native `{autofunction}` is a fallback only if the build is verified clean. Keep the option indentation the RST used.
- **Include-only content partials** (a file that exists only to be `.. include::`d, like `involvement.rst`): give the `.md` **no frontmatter and no title** — it's spliced into its includer, and frontmatter would render mid-page there. These files don't orphan-warn even though they're not in any toctree. That doesn't mean Sphinx skips them: unless `exclude_patterns` drops a partial, Sphinx also builds it as a page of its own, which is what makes the heading rule below necessary. **A partial with headings has two renderings, and both have to be valid.** In RST, an included file's underline styles join the includer's adornment order, so `train/common/torch-configure-run.rst`'s `---` headings render as H2 inside `getting-started-pytorch`. Unless `exclude_patterns` drops the partial, Sphinx also builds it as a page of its own, where the same headings render as H1. A MyST `{include}` doesn't re-level headings, and MyST fails a document whose first heading is H2 (`myst.header`, a `fail_on_warning` error). So keep the partial's headings at the standalone level (`#`), and add `:heading-offset: 1` to each `{include}` to shift them in the includer. Check both levels against `/en/master` first. Setting the partial to `##` builds red, ray-project/ray#66668.
- **No alpha-enumerated lists**: MyST/CommonMark ordered lists are numeric only. Convert `a.`/`b.`/`c.` sub-lists to `1.`/`2.`/`3.` (a forced, minor rendering change — call it out in the PR). A column-0 code block between numbered items breaks the list, but explicit numbers still render correctly (faithful to the RST).
- **Inline pseudo-lists: escape the leading marker.** RST needs a blank line before a list. A `1. text` or `- text` line directly under a prose line is paragraph text, and docutils renders it inline. CommonMark lets a bullet or a list starting at `1` interrupt a paragraph, so a transcribed line turns into a real list. Escape the marker, as `1\.` or `\-`. A `2.` line can't interrupt a paragraph, so later numbers need no escape, but every bullet can, so escape each `-` line. Blank-separated lists stay lists. Verified against the live HTML for `ray-core/patterns/fork-new-processes` and `ray-core/fault-tolerance`. The same applies inside a list item: a `- ` line at the item's own text column, with no blank line before it, is part of the item's paragraph. `- **For launching new instances**:` followed by `  - ``instance_id``: …` renders inline on `/en/master` (`internals/autoscaler-v2`, `internals/ray-event-exporter`). Indentation decides which construct you have. A marker line at the paragraph's text column is inline text. One indented past it is a definition list, covered below. One at the item's marker column is a sibling item. The converter applies the escape at the text column only.
- **An enumerator whose next line isn't indented is paragraph text, even after a blank line.** docutils accepts an enumerated list item only when the next line is blank, indented, or another enumerator. So `1. Ensure your EC2 instance...` followed by `Notebook on it.` at the same column renders `<p>1. Ensure...`, and so does a `1)` line. CommonMark starts a list at any number after a blank line, with the unindented lines as lazy continuations, so escape every such marker, as `2\.` or `1\)`, not only a `1`. `ray-core/using-ray-with-jupyter` has four of these, ray-project/ray#66425. The converter applies the escape and flags each one. Check the flag against `/en/master` before accepting it, because the rule is an approximation of docutils' list-start check.
- **MyST's `replacements` extension rewrites text that docutils leaves alone.** Ray's `conf.py` enables it, and it turns `(c)`, `(tm)`, and `(r)` into symbols, `+-` into `±`, and any run of two or more dots into one ellipsis. docutils' smartquotes already converts `...` and dashes, so those match and need nothing. The rest build green and change the text: `handling-dependencies`' `(c)` list markers rendered as `©`, and `getting-started`'s `etc..` as `etc…`, ray-project/ray#66461. Escape the first character that matters: `\(c)`, `+\-`, `etc.\.`. The converter does this outside code spans and autolinks, including in link text. It skips grid-table borders and a `..` that opens a word, which is leftover RST such as `- .. image::` that you still have to convert. It flags runs of four or more dots, `????`, `!!!!`, and `,,` without escaping them, because what docutils renders for those is unverified.
- **A doctest directive whose body has no blank line before it renders nothing on `/en/master`.** In `.. testcode::` followed directly by an indented line, docutils reads that first block as the directive's optional argument, the group name, and renders none of it. `rllib/getting-started` lost six lines of setup code this way, so `/en/master` showed code that used `best_result` without defining it. The `.md` renders the block, which is what the author meant, so keep it and call out the render diff. The converter flags it. As of 2026-09-24, `ray-observability/getting-started` (`testoutput`) and `train/getting-started-xgboost` (`testcode`) also have it.
- **A prose line starting with `#` becomes a heading: escape it.** RST renders `# Alternatively, run this.` as paragraph text. CommonMark reads `#` plus a space as an ATX heading, even in the middle of a paragraph. Write `\# Alternatively, ...`. The stray heading also shifts the page's section structure. `ray-core/configure` had one, and it failed the build only because the following `###` headings then tripped `myst.header` ("H1 to H3"). Where no level jump follows, it builds green, so grep converted pages for heading lines whose text isn't a heading in the `.rst`. The converter applies this escape. The same holds for a `{list-table}` cell, which MyST parses as its own block: a cell such as `- # of trials` renders as a heading inside the table, so write `- \# of trials`. `tune/tutorials/tune-scalability` has twelve of these. The converter escapes both.
- **A prose line starting with `>` becomes a block quote: escape it.** RST has no `>` quote syntax, so a wrapped line that happens to start with `>`, such as `(p3 > p2, g4` / `> g3)`, is paragraph text. CommonMark lets a block quote interrupt a paragraph, so write `\> g3)`. `vms/user-guides/large-cluster-best-practices` had two, in list items. A `>>>` line is an RST doctest block, which this escape leaves alone. The converter applies this.
- **Line blocks have no MyST equivalent.** An RST line block (`| line` per line) renders as `div.line-block` with one `div.line` per line. The closest native form is a Markdown hard line break: end every line except the last with a trailing `\`. That renders a `<p>` with `<br>` between the lines, the same visible result. It's an explainable render diff, so call it out in the PR. `ray-core/ray-dag` ends with a two-line line block. The same applies inside a `{list-table}` cell (`- | line` followed by `  | line`): left as-is, it renders literal `|` text through a green build. `ray-core/tips-for-first-time`'s API table has five such cells.
- **An indented block under prose is a block quote.** In RST, a paragraph or list indented under a prose paragraph, with a blank line between them, renders inside `<blockquote>`. Convert it with a `> ` prefix on each line to keep the quote. The exception is a list-item continuation: a block indented to the content column of the list item above it belongs to that item, so keep it indented instead. `ray-core/tasks` has a quoted list under "Note the following behaviors:".
- **`:orphan:`** as a leading field becomes `orphan: true` in the YAML frontmatter.
- **Roles and links can wrap across source lines.** A `` :ref:`text `` at the end of one line with ``more <label>` `` on the next is one role. Convert inline markup per paragraph, not per line, or the role stays RST and fails the build. `ray-core/fault-tolerance` has one.
- **A link whose `<url>` starts the next line: put `](url)` on the text's line.** RST drops the whitespace before `<url>`, so `` `the Hadoop Java client `` / ``<https://…>`_ `` renders as link text with no trailing space. Keeping the source wrap verbatim moves that line break inside the Markdown brackets, `[the Hadoop Java client` / `](https://…)`, and CommonMark keeps it as part of the link text. The `<a>` then ends in a whitespace character, so the underline runs over a trailing space. The build stays green, and the later soft-wrap pass turns the break into a literal `client ](`. This is the one exception to keeping the source wrap. The converter strips the label, so this bites hand conversions: `data/loading-data` shipped three of these, ray-project/ray#66375.

---

## Reference updates — what changes, what doesn't

**No-ops (don't touch / don't scope-creep):**

- **Toctree entries, with or without a suffix** — Sphinx resolves an extensionless entry to whichever source exists, and it strips a known source suffix from an entry before resolving it, so a `foo.rst` entry also resolves once `foo` converts to `.md`. ray-project/ray#66414 built green under `fail_on_warning` with its still-`.rst` parents listing `fault_tolerance/tasks.rst` and `internals/object-spilling.rst`. So a still-`.rst` parent needs no edit, which keeps the PR clear of files that another conversion PR is renaming. The converter makes entries extensionless in the file it converts, as house style.
- **Extensionless links** to the converted doc (e.g. `[text](page)` with no extension) — resolve fine.
- **`.html` URL references** — `doc/redirects/current.yaml`, `CONTRIBUTING.rst`, semgrep/lint scripts, and **absolute** `https://docs.ray.io/…/page.html#sec` links point at HTML output, which is identical regardless of source format. (Only *relative* `page.html#sec` self-links need fixing — Hard rule 2.)
- **External `{ref}`/`:ref:` callers** — safe as long as labels are preserved (Hard rule 1).

**Do change (same PR as the file):**

- **`doc/BUILD.bazel`** — the page's doctest `files =` entry (Hard rule 6) and any explicit doc-code test target naming the `.rst`. Also its `UNTESTED` key in `doc/test_prose_doctest_targets.py`, if any.
- **`.. include::` / `{include}`** directives pointing at a file you're converting (convert both).
- **Relative `.rst` links from sibling pages** to the file you're renaming — found via the bare-stem grep. Point them at the new doc (extensionless or `{doc}`).
- **`.claude/` path mentions** of the file (e.g. `CLAUDE.md`, skill/rule files referencing `…/development.rst`). Re-grep `.claude/` for the stem. These are tiny string edits, and `.buildkite/test.rules.txt` skips `.claude/` and `doc/.claude/`, so they don't pull extra CI suites.

---

## Verification

1. **Static checks** on each new `.md` — frontmatter parses (skip this for include-only partials, which have none), backtick **and** `:::` colon fences balance, no residual RST leaked outside fences, **no tool-call scaffolding leaked from the conversion agent itself**, every label present, executed-directive counts match. Sketch:

   ```python
   import re, yaml
   for f in FILES:  # the new .md paths
       t = open(f).read()
       if t.startswith('---\n'):  # partials have no frontmatter
           assert yaml.safe_load(t.split('---\n', 2)[1])['myst']['html_meta']['description']
       L = t.splitlines()
       assert sum(ln.lstrip().startswith('```') for ln in L) % 2 == 0, f"unbalanced ``` {f}"
       assert sum(bool(re.match(r'^:{3,}\{', ln)) for ln in L) == sum(bool(re.match(r'^:{3,}\s*$', ln)) for ln in L), f"unbalanced colon fences {f}"
       assert not re.search(r'</?(?:invoke|function_calls|parameter|content)\b|antml:', t), f"leaked agent tool-call scaffolding {f}"
       infence = False                       # residual RST outside ``` fences
       for i, ln in enumerate(L, 1):
           if ln.lstrip().startswith('```'): infence = not infence; continue
           if infence: continue
           for pat in (r':(ref|doc):`', r'^\.\. ', r'`[^`]*<[^>]*>`_', r'[^:]::\s*$'):
               if re.search(pat, ln): print(f"RESIDUAL {f}:{i}: {ln!r}")
           if re.search(r'^\s*\]\(|\S\s+\]\(', ln):   # whitespace closing a link's text
               print(f"LINK-WS {f}:{i}: {ln!r}")
   ```

   Also `grep -c '^```{testcode}'` (etc.) and confirm the count equals the original's real blocks; grep for every label you noted in pre-flight; and `grep -n '\.html#\|](.*\.rst)'` to catch any link form Hard rule 2 forbids. Add a scheme-less bare-domain detector for the Hard rule 2 case that transcribes clean — `grep -noE '\]\([a-z0-9.-]+\.[a-z]{2,}(/[^)]*)?\)' <files> | grep -vE 'https?://|\.(md|rst|html|svg|png)[)#]|\]\(#|\]\(\.{1,2}/'` — and expect zero hits (a link target like `(pytorch.org)` with no scheme is a build-failing xref, not a URL). The `-o` is load-bearing: it emits one matched link per line so the exclusion filter runs **per link**. Without it the filter excludes on the whole line, and since the soft-wrap pass collapses each paragraph to a single line, a bad scheme-less target sharing a paragraph with a real `https://` link (or a valid `.md`/`.html` link) would be dropped along with the line it shares.

   **Grep for your own scaffolding, not just the source's.** When *you* — an agent — do the conversion, your tool-call framing can leak into the output: closing tags like `</content>` and `</invoke>` trail at the end of a page (or `<function_calls>`, `<parameter …>`, an `antml:`-prefixed tag mid-file). These build green and render as literal visible text, and because they sit at EOF the render diff can miss them if the page's tail is below the fold. Run `grep -rnE '</?(invoke|function_calls|parameter|content)>|antml:' <the new .md files>` and expect zero hits. Seen for real: seven of the RLlib guide pages shipped with trailing `</content>`/`</invoke>` tags from the conversion agent (ray-project/ray#66213).

2. **Build (decisive parse check)** — `pre-commit run --files <changed>` is effectively a no-op for most `doc/source/**/*.md` (the `doc/source/data/` vale hook is commented out for now, prettier is scoped to js/ts/html/css), so the real check is Sphinx. A full local build is heavy; the practical signal is the **Read the Docs PR preview** (`docs/readthedocs.com:anyscale-ray`). With `fail_on_warning`, a green RtD build proves every label, `{ref}`, link, toctree entry, `{literalinclude}`, `{eval-rst}`, `{list-table}`, and sphinx-design directive resolved. **Read the raw RtD log on failure**: the build page lazy-loads, so fetch `https://app.readthedocs.com/api/v2/build/<BUILD_ID>.txt` and grep for `WARNING:`/`ERROR:`. `-W --keep-going` lists all warnings of a build that *completes* — but a hard-broken build (a `conf.py`/extension error, a traceback, or `SEVERE:`) aborts before producing that list, and a parse error or broken `toctree` masks the xref/orphan warnings beneath it. So if a failed build's log is empty or short, or fixing one error reveals new ones, fix the highest-severity error first and **re-run** — don't trust a single pass to be complete. **A preview that shows `cancelled` failed nothing.** The RtD queue cancels PR builds under load, sometimes before Sphinx starts, so retrigger it as below. The `rtd-build-logs` skill covers reading build state and logs. **A build that fails in about a minute with `Segmentation fault (core dumped)` during `[autosummary] generating` is a runner flake**, not your change: on 2026-09-23 it hit master and several unrelated PRs, and a rebuild passed. Retrigger it without a push: `curl -X POST -H "Authorization: Token $RTD_API_TOKEN" https://app.readthedocs.com/api/v3/projects/anyscale-ray/versions/<PR>/builds/`.

3. **Doctest (only for files the doctest rule includes)** — the RtD html builder does **not** execute testcode/doctest; that runs in the Buildkite doctest targets. **A docs-only PR doesn't run them premerge by default.** Since ray-project/ray#66806 and #66809, `.buildkite/test.rules.txt` routes `doc/source/*.md` and `*.rst` to `doc_example`, which only the label-gated `doc: docs example tests (opt-in)` step listens for. Add the `docs-example-test` and `go` labels to run each changed page's own target (`//doc:source/<path>`) on premerge; microcheck never runs it. Without the labels, the premerge evidence is the parity check below plus a local run. For a local run, use a scratch venv with `pytest==7.4.4` (Ray's pin, because pytest 9 breaks `pytest-sphinx` collection with `'SphinxDoctestTextfile' object has no attribute 'getini'`), the plugin, the Ray nightly wheel, and whatever the examples import, such as `numpy` or `torch` (`objects/serialization`). Use Python 3.10: on 3.11, `actors/async_api` fails in `asyncio.wait` on an `ObjectRef`, identically for the master `.rst`. Export `RAY_USAGE_STATS_PROMPT_ENABLED=0`, or the nightly wheel's usage-stats banner lands in the first captured output and fails every `testoutput` comparison (`RAY_USAGE_STATS_ENABLED=0` alone swaps it for a "disabled" banner). Some pages need hardware: `scheduling/accelerators` fails on a missing `CUDA_VISIBLE_DEVICES` without a GPU, and for those the parity check is the evidence. Then, from the repo root, run `PYTHONPATH=$PWD python -m pytest --doctest-modules "--doctest-glob=*.md" --disable-warnings -v -c NO_PYTEST_CONFIG -p bazel.default_doctest_pytest_plugin <files>`, the same arguments the `doctest` macro passes. Confirm the real executed blocks pass and the illustrative ones don't run. MyST `{testcode}` in `.md` *is* exercised — `getting-started.md` and `configure-manage-dashboard.md` are tested `.md` precedents. A `{doctest}` fence runs only through the stock parser, which is why Hard rule 4 rewrites it. In a stack, a docs-only PR skips these steps, but a PR that also touches library source runs them. A docstring edit in `python/ray/data/` carries the `data` tag, and on the ray-project/ray#66375 stack that's the first PR where `data: docs example tests` ran and caught a page converted five PRs lower.

   **Before pushing, prove coverage didn't change.** The doctest targets run pytest with Ray's [`pytest-sphinx`](https://github.com/ray-project/pytest-sphinx) fork, which yields **one pytest item per file**, so `pytest --collect-only` counts files, not blocks. What executes is the list of `doctest.Example`s that the plugin's `docstring2examples` builds per file. That's one per `testcode` (paired with its `testoutput`), minus blocks whose `:skipif:` is true. `testsetup` and `testcleanup` are parsed but never run, and so are `doctest` sections. On a `.md`, the stock parser from Hard rule 4 builds a second item, which the parity check reports on its own `FENCE` or `INFO` line. `scripts/doctest_parity.py` runs that parser over each converted `.md` and over the `.rst` it replaced, fetched from a git ref so it works after the `git rm`. It asserts identical sections (directive, body, `:options:`, `:skipif:`) and examples, and exits 1 on any difference. It needs no Bazel and no Ray install, only `pytest` and the plugin in a scratch venv (the install line is in its docstring). from the repo root:

   ```bash
   python3 $SKILL/scripts/doctest_parity.py doc/source/core/actors/named-actors.md [...]   # parity; --base defaults to upstream/master
   python3 $SKILL/scripts/doctest_parity.py --census doc/source/core                      # per-file baseline for a whole tree
   ```

   Re-run parity after the soft-wrap commit and record the before and after example counts on the PR. Because the parity check proves the plugin builds identical examples from both files, a passing local run on the `.md` shows the plugin's item behaves like master. The stock parser's examples have no master baseline, so only the local run covers them. When a block fails locally, run the master `.rst` too before blaming the conversion. On #66380, both failed identically on a missing `numpy` in the venv. `--census` counts every file under the directory, so count only the pages `doc/BUILD.bazel` lists in a `doctest_each()` call; each listed page is its own target now, and `doctest[core]` no longer exists.

4. **Regression — run this, don't eyeball it.** Compare the RtD preview against **`/en/master`** (not `/en/latest`) with [`render_diff.py`](scripts/render_diff.py), which fetches both, extracts `<article>`, normalizes the host, release string, and search-highlight params, and diffs:

   ```bash
   python3 $SKILL/scripts/render_diff.py \
       https://anyscale-ray--<PR>.com.readthedocs.build/en/<PR>/ \
       core/key-concepts.html cluster/key-concepts.html
   ```

   **This is not optional, and a green step 2 is not a substitute for it.** Steps 1–3 are all source-side or resolution-side; this is the only step that looks at output, and it's the only one that catches Hard rule 5 or the `nested_parse` degradation below. On the first batch it caught three regressions — a lost `<title>` on the site root, an `alt=""` image, a dropped `code` class — through a build that was green and silent on all three.

   The script filters the two differences every MyST page shows against an RST page, both inert: `class="tex2jax_ignore mathjax_ignore"` on the root `<section>`, and the missing `code` class on inline literals. Pass `--keep-benign` to see them.

   Read the surviving diffs rather than trusting the exit code. Byte-identical is not always the right bar — a caption-less `{figure}` or a deliberate alt-text addition shows up here too. The question is whether every diff is explainable, not whether every diff is empty. Note that `ray-overview/examples.html` differs between *any* two builds: `custom_directives.py` picks its gallery icons with `random.randint`.

---

## Verified Ray-specific facts (as of mid-2026)

- `doc/source/conf.py`: `default_role = "code"`; `myst_enable_extensions` includes `colon_fence` and `html_image` but **not** `linkify` or `substitution`; `myst_heading_anchors = 4` (so `[text](#slug)` resolves to any h1–h4 heading).
- `doc/BUILD.bazel` tests prose with `doctest_each()` over explicit per-library page lists plus a `team:none` list, one target per page, and no prose globs (ray-project/ray#66809, #67003). The `doc_prose_doctest_targets` lint keeps the lists and `UNTESTED` in `doc/test_prose_doctest_targets.py` in sync with which pages have a runnable example.
- `pre-commit` has no active hook that lints `doc/source/**/*.md` (the `doc/source/data/` vale hook is commented out for now) — so pre-commit passing is not evidence the page is correct; the Sphinx build is.
- `sphinx_design==0.7.0` (`doc/requirements-doc.txt`) supports MyST first-class: its own docs are MyST and it ships a `snippets/myst/` tree, and its directives register through `app.add_directive`, so MyST's `{name}` fence dispatch reaches them like any other directive.
- `doc/source/_ext/callouts.py` defines `callout` and `annotations`, used by exactly one page (`tune/index.md`). Its `<1>`-to-① substitution happens in two independent places: `_replace_numbers()` for the annotation text, and the `CalloutIncludePostTransform` pass for the code in `literal_block`s. Both work under MyST, because `nested_parse` still hands the directive a docutils `StringList` and `StringList.replace()` mutates in place. Don't "fix" `_replace_numbers()` on the strength of its discarded return value — the mutation already happened. Its `content: str` type hint is wrong, though, and passing it an actual `str` would silently no-op, since Python strings are immutable.
- The `ray-contribute/` directory was the first batch fully migrated (precedent for every pattern above, including sphinx-design tabs/dropdowns in `development.md` and the shared-include + partial handling in `getting-involved.md` / `involvement.md`).

---

## Gotchas

- **Heading level surprises are usually faithful, not bugs.** If a section renders one level deeper than feels right, the RST adornment order put it there. Reproduce it; don't fix it in a conversion PR.
- **Factual errors you find while converting go in a separate PR.** A wrong claim, a stale example output, or a code bug that a doctest exposes isn't part of a faithful conversion. Convert the page as it stands, and put the corrections in their own PR, grouped by reviewing team and titled as a correctness change, so the owning engineers sign off on them. Translating a link form that breaks under MyST (Hard rule 2) is still part of the conversion.
- **Two `#` (H1) headings in one page** is fine when the RST had two top-level (`=`) sections.
- **Trailing whitespace inside code blocks** can be dropped (invisible, no linter on these files) — don't preserve it deliberately.
- **A converted file listed in `doc/BUILD.bazel` that you forgot to update** fails the `doc_prose_doctest_targets` lint, and otherwise would silently drop the page's doctest coverage. Always re-grep `doc/BUILD.bazel` and `doc/test_prose_doctest_targets.py` for the stem.
- **A sibling's *relative* link to the file you're renaming** (`[text](./page.rst)`) is the easiest reference to miss — it has no `dir/` prefix, so a path-scoped grep won't catch it. Grep the bare stem across all of `doc/`.
- **`.html#anchor` self-links and bare `.md`→`.rst` links** are faithful-but-wrong: they render in RST but fail `fail_on_warning` in MyST. Translate them (Hard rule 2).
- **An extensionless link to a doc that also carries a same-named label is ambiguous** (`myst.xref_ambiguous`), not missing — MyST can't tell the doc from the label. Use `{doc}` for whole-doc links so it always resolves to the document.
- **Don't trust pre-commit's silence** as a quality signal for these `.md` files — it skips them.
