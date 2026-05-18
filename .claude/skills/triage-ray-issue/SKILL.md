---
name: triage-ray-issue
description: Operate on the link between a `ray-project/ray` GitHub issue and a Jira `DOC` ticket. Use when the user provides a `DOC-XXX` key tied to an OSS Ray issue (resolve mode → fix PR in the Ray repo that auto-closes the GH issue and back-links the DOC ticket), or pastes / refers to a `ray-project/ray` GH issue with the `docs` label that needs triaging into a DOC ticket (triage mode). The skill assumes a stable 1:1 DOC↔GH mapping enforced by the `OSS Ray` Jira component.
---

# Triage / resolve a Ray OSS docs issue

Two modes, chosen by what the user gives you:

- **Resolve mode** — input is a `DOC-XXX` key. Output: a fix PR in `ray-project/ray` whose body carries the markers that auto-close the linked GH issue and transition the DOC ticket.
- **Triage mode** — input is a `ray-project/ray` GH issue (URL or `#NNNN`), or a request to triage the docs queue. Output: a new DOC ticket under the right epic, or a justified non-action.

If the user's input is ambiguous (e.g. a bare number that could be either), ask which mode.

The 1:1 schema below is the contract between the two modes. Both produce or consume tickets that match it; nothing else needs to know about a one-time audit doc to operate.

---

## DOC ↔ GH ticket schema

Every DOC ticket created for an OSS Ray issue has:

- **Project / type:** `DOC` / `Task`.
- **Component:** `OSS Ray`. The component carries the default assignee and board routing — do not set assignee by hand.
- **Parent (epic link):** one of the active OSS Ray epics. Discover them at runtime:

  ```
  project = DOC AND issuetype = Epic AND component = "OSS Ray" AND statusCategory != Done
  ```

  Don't hardcode the list; epics get added and closed. Filter the live list by topic to pick the parent. Current sweep epics cover broken-link, theme/CSS, API docstring + ref-page, and good-first-issue work; current per-library epics cover Core, Serve, Train, Tune, RLlib, Data, Cluster/KubeRay/VM, Dashboard/observability. New buckets (e.g. a future "deprecation residue" sweep) need a new epic — ask the user before creating one.

- **Title:** `Ray docs (ray#<NNNN>): <short specific summary>`.
- **Description (markdown):**

  ```markdown
  Closes [ray-project/ray#<NNNN>](https://github.com/ray-project/ray/issues/<NNNN>).

  ## Problem

  <1-3 sentences. State the concrete defect verified against current ray master, not the user-reported symptom. Cite file:line where the broken thing lives.>

  ## Action

  <Imperative. Name the target file under doc/source/... (or python/ray/...) and the specific edit. "Update X", "Remove Y", "Replace Z with W". Avoid hedges.>

  ## Auto-close mechanic

  The fix PR in `ray-project/ray` must include `Closes ray-project/ray#<NNNN>` in the PR description. Include `[DOC-XXX]` (this ticket's key) in the same PR description so Jira renders the back-link.
  ```

  (An `## Audit source` footer is optional and useful when a ticket originated from a one-time audit; new triage doesn't need it.)

- **Priority:** P2 unless the issue body, labels, or maintainer comments justify higher.

---

## Resolve mode (DOC-XXX → fix PR)

### 1. Pull the ticket

Use Atlassian `getJiraIssue` on the `DOC-XXX` key. Extract:

- The GH issue number (`<NNNN>`) from the `Closes [ray-project/ray#NNNN](…)` line in the description.
- The target file from the **Action** section.
- Any verification notes from **Problem**.

If the description is missing those markers, stop and tell the user — the ticket isn't in resolve-ready shape.

### 2. Pre-flight: is the defect still real?

GH issues stay open long after the underlying defect is fixed — a different PR may have removed the page, restructured the section, or replaced the broken link as a side effect. Verify before paying the worktree cost (worktree creation copies the full Ray checkout, ~10k files).

First check GH state:

```bash
gh issue view <NNNN> --repo ray-project/ray --json state,title
```

If `CLOSED`, stop.

Then confirm the broken thing named in the Action still exists in `upstream/master`. From any existing checkout of the repo (don't create the worktree yet):

```bash
git fetch upstream master --quiet
# Broken URL fragment or anchor name:
git grep "<broken-fragment>" upstream/master -- <target-file>
# Removed/renamed file:
git cat-file -e upstream/master:<target-file> 2>/dev/null && echo present || echo absent
```

If the grep returns no hits, or the named target file is absent, the defect is gone. Do **not** open a stale PR. Instead:

1. Find the resolving PR with `git log upstream/master --oneline -S "<broken-fragment>" -- <target-file>` (pickaxe) or `git log upstream/master --oneline --grep "#<NNNN>"` (commit-message reference).
2. Close the GH issue citing it: `gh issue close <NNNN> --repo ray-project/ray --reason completed --comment "Fixed in #<RESOLVING_PR> ..."`.
3. Transition the DOC ticket to Done via the Atlassian `transitionJiraIssue` tool, with a comment pointing at the resolving PR.
4. Stop. Skip the worktree.

This step caught ~40% of one batch of broken-link tickets; the cheap pre-flight is the right place for it.

### 3. Worktree

Per the global worktree rule, create a worktree under `.claude/worktrees/` named after the ticket. Branch name and directory must match exactly; use hyphens, never slashes:

```bash
git worktree add -b doc-XXX-<short-desc> .claude/worktrees/doc-XXX-<short-desc>
```

`<short-desc>` is 2-4 hyphenated words that summarize the fix (`fix-bazel-redhat-link`, `remove-zoom-recording`, …).

Do all subsequent work in the worktree directory.

### 4. Make the edit

Apply the Action from the ticket. Guardrails:

- **Broken links:** before swapping, hit the proposed new target (`curl -sI` or `gh api` for repo paths) and confirm it resolves.
- **Removed examples / files:** prefer linking to a current alternative over deleting the section outright. Mention the alternative in the commit body.
- **Anchor changes:** if you change a Sphinx label, also grep `doc/source/` and `rllib/` for inbound references and fix them in the same commit.
- **Scope:** stay inside the Action's named target. If you find adjacent rot, note it for a follow-up ticket rather than expanding scope.

### 5. Lint

Run `/lint` on the modified files. Pre-commit fixes whitespace, line length, and a few other things automatically; resolve anything else by editing the source.

### 6. Commit (signed off)

Single commit. Stage by name, never `git add -A`. Sign off with `-s` so the commit carries a `Signed-off-by:` trailer alongside the Co-Authored-By trailer:

```bash
git commit -s -m "$(cat <<'EOF'
[Docs] <short imperative summary> (#<NNNN>)

<1-3 sentences: what was wrong, what the fix is, any cross-link or
alternative chosen. Mention the target file once.>

Closes ray-project/ray#<NNNN>

[DOC-XXX]

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

The `[Docs]` prefix matches the Ray repo's commit-message convention (`[<area>] <subject>` — see `git log --oneline` for examples like `[RLlib]`, `[core]`, `[Data]`).

### 7. Push and open the PR

```bash
git push -u origin doc-XXX-<short-desc>
gh pr create --repo ray-project/ray --title "[Docs] <subject> (#<NNNN>)" --body "$(cat <<'EOF'
## Description

<2-4 sentences echoing the commit body. State the defect and the fix.>

## Related issues

Closes ray-project/ray#<NNNN>

[DOC-XXX]

## Additional information

<Optional: target file path, alternative chosen if a section was removed,
or any cross-link the reviewer needs.>

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

The body follows the headings in `.github/PULL_REQUEST_TEMPLATE.md`; check that file before opening the PR in case the template changes.

Both markers — `Closes ray-project/ray#<NNNN>` and `[DOC-XXX]` — must appear in the PR body literally. The first is what GitHub uses to auto-close the issue on merge; the second is what Jira's GitHub integration uses to render the back-link and transition the DOC ticket. Keep them on their own lines.

Confirm the PR URL with the user and stop. Don't push further commits or merge — the Ray maintainers review and merge.

---

## Triage mode (GH issue → DOC ticket)

This mode is the on-ramp for new docs-labeled OSS issues. It does not require a pre-existing audit doc.

### 1. Read the issue

```bash
gh issue view <NNNN> --repo ray-project/ray --json number,title,state,labels,author,body,createdAt
```

If `CLOSED`, stop and tell the user.

### 2. Check for an existing DOC ticket

```
project = DOC AND text ~ "ray#<NNNN>"
```

If one exists, report its key and status and stop. Don't duplicate.

### 3. Classify

Walk this decision tree on the issue body + labels:

| Signal | Route |
|---|---|
| Concrete, bounded docs edit (broken link, wrong number, missing docstring, missing one-page section) with a clear target file or symbol | Triage to DOC ticket — auto-triageable. |
| Real bug masquerading as docs (test fails, runtime error, API doesn't behave as documented) | Don't file a DOC ticket. Comment on the GH issue routing it to the right area, and recommend re-labeling (`bug`, `core`, `serve`, etc.). |
| Question or RFE | Don't file. Recommend `question` or `enhancement` label. |
| Needs canonical-behavior decision from engineering before docs can write anything | File the DOC ticket but mark it as needing engineering input; pick the per-library epic so it lands in the right team's view. |
| Issue describes feature/area that's deprecated or already removed | Don't file. Comment on the GH issue with the deprecation evidence and request closure. |

For broken-link tickets specifically, before filing, grep current master for the broken target so the Action line names a real file:

```bash
git grep -n "<broken-url-or-anchor>" doc/source/ rllib/
```

If the broken target is already gone in master, comment on the GH issue and stop — no DOC ticket needed.

### 4. Pick the parent epic

Query the live epic list:

```
project = DOC AND issuetype = Epic AND component = "OSS Ray" AND statusCategory != Done
```

Match the issue to a sweep epic if it fits a sub-cluster (broken links, theme/CSS, API docstrings, good-first-issue). Otherwise match by library (Core, Serve, Train, …). If nothing fits, ask the user before creating a new epic.

### 5. Create the ticket

Use Atlassian `createJiraIssue` with the schema in [DOC ↔ GH ticket schema](#doc--gh-ticket-schema). Set `component = "OSS Ray"`, parent = the chosen epic. Don't set assignee — the component handles it. After creation, report the new key and URL.

---

## When NOT to use this skill

- The user is fixing a broken link they noticed themselves, no GH issue or DOC ticket. Just edit; don't add Jira overhead.
- The issue is a core/runtime bug, not docs. Route to the right team; don't file under `DOC`.
- The GH issue is already `CLOSED`. Confirm with the user before doing anything.
- The fix touches code semantics, not just documentation, and would need engineering review beyond CODEOWNERS for docs. File the DOC ticket but don't ship a fix from this skill — flag for engineering.

---

## References

- Worktree + branch naming: `~/.claude/CLAUDE.md`.
- Linting and rebuilds: `/lint`, `/rebuild`.
- Active OSS Ray epics: query Jira (see above). For the historical seeding of the current backlog, see [`anyscale/docs` → `strategy/ray-docs/oss-issue-audit-2026-05.md`](https://github.com/anyscale/docs/blob/master/strategy/ray-docs/oss-issue-audit-2026-05.md) — used once to populate DOC-950 onward; not required for routine operation.
