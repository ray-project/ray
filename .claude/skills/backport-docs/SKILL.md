---
name: backport-docs
description: Cherry-pick documentation changes from master onto a release branch so they appear on the docs.ray.io /latest build
---

# Backport docs to a release branch

`docs.ray.io/en/latest` is built from the newest `releases/X.Y.Z` branch, **not**
from `master`. A docs change merged to `master` shows up only on
`docs.ray.io/en/master` until it's cherry-picked onto the release branch. Use this
skill to get already-merged docs onto `/latest`.

Throughout, `<remote>` is the remote that points at `ray-project/ray`. Derive it
from `git remote -v` — it's `origin` in a direct clone and `upstream` in a clone
that started as a fork. Don't assume.

## 1. Find the release branch that `/latest` serves

```bash
git ls-remote --heads <remote> 'refs/heads/releases/*' | sort -t/ -k4 -V | tail -5
```

The highest `releases/X.Y.Z` version is what `/latest` tracks. Fetch it:

```bash
git fetch <remote> releases/X.Y.Z
```

## 2. Identify what's missing on `/latest`

You usually start from a set of already-merged master commits or PRs (for example,
the docs behind a release blog post). For each candidate, check whether it — or
equivalent content — is already on the release branch:

- **File missing entirely:**
  `git cat-file -e <remote>/releases/X.Y.Z:doc/source/<path>.md`
- **File present but content differs:**
  `git diff <remote>/releases/X.Y.Z..<remote>/master -- doc/source/<path>.md`
- **Which master commits touch a page (newest first):**
  `git log --oneline --no-merges <remote>/releases/X.Y.Z..<remote>/master -- doc/source/<path>.md`

## 3. Check for prior backports (avoid duplicates)

Equivalent content is often already on the release branch under a **different SHA**
(a prior cherry-pick). Re-applying it will conflict or produce an empty commit.
Before picking a PR, search the release-branch history for it:

```bash
git log --oneline <remote>/releases/X.Y.Z --grep "#<PR_NUMBER>)"
```

If it's already there, skip that commit. Confirm with a file diff
(`git diff <remote>/releases/X.Y.Z..<remote>/master -- <path>`); an empty diff means
the page is already up to date on `/latest`.

Also check nothing is already in flight:

```bash
gh pr list --repo ray-project/ray --state open --base releases/X.Y.Z
gh pr list --repo ray-project/ray --state open --search "<PR_NUMBER> in:title,body"
```

## 4. Cherry-pick onto a worktree of the release branch

```bash
git worktree add -b <branch> .worktrees/<branch> <remote>/releases/X.Y.Z
cd .worktrees/<branch>
```

Apply the chosen commits in **chronological (oldest-first)** order, preserving
provenance (`-x`) and DCO sign-off (`--signoff`, required — see
`doc/source/ray-contribute/agent-development.md`):

```bash
git cherry-pick -x --signoff <sha1> <sha2> ...
```

**Keep the backport tight.** Cherry-pick only the feature/fix commits. Leave out
broad, non-feature commits that happen to touch the same files (site-wide
frontmatter/SEO passes, terminology renames, tooling like vendored references).
Their hunks will remain as harmless residual diffs against master.

### Resolving conflicts

Conflicts here almost always come from an **excluded** commit that the picked
commit carried as adjacent context (for example, `html_meta` frontmatter the
release branch doesn't have). Resolve toward the release branch's state for those
excluded hunks, and keep only the feature substance. If the whole page turns out to
be already backported (step 3), `git cherry-pick --skip` it.

## 5. Verify the build won't break

The docs build runs with `fail_on_warning: true` (`.readthedocs.yaml`), so an
unresolved cross-reference or a toctree entry pointing at a nonexistent file fails
the build. For every file the backport changed:

- **Cross-references resolve on the branch.** Collect the `{ref}` and `{doc}`
  targets and confirm each label exists:
  ```bash
  git grep -nE "^\(<label>\)=" -- 'doc/source/'   # MyST label
  git grep -nE "^\.\. _<label>:" -- 'doc/source/'  # rST label
  ```
- **New toctree entries point at files that exist** on the branch.
- Sanity-check that each changed file matches master except for the hunks you
  intentionally excluded:
  `git diff HEAD..<remote>/master -- <path>`.

A local docs build (see the "Building the Ray documentation" section of
`doc/source/ray-contribute/docs.md`) is the strongest check before handing off.

## 6. Open the PR

Push the branch to `ray-project/ray` (not a fork, if you have push access) and open
against the release branch:

```bash
git push -u <remote> <branch>
gh pr create --repo ray-project/ray \
  --base releases/X.Y.Z --head <branch> --draft \
  --title "[cherry-pick][X.Y.Z][docs] <summary>" \
  --body-file <body>
```

- Match the release branch's existing title convention: `[cherry-pick][X.Y.Z]...`.
- Open as **draft** — Ray's contribution policy requires a human to review every
  line and run tests before it requests review.
- The PR body must state why it isn't a duplicate, what testing ran, and that AI
  assistance was used (see `AGENTS.md`). Note any commits you deliberately excluded
  and any already-present backports you skipped.
- Keep internal tracking keys out of the PR title, body, and commits.
