---
name: data-pr
description: Open a Ray Data pull request — rebase and duplicate checks, an independent edge-case review by a fresh subagent, a self-review against Ray Data's rules, lint and tests, a signed commit, a description to Ray Data's AGENTS.md template, and the author's approval before anything is pushed. Use when asked to raise, open or create a Ray Data PR, push a branch for review, self-review a branch before review, or write a PR description.
---

# Opening a Ray Data PR

Ray's root `AGENTS.md` and `python/ray/data/AGENTS.md` set the contribution
policy and the description format; this skill adds the review steps. `<master>`
is ray-project/ray's master as your remote names it (e.g. `upstream/master`);
`<base>` is `<master>`, or the parent branch for a stacked PR. Nothing is
pushed, opened or posted before the author approves the exact text (step 6).

## 1. Before starting
- **Root `AGENTS.md`'s duplicate-work checks pass.**
- **Rebased on current master.** For a stacked branch whose parent
  squash-merged: `git rebase --onto <master> <old parent head>`.

## 2. Independent edge-case review
Start a fresh agent in the background, then do step 3 while it runs: `Agent`
with `subagent_type: "general-purpose"`, `model: "opus"`,
`run_in_background: true`, never `fork`. It gets only the prompt below, nothing
about why the change is right, so it doesn't share the author's blind spots.

```
Review an unpublished Ray change: repo <repo root>, `git diff <base>...<branch>`,
commit subject "<subject>". Read any code you need.
Find inputs, states and environments where it gives wrong results, fails, or
uses far more time or memory than needed: empty, huge and duplicate inputs,
nulls, mismatched schemas, partial failure and retries, pickling to workers,
the oldest supported dependencies, remote storage. For each new test: does it
still pass with the change reverted?
Don't edit the repo, switch branches, install, or run test suites, benchmarks
or fetches. You may run small repro scripts from a temp directory outside the
repo, but only if `python -c "import os, ray.data; print(os.path.realpath(ray.data.__file__))"`
prints a path inside this checkout; otherwise reason from the code.
Per finding: input, actual vs expected, ran (script + output) or reasoned, how
likely. Ran ones first; say so if you found nothing. Under 800 words.
```

Rerun a finding's repro before acting on it; drop what "Not worth flagging"
covers.

## 3. Check the diff against the rules
Read every file in `python/ray/data/.claude/rules/`, and check each rule there
and in "Scope" below against `git diff <base>...HEAD`. Report failures together
with the reviewer's verified findings, biggest first: `file:line`, rule, fix.
Fix only what the author agrees to.

### Scope
- **One PR, one behavior.** A second behavior, or a change to shared code or
  to code another team owns, gets its own PR.
- **Only change what the PR needs:** no "consistency" edits to code nothing
  calls. Exception: a public method the change would otherwise leave wrong.
- **No new public parameter that an existing API covers.**
- **Check whether a library already does it** before hand-writing it.

### Not worth flagging
- Guards for states that can't happen.
- Costs measured as small.
- Rare cases with a documented trade-off.
- Moving correct shared code.
- Removing a correct change that keeps a public API right.

## 4. Lint and test
- **Lint** every file the branch changes, as the repo's `lint` skill does:
  `pre-commit run --files $(git diff --name-only --diff-filter=d <base>...HEAD)`.
  Its `pyrefly (ray data)` hook uses CI's pyrefly version and exclusions, but
  not CI's installed packages, so CI can still report import-related errors.
- **Test** every module the diff touches, on the final head after the last
  rebase: `python -m pytest python/ray/data/tests/<test_file>.py -x`.

## 5. Commit
`[Data]` prefix, signed as root `AGENTS.md` requires, including
`git rebase --signoff` and `git cherry-pick -s`: signing after the PR is open
means force-pushing.

## 6. Description and approval
Write the description to `python/ray/data/AGENTS.md`'s template, with root
`AGENTS.md`'s three statements (not a duplicate, tests run, AI assistance).
Every claim matches the final head. Show the author the commit message, the
description and the files changed; push or open nothing until the author
approves that exact text.
