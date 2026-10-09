# Read the Docs redirects for docs.ray.io

`current.yaml` is the source of truth for the HTTP redirects configured on the
`anyscale-ray` Read the Docs project, which serves docs.ray.io. The file is
managed with [rtd-redirects](https://github.com/anyscale/rtd-redirects) and
mirrors the live configuration exactly.

## Policy

- Change redirects by editing `current.yaml` in a pull request, not in the
  Read the Docs dashboard. Dashboard edits are out-of-process: they aren't
  reviewed, and the next reconciliation overwrites them.
- If an urgent fix has to land through the dashboard, record why in the PR
  that follows, and open that reconcile PR within 24 hours
  (`rtd-redirects dump --project anyscale-ray -o doc/redirects/current.yaml`
  regenerates this file from live state).

## Adding or changing a redirect

1. Edit `current.yaml`. Prefer `type: page` rules with version-less paths,
   and leave `force` unset. When you add a rule, point `to` at the final
   destination, not at another redirect's source. When a page that existing
   rules point at moves, see
   [Moving or renaming pages](#moving-or-renaming-pages).
2. Validate locally: `rtd-redirects validate doc/redirects/current.yaml`.
   No Read the Docs credentials needed.
3. Open a PR. After it merges, CI applies the change to the live project
   automatically. A postmerge Buildkite step runs
   `rtd-redirects apply --project anyscale-ray --file doc/redirects/current.yaml --strict`
   on each postmerge run of master, and the change goes live on the next run
   after the merge, which can be a few hours later. No manual apply step is
   needed.

## How redirects apply across versions

A rule with the default `force: false` fires only when the requested URL would
otherwise return 404. A `type: page` rule applies to every docs version, and
each version resolves the rule's `to` path within that version. Together,
these properties let one version-less rule behave correctly on every version:

- On a version where the old path still exists, such as `/en/latest/` before
  a release or any older release, the page renders and the rule doesn't fire.
- On a version where the old path is gone, such as `/en/master/` after a move
  merges, the rule fires and sends the reader to the new path in that same
  version.

Each release that ships a change picks up its redirect when the old path stops
existing in that release. The redirect needs no release-day edit to this file.

Set `force: true` only to redirect a page that still exists. A forced rule
fires on every version it matches, including `/en/latest/` and older releases.

## Moving or renaming pages

When a PR moves or renames pages under `doc/source/`, add a version-less
`page` rule from each old path to its new path in the same PR. For a
directory, one wildcard rule covers every page under it:

```yaml
- from: /old-dir/*
  to: /new-dir/:splat
  type: page
```

Don't repoint existing rules whose `to` points under the moved path. Older
releases still serve the old path, so an existing rule that points there
resolves directly on those versions. On versions that include the move, the
request takes a second hop through the new rule and lands on the moved page.
Repointing the existing rule to the new path would send readers of older
releases to a page that doesn't exist in their version.

Do move the `from` of a catch-all rule that matches under the old path, such
as `/old-dir/examples/*`, to the new path. A catch-all is more specific than
the directory rule, so it sits ahead of it and matches first. Left on the old
path, it would catch moved pages before the directory rule could send them to
their new location.

`rtd-redirects validate` reports each two-hop path as an info-level chain
note. It counts these notes and lists them only with `--show-info`. Chain notes
from a move are expected. A chain is a warning only when the second rule sets
`force: true`, because that chain happens on every version. Neither blocks CI,
which fails only on error-level findings.

## Auditing drift

`rtd-redirects plan --project anyscale-ray --file doc/redirects/current.yaml`
shows any difference between this file and the live configuration. An empty
plan means no drift.

The ruleset began as a May-June 2026 cleanup that reduced 287 inherited rules
to a curated set of 169 version-agnostic `page` rules plus 3 intentional
version-pinned `exact` rules, all returning 301. The pre-cleanup snapshot is
preserved in git history. A later legacy-version 404-coverage pass
added 21 `page` catch-all rules that land high-traffic out-of-support docs
paths, which have no equivalent on current docs, on the nearest surviving
section index.
