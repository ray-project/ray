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

1. Edit `current.yaml`. Prefer `type: page` rules with version-less paths;
   they apply to every docs version. Point `to` at the final destination, not
   at another redirect's source.
2. Validate locally: `rtd-redirects validate doc/redirects/current.yaml`.
   No Read the Docs credentials needed.
3. Open a PR. After it merges, a maintainer applies the change with
   `rtd-redirects apply --project anyscale-ray --file doc/redirects/current.yaml --strict`.
   CI automation for this step is planned; application is manual for now.

## Auditing drift

`rtd-redirects plan --project anyscale-ray --file doc/redirects/current.yaml`
shows any difference between this file and the live configuration. An empty
plan means no drift.

The current ruleset is the result of a May-June 2026 cleanup that reduced
287 inherited rules to a curated set: 169 version-agnostic `page` rules plus
3 intentional version-pinned `exact` rules, all returning 301. The
pre-cleanup snapshot is preserved in git history.
