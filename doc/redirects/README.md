# Read the Docs redirects for docs.ray.io

`current.yaml` and `master.yaml` in this directory are the source of truth for
the HTTP redirects configured on the `anyscale-ray` Read the Docs project, which
serves docs.ray.io. They're managed with
[rtd-redirects](https://github.com/anyscale/rtd-redirects) and mirror the live
configuration exactly.

## Two files: immediate and release-gated

- **`current.yaml`** holds redirects that apply now, to `/en/latest/` and, for
  `type: page` rules, every published docs version. This is the large curated
  set, and most redirects belong here.
- **`master.yaml`** holds release-gated redirects scoped to the staged
  next-release docs at `/en/master/`. Use it when a redirect should go live on
  the next-release docs but must not change `/en/latest/` or any published
  version yet. Between release cycles its resting state is an empty
  `redirects: []`.

The two files compose into one ordered redirect set, `master.yaml` first
(earlier files match first). That ordering indexes `master.yaml`'s exact
`/en/master/` rules ahead of `current.yaml`'s broad `page` and wildcard rules,
so an exact rule wins for its `/en/master/` URL. CI validates and applies the
composed set; the compose order is spelled out in `.buildkite/doc.rayci.yml`.

## Adding or changing a redirect

1. Pick the file and type for what the redirect should do:
   - Apply to `/latest` and every version, now: add a version-less `type: page`
     rule to `current.yaml`. Point `to` at the final destination, not at another
     redirect's source.
   - Apply to the next-release docs only: add a `type: exact` rule to
     `master.yaml` with fully qualified `/en/master/` `from:` and `to:` paths, so
     it fires only on the master version and leaves `/latest` untouched.
2. Validate locally. No Read the Docs credentials needed:

   ```
   rtd-redirects validate --composed doc/redirects/master.yaml doc/redirects/current.yaml
   ```

3. Open a PR. After it merges, CI applies the composed set to the live project
   automatically. A postmerge Buildkite step runs
   `rtd-redirects apply --project anyscale-ray --file doc/redirects/master.yaml doc/redirects/current.yaml --strict`
   when the merge touches `doc/redirects/`. No manual apply step is needed.

## Release day

When the next-release docs are promoted and `/en/master/` becomes `/en/latest/`,
fold `master.yaml` into `current.yaml` so its rules survive the version cutover:

1. Move each entry from `master.yaml` into `current.yaml`, rewriting `/en/master/`
   in `from:` and `to:` to `/en/latest/` where that is the intended stable
   behavior. An entry that should keep pointing at the master version stays as-is.
2. Reset `master.yaml` to an empty `redirects: []` for the next cycle.
3. Validate the composed set:
   `rtd-redirects validate --composed doc/redirects/master.yaml doc/redirects/current.yaml`.
4. Open a PR. The postmerge step applies the change on merge.

## Policy

- Change redirects by editing these files in a pull request, not in the
  Read the Docs dashboard. Dashboard edits are out-of-process: they aren't
  reviewed, and the next reconciliation overwrites them.
- If an urgent fix has to land through the dashboard, record why in the PR
  that follows, and open that reconcile PR within 24 hours. `rtd-redirects dump
  --project anyscale-ray -o doc/redirects/current.yaml` regenerates the full set
  from live state into `current.yaml`, but it can't split the master-gated rules
  back out, so run it only when `master.yaml` is empty, or re-split afterward.

## Auditing drift

```
rtd-redirects plan --project anyscale-ray --file doc/redirects/master.yaml doc/redirects/current.yaml
```

shows any difference between the composed set and the live configuration. An
empty plan means no drift.

The ruleset began as a May-June 2026 cleanup that reduced 287 inherited rules
to a curated set of 169 version-agnostic `page` rules plus 3 intentional
version-pinned `exact` rules, all returning 301. The pre-cleanup snapshot is
preserved in git history. A later legacy-version 404-coverage pass
added 21 `page` catch-all rules that land high-traffic out-of-support docs
paths, which have no equivalent on current docs, on the nearest surviving
section index.
