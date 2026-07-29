# Intersphinx inventory snapshots

This directory holds committed snapshots of the third-party Sphinx inventories
(`objects.inv`) that Ray's docs cross-reference (NumPy, PyTorch, pandas, and
~two dozen others).

## Why these are committed

Without snapshots, every Sphinx build fetches every inventory over the network
before it can resolve a single cross-reference. That is slow (~20–60s) and
occasionally flaky — a couple of inventories are served through GitHub
release-asset redirects to signed, expiring blob-storage URLs, which are the
most fragile part of the build's startup and a plausible cause of intermittent
timeouts.

`intersphinx_mapping` in [`../conf.py`](../conf.py) is built to list the **local
snapshot first** and the upstream location second. Sphinx uses the first
inventory that loads, so:

- when the snapshot is present, the build resolves references from disk with
  **no network fetch**; and
- if a snapshot is ever missing or unreadable, Sphinx transparently falls back
  to the upstream URL (logged as an info message, *not* a build-breaking
  warning).

Generated cross-reference links still point at each project's live docs site —
only *resolution* uses the local snapshot, so the URLs Sphinx emits are the same
ones it emits today. What the snapshot does affect is *which* references resolve
at all; see the next section.

## What staleness costs

Every target in `_intersphinx_targets` points at upstream's *moving* docs —
`.../stable/`, `.../latest/`, `.../main/`, or an unversioned root. None is
pinned to a version. An inventory therefore changes when the upstream project
*releases*, independently of anything Ray pins in its own requirements.

So these snapshots must be refreshed on a clock, and the refresh cadence is what
bounds two failure modes:

- upstream **adds** a symbol that Ray's docs then cross-reference — the
  reference fails to resolve and breaks the `-W` build. Loud, self-announcing,
  fixed by refreshing; and
- upstream **removes or renames** a symbol Ray's docs already reference — a
  stale snapshot still resolves it, so the build stays green and emits a link to
  a page that no longer exists upstream. **Silent.** Ray's docs CI does not run
  Sphinx's `linkcheck` builder, so nothing else catches this.

The second is why refreshing is not optional. The refresh itself is what
surfaces it: once the fresh inventory no longer carries the removed symbol, the
stale cross-reference fails on the refresh PR's `-W` build, which is exactly the
signal we want. **A refresh must therefore land as a reviewed PR — never
auto-merged** — because that `-W` build is the safety net.

## Refreshing

A scheduled monthly job refreshes these snapshots and opens a PR when any of
them has drifted; the Read the Docs `-W` build on that PR is the gate. That
bounds staleness at roughly a month without depending on anyone remembering.
See [DOC-1050] for the job's ownership.

Refresh by hand whenever you need to — after adding a target, or when a
cross-reference to an upstream symbol that does exist upstream stops resolving.
Run it from the repo root, inside the docs virtualenv:

```bash
python doc/source/_intersphinx/refresh.py              # refresh all
python doc/source/_intersphinx/refresh.py numpy torch  # refresh a subset
```

Then review the diff and re-run the docs build before committing. The script
reads the project list and upstream URLs straight from `_intersphinx_targets`
in `../conf.py`, so it never drifts from the build configuration. To add or
remove a target, edit `_intersphinx_targets` and re-run the script.

[DOC-1050]: https://anyscale1.atlassian.net/browse/DOC-1050
