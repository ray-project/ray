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
only *resolution* uses the local snapshot, so link targets are unaffected.

## Refreshing

Snapshots drift from upstream between refreshes (the same staleness that exists
today between fetches, just moved into our control). Refresh them:

- **on the [DOC-932] dependency-upgrade cadence**, alongside the periodic docs
  dependency bumps — an upstream version bump is exactly when its public API
  surface, and therefore its inventory, changes; and
- **ad hoc** whenever a valid cross-reference to an upstream project stops
  resolving (a Sphinx `py:obj reference target not found`-style warning for an
  external symbol that does exist upstream).

Run the refresh script from the repo root, inside the docs virtualenv:

```bash
python doc/source/_intersphinx/refresh.py              # refresh all
python doc/source/_intersphinx/refresh.py numpy torch  # refresh a subset
```

Then review the diff and re-run the docs build before committing. The script
reads the project list and upstream URLs straight from `_intersphinx_targets`
in `../conf.py`, so it never drifts from the build configuration. To add or
remove a target, edit `_intersphinx_targets` and re-run the script.

[DOC-932]: https://anyscale1.atlassian.net/browse/DOC-932
