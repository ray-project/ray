# Intersphinx inventory snapshots

This directory holds committed snapshots of the third-party Sphinx inventories (`objects.inv`) that Ray's docs cross-reference (NumPy, PyTorch, pandas, and fifteen others).

## Why these are committed

Without snapshots, every Sphinx build fetches every inventory over the network before it can resolve a single cross-reference. That costs roughly 20 to 60 seconds, and it's occasionally flaky. GitHub serves a couple of the inventories through release-asset redirects to signed, expiring blob-storage URLs. Those are the most fragile part of the build's startup and a plausible cause of intermittent timeouts.

[`../conf.py`](../conf.py) builds `intersphinx_mapping` to list the local snapshot first and the upstream location second. Sphinx uses the first inventory that loads, so:

- When the snapshot is present, the build resolves references from disk with no network fetch.
- When a snapshot is missing or unreadable, Sphinx falls back to the upstream URL, logged as an info message rather than a build-breaking warning.

Generated cross-reference links still point at each project's live docs site. Only *resolution* uses the local snapshot, so this change doesn't alter the URLs Sphinx emits. What a snapshot does affect is *which* references resolve at all. See the next section.

## What staleness costs

Most targets resolve against upstream's *moving* docs: `.../stable/`, `.../latest/`, `.../main/`, or an unversioned root. Their inventory therefore changes when the upstream project *releases*, independently of anything Ray pins in its own requirements. There's no Ray-side event to refresh against.

Three targets are the exception, because they set an explicit inventory URL rather than deriving it from `base_url`: `pandas` reads a frozen `object-mirror-*` release asset under `ray-project`, `torch` is pinned to `docs/2.7/`, and `tensorflow` reads a third-party GPflow mirror that tracks its own `master`. For the two frozen ones a refresh is a no-op. They change only when someone re-cuts the mirror or repoints the URL, so committing a snapshot of them changes nothing about their staleness.

On `master`, that leaves a clock as the only thing that can bound staleness, and the refresh cadence is what bounds two failure modes:

- upstream adds a symbol that Ray's docs then cross-reference. The reference fails to resolve and breaks the `-W` build. That one is loud and self-announcing, and refreshing fixes it.
- upstream removes or renames a symbol Ray's docs already reference. A stale snapshot still resolves it, so the build stays green and emits a link to a page that no longer exists upstream. That one is silent. CI does run Sphinx's `linkcheck` builder in the `doc: linkcheck` step, but the step is `skip-on-premerge` and `soft_fail: true`, so it runs only after merge and never blocks. `conf.py` also sets `linkcheck_anchors = False`, so it confirms the target page resolves but not the `#anchor` a symbol-level reference points at.

The second is why refreshing isn't optional. The refresh itself is what surfaces it: once the fresh inventory no longer carries the removed symbol, the stale cross-reference fails on the refresh PR's `-W` build. That's the signal you want, and it's why a refresh must land as a reviewed PR and must never be auto-merged. That `-W` build is the safety net.

On a release branch or tag, the opposite holds: a frozen snapshot is the intended behavior rather than a debt. It pins cross-reference resolution to the release epoch, so rebuilding a release's docs later resolves against the inventories that release shipped with, instead of drifting with upstream on every rebuild the way a live fetch does. The refresh job targets `master` only, which is what preserves that property.

## Refreshing

A scheduled monthly job refreshes these snapshots and opens a PR when any of them has drifted. The Read the Docs `-W` build on that PR is the gate. That bounds staleness at roughly a month without depending on anyone remembering. The Ray docs team owns the job.

Refresh by hand whenever you need to: after adding a target, or when a cross-reference to a symbol that does exist upstream stops resolving. Run it from the repo root, inside the docs virtualenv:

```bash
python doc/source/_intersphinx/refresh.py              # refresh all
python doc/source/_intersphinx/refresh.py numpy torch  # refresh a subset
```

Then review the diff and re-run the docs build before committing. The script reads the project list and upstream URLs straight from `_intersphinx_targets` in `../conf.py`, so it never drifts from the build configuration. To add or remove a target, edit `_intersphinx_targets` and re-run the script.
