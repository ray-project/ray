# Intel Inner-Source Ray Fork

## Purpose

This repository is a mirror of [upstream Ray](https://github.com/ray-project/ray)
maintained under the `intel-innersource` GitHub organisation. Its goals are:

- Host Intel-GPU (XPU) specific validation tests and patches.
- Provide a staging area for Intel GPU accelerator support
  (e.g. XPU device detection).
- Track upstream Ray releases while carrying Intel-specific additions.
- Reference ticket: **AIFQA-205**.

## Upstream Sync

A GitHub Actions workflow (`.github/workflows/sync-upstream.yml`) runs daily
at **04:00 UTC** to merge changes from `ray-project/ray` `master` into this
fork's `main` branch and force-pushes upstream `releases/*` branches.

The sync can also be triggered manually from the **Actions** tab.

If a merge conflict occurs the workflow fails and manual resolution is
required.

## Branch Strategy

| Branch pattern | Description |
|----------------|-------------|
| `main` | Tracks `upstream/master` plus Intel-specific patches. |
| `releases/*` | Mirrors of upstream release branches. |
| `intel/*` | Branches for Intel-specific feature work. |

## Contributing

Intel-specific changes should go on `intel/*` branches and be submitted as
pull requests against `main`. Upstream contributions should be submitted
directly to [ray-project/ray](https://github.com/ray-project/ray).
