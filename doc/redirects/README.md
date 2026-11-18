# Read the Docs redirects for docs.ray.io

`current.yaml` is the source of truth for the HTTP redirects configured on the `anyscale-ray` Read the Docs project, which serves docs.ray.io. The file is managed with [rtd-redirects](https://github.com/anyscale/rtd-redirects) and mirrors the live configuration exactly.

Any contributor can add or change a redirect in a pull request. CI checks the change before merge and applies it to Read the Docs after merge. You don't need Read the Docs access or an API token.

## Policy

- Change redirects by editing `current.yaml` in a pull request, not in the Read the Docs dashboard. Dashboard edits are out-of-process: they aren't reviewed, and the next CI apply deletes them.
- If a maintainer with Read the Docs admin access has to land an urgent fix through the dashboard, they record why in the PR that follows, and open that reconcile PR within 24 hours. `rtd-redirects dump --project anyscale-ray -o doc/redirects/current.yaml` regenerates this file from live state.

## Install rtd-redirects

Install the version that CI pins in `.buildkite/doc.rayci.yml`:

```bash
uv tool install anyscale-rtd-redirects==0.3.0
```

Or `pip install anyscale-rtd-redirects==0.3.0` in a virtual environment.

The commands a contributor needs, `validate`, `diff-file`, and `simulate`, read only local files, git history, and public docs pages, so they need no credentials. The commands that read or change the live project, `plan`, `audit`, `dump`, `list`, and `apply`, need a Read the Docs API token with admin access to `anyscale-ray`. CI holds that token, and only for the postmerge apply step.

## Adding or changing a redirect

1. Edit `current.yaml`. Pick the rule as described in [Choosing a rule](#choosing-a-rule). When a page that existing rules point at moves, also see [Moving or renaming pages](#moving-or-renaming-pages).
1. Validate locally:

   ```bash
   rtd-redirects validate doc/redirects/current.yaml
   ```

   Fix every error before you push. If `validate` reports an ordering error, `rtd-redirects validate doc/redirects/current.yaml --fix` reorders the file for you. `--fix` drops YAML comments, so review the diff.
1. Open a PR. CI runs the checks in [What CI checks on your PR](#what-ci-checks-on-your-pr).
1. After the PR merges, CI applies the change to the live project. You don't apply anything yourself. See [When merged redirects go live](#when-merged-redirects-go-live).

## Choosing a rule

Each rule has a `from` path, a `to` path, and a `type`. Write paths without the `/en/<version>` prefix, starting at the docs root, with the `.html` suffix. When a page or directory moves, use the first option that fits:

1. **One page renamed or moved.** Map the old page to the new one:

   ```yaml
   - from: /serve/getting_started.html
     to: /serve/getting-started.html
     type: page
   ```

1. **A directory moved and kept its page names.** Use a wildcard `from` and `:splat` in `to`. `:splat` stands for whatever `*` matched, so every page lands on its own new path:

   ```yaml
   - from: /old-dir/*
     to: /new-dir/:splat
     type: page
   ```

1. **Pages removed, or moved with no one-to-one mapping.** Send every old URL under the path to the nearest surviving index page:

   ```yaml
   - from: /rllib/rllib-*
     to: /rllib/index.html
     type: page
   ```

When rules overlap, put the more specific rule first. Read the Docs uses the first rule in the file that matches, so a specific rule after a wildcard that also matches it never fires. `validate` reports that case as an error.

Point `to` at the final destination, not at the `from` of another rule. Never write a wildcard rule that sends a path to the same path, such as `from: /x/*` with `to: /x/:splat`. It redirects a missing page to itself.

Leave `force` unset for all of these. [How redirects apply across versions](#how-redirects-apply-across-versions) explains why one version-less `page` rule is correct on every docs version. Two other forms exist for narrower cases:

- **`type: exact`** matches one full URL, including the language and version prefix, so it applies to only that version. Use it only to redirect a URL on one specific version, such as a page that existed only in an old release:

  ```yaml
  - from: /en/releases-2.0.0rc0/cluster/cluster_under_construction/getting-started.html
    to: /en/latest/cluster/vms/getting-started.html
    type: exact
  ```

- **`force: true`** redirects a page even when it still exists. See [How redirects apply across versions](#how-redirects-apply-across-versions) before you use it.

## What CI checks on your PR

A PR that changes `doc/redirects/*.yaml` runs the `doc: validate redirects` Buildkite step. A PR that changes only redirect YAML also runs the pre-commit lint step, and no doc build or tests. The step uses no Read the Docs token, so it runs the same way on PRs from forks. It does the following:

- Fails if `doc/redirects/` contains any `.yaml` file other than `current.yaml`. Add rules to `current.yaml` instead of creating a new file.
- Runs `rtd-redirects validate`, the same check you run locally. Errors fail the step. Warnings and info-level notes don't.
- Runs `rtd-redirects diff-file` against the base branch of your PR and prints one line per added, changed, removed, or reordered rule, such as `+ /serve/getting_started.html -> /serve/getting-started.html (page)`. Reviewers can read the redirect-level change in the step's log instead of the YAML diff.

To preview that list locally, run the following, replacing `<base>` with the ref of `ray-project/ray` master in your clone, such as `upstream/master`:

```bash
rtd-redirects diff-file --file doc/redirects/current.yaml --base <base> --head HEAD --repo .
```

## When merged redirects go live

The `doc: apply redirects` Buildkite step runs `rtd-redirects apply --project anyscale-ray --file doc/redirects/current.yaml --strict`. It runs only on the postmerge builds that the scheduled release-automation pipeline triggers, not on every merge to master. A merged change goes live on the first of those runs after the merge, usually within a few hours on a weekday and longer over a weekend. If that run fails or skips the step, the change waits for the next one.

To check whether a redirect is live, request the old URL on a version where the old page no longer exists, such as `/en/master/`:

```bash
curl -sI https://docs.ray.io/en/master/<old-path>.html
```

A live redirect returns `301` with a `location` header that points at the new page. A `404` means either no apply run has happened since the merge, or the rule doesn't match the URL you requested.

## How redirects apply across versions

A rule with the default `force: false` fires only when the requested URL would otherwise return 404. A `type: page` rule applies to every docs version, and each version resolves the rule's `to` path within that version. Together, these properties let one version-less rule behave correctly on every version:

- On a version where the old path still exists, such as `/en/latest/` before a release or any older release, the page renders and the rule doesn't fire.
- On a version where the old path is gone, such as `/en/master/` after a move merges, the rule fires and sends the reader to the new path in that same version.

Each release that ships a change picks up its redirect when the old path stops existing in that release. The redirect needs no release-day edit to this file.

Set `force: true` only to redirect a page that still exists. A forced rule fires on every version it matches, including `/en/latest/` and older releases.

## Moving or renaming pages

When a PR moves or renames pages under `doc/source/`, add a version-less `page` rule from each old path to its new path in the same PR. For a directory, one wildcard rule covers every page under it, as in [Choosing a rule](#choosing-a-rule).

Don't repoint existing rules whose `to` points under the moved path. Older releases still serve the old path, so an existing rule that points there resolves directly on those versions. On versions that include the move, the request takes a second hop through the new rule and lands on the moved page. Repointing the existing rule to the new path would send readers of older releases to a page that doesn't exist in their version.

Do move the `from` of a catch-all rule that matches under the old path, such as `/old-dir/examples/*`, to the new path. A catch-all is more specific than the directory rule, so it sits ahead of it and matches first. Left on the old path, it would catch moved pages before the directory rule could send them to their new location.

`rtd-redirects validate` reports each two-hop path as an info-level chain note. It counts these notes and lists them only with `--show-info`. Chain notes from a move are expected. A chain is a warning only when the second rule sets `force: true`, because that chain happens on every version. Neither blocks CI, which fails only on error-level findings.

`validate` checks the rules on their own, so it can't tell you where an old URL lands. For a large move, `rtd-redirects simulate` replays the old URLs through the rules before and after your change on several docs versions, and reports any that land on a 404, a loop, or a different page than before. It needs no credentials. See the `simulate` section of the [rtd-redirects README](https://github.com/anyscale/rtd-redirects#simulate) for its options.

## Renaming, moving, or removing APIs

Generated API reference pages follow the same rules as hand-written pages. Sphinx autosummary generates one page per documented object, named after the object's fully qualified name and placed under the `:toctree:` directory of the API page that lists it, such as `/data/api/doc/ray.data.Dataset.map.html`. Renaming or moving an API changes that path, and so does moving the API page that lists it. A code change under `python/ray/` that renames or moves a public API therefore changes docs URLs too. Add a redirect for each generated page whose path changes, in the same PR.

When you remove the reference page of a deprecated or end-of-life API that has a clear successor, redirect the old page to the successor's reference page.

Rules with the default `force: false` fire only on a 404, so they never replace a reference page that still exists. Docs versions that still document the old API, such as older releases, keep serving its page. The redirect takes effect only on versions where the page is gone.

## Auditing drift

This section is for maintainers with Read the Docs admin access. `rtd-redirects plan --project anyscale-ray --file doc/redirects/current.yaml` shows any difference between this file and the live configuration. An empty plan means no drift. It needs an `RTD_API_TOKEN` with admin access to `anyscale-ray`.
