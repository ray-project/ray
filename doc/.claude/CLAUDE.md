<!-- Loaded on-demand when Claude works on Ray documentation files. -->
<!-- Keep under 50 lines. Multi-step procedures → skills. Code style → rules/. -->

# Ray Documentation

Sphinx documentation built by Read the Docs and served at `docs.ray.io`. Build pipeline: `.buildkite/doc.rayci.yml`.

## How CI is configured

`.buildkite/test.rules.txt` maps file-change patterns to tag sets, and tags drive which CI suites run. The `doc` tag covers content under `doc/`, `doc/requirements-doc.txt`, `.vale.ini`/`.vale/`, and `.buildkite/doc.rayci.yml`. Docs-only deplock changes (`python/deplocks/docs/*.lock`, `ci/raydepsets/configs/docs.depsets.yaml`) tag to `doc python_dependencies` to skip the full python-deps test set.

## Scope discipline for PRs

For docs-only fixes, take the lightest path:

- Touch only files mapped to the `doc` tag.
- Don't bundle in a non-doc change "while you're at it" — that change pulls its own (often expensive) test set into the PR.
- If a docs change requires a non-doc change to land cleanly (e.g., autodoc references a renamed symbol), land them in the larger non-doc PR, not a docs-led PR.

For generated API docs that depend on Python source under `python/ray/...`, expect broader test runs. That's correct, not a misconfiguration.

## When to revise the test rules

If a docs-only PR hits unnecessarily broad tests, file a `.buildkite/test.rules.txt` PR (precedent: #63132) instead of working around it. Quick check: does the file change affect any non-`doc` build artifact, runtime behavior, or API surface? If no, tag it `doc`.

## Skills

- `/lint` — run linters on modified files.
- `/fetch-buildkite-logs` — pull build logs for CI failures.

See top-level `.claude/CLAUDE.md` for the full skill index.
