# Buildkite pipelines

This directory contains the Buildkite pipeline definitions for Ray CI, plus the rules that decide which tests run on a given change.

## Pipelines

Pipelines are defined in the `*.rayci.yml` files in this directory and are generated and run by [rayci](https://github.com/ray-project/rayci), Ray's CI orchestration tool. Each file groups the steps for one area (for example, `core.rayci.yml`, `data.rayci.yml`, `doc.rayci.yml`).

## Conditional test selection

Which steps run on a given pull request is determined by mapping changed file paths to *tags*. Two rule files drive this:

- `test.rules.txt` is the conditional rule set. Rules are evaluated in order and the first match wins. A rule with no `@` tags is a skip rule (it matches but emits nothing), and a trailing `*` catch-all fans unmatched changes out to the full suite.
- `always.rules.txt` is an always-on overlay applied to every change.

rayci parses these files (its `test-rules` command) and runs the steps whose tags are emitted. See the header comment in each rules file for the full format.

`ci/pipeline/determine_tests_to_run.py` is an in-repo reference implementation of the same rule-file format. It is not the live evaluator; rayci is.
