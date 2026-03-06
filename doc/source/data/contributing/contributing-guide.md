# Contributing Guide

If you want your changes to be reviewed and merged quickly, following a few key 
practices makes a big difference. Clear, focused, and well-structured contributions help 
reviewers understand your intent and ensure your improvements land smoothly.

:::{seealso}
This guide covers contributing to Ray Data in specific. For information on contributing 
to the Ray project in general, see the {ref}`general Ray contributing guide<getting-involved>`.
:::

## Find something to work on

Start by solving a problem you encounter, like fixing a bug or adding a missing feature. 
If you're unsure where to start:
* Browse the issue tracker for problems you understand.
* Look for labels like ["good first issue"](https://github.com/ray-project/ray/issues?q=is%3Aissue%20state%3Aopen%20label%3Agood-first-issue%20label%3Adata) for approachable tasks.
* [Join the Ray Slack](https://www.ray.io/join-slack) and post in #data-contributors.

## Get early feedback

If you’re adding a new public API or making a substantial refactor, 
**share your plan early**. Discussing changes before you invest a lot of work can save 
time and align your work with the project’s direction.

You can open a draft PR, discuss on an Issue, or post in Slack for early feedback. It 
won’t affect acceptance and often improves the final design.

## Write good tests

Most changes to Ray Data require tests. For tips on how to write good tests, see 
{ref}`How to write tests <how-to-write-tests>`.

## Write simple, clear code

Ray Data values **readable, maintainable, and extendable** code over clever tricks.
For guidance on how to write code that aligns with Ray Data's design taste, see
[A Philosophy of Software Design](https://web.stanford.edu/~ouster/cgi-bin/aposd2ndEdExtract.pdf).

## Test your changes locally

To test your changes locally, build [Ray from source](https://docs.ray.io/en/latest/ray-contribute/development.html). 
For Ray Data development, you typically only need the Python environment—you can skip the C++ build unless you’re also contributing to Ray Core.

Before submitting a PR, run `pre-commit` to lint your changes and `pytest` to execute your tests.

Note that the full Ray Data test suite can be heavy to run locally, start with tests directly related to your changes. For example, if you modified `map`, from `python/ray/data/tests` run: `pytest test_map.py`.


## Open a pull request

### Write a clear pull request description

Explain **why the change exists and what it achieves**. Clear descriptions reduce 
back-and-forth and speed up reviews.

Here's an example of a PR with a good description: [[Data] Refactor PhysicalOperator.completed to fix side effects ](https://github.com/ray-project/ray/pull/58915).

### Keep pull requests small

Review difficulty scales non-linearly with PR size.

For fast reviews, do the following:
* **Keep PRs under ~200 lines** of change when possible.
* **Split large PRs** into multiple incremental PRs.
* Avoid mixing refactors and new features in the same PR.

Here's an example of a PR that keeps its scope small: 
[[Data] Support Non-String Items for ApproximateTopK Aggregator](https://github.com/ray-project/ray/pull/58659).
While the broader effort focuses on optimizing preprocessors, this change was 
deliberately split out as a small, incremental PR, which made it much easier to review.

### Make CI pass

Ray's CI runs lint and a small set of tests first in the `buildkite/microcheck` check. 
Start by making that pass.

Once it’s green, tag your reviewer. They can add the go label to trigger the full test 
suite.
