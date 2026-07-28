---
myst:
  html_meta:
    description: "Explains the continuous integration workflow on Ray pull requests, including the microcheck default test set, how to add tests to it, and the full suite that runs at merge time. Read this to understand which tests run on your PR and how to trigger more."
---

# CI testing workflow on PRs

This guide helps contributors understand the continuous integration (CI) workflow on a PR. Here, CI stands for automated testing of the codebase on the PR.

## `microcheck`: default tests on your PR

With every commit on your PR, by default, we'll run a set of tests called `microcheck`.

These tests are designed to be 90% accurate at catching bugs on your PR while running only 10% of the full test suite. As a result, microcheck typically finishes twice as fast and at half the cost of the full test suite. Some notable features of microcheck are:

* If a new test is added or an existing test is modified in a pull request, microcheck ensures these tests are included.
* You can manually add more tests to microcheck by including the following line in the body of your git commit message: `@microcheck TEST_TARGET01 TEST_TARGET02 ....`. This line must be in the body of your message, starting from the second line or below (the first line is the commit message title). For example, here is how I manually add tests in my pull request:

  ```
  // git command to add commit message
  git commit -a -s

  // content of the commit message
  run other serve doc tests

  @microcheck //doc:source/serve/doc_code/distilbert //doc:source/serve/doc_code/object_detection //doc:source/serve/doc_code/stable_diffusion

  Signed-off-by: can <can@anyscale.com>
  ```

If microcheck passes, you'll see a green checkmark on your PR. If it fails, you'll see a red cross. In either case, you'll see a summary of the test run statuses in the GitHub UI.

## Additional tests at merge time

`microcheck` runs on every commit, but the full test suite must pass before a PR can merge. Adding the `go` label triggers the full suite, and committers require the `go` tests to have passed before adding a PR to the merge queue.

If you're a committer, add the `go` label to your PR once it's ready, then merge after the full suite passes. Clicking **Enable auto-merge** does both in one step: it adds the `go` label and merges the PR automatically once the suite passes. Pushing a new commit disables auto-merge, so re-enable it afterward. When you review an external contributor's PR, add the `go` label for them, since they can't add it themselves.

If you're an external contributor, adding the `go` label and enabling auto-merge both require write access, so a committer runs the full suite and merges when your PR is ready.
