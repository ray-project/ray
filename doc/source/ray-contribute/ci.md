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

In this workflow, to merge your PR, click the **Enable auto-merge** button (or ask a committer to do so). This triggers additional test cases, and the PR merges automatically once they finish and pass.

Alternatively, you can add a `go` label to manually trigger the full test suite on your PR. Be mindful that this is less recommended, but we understand you know best about the needs of your PR. We anticipate this being rarely needed, but if you require it constantly, please let us know. We're continuously improving the effectiveness of microcheck.
