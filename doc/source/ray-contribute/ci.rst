CI Testing Workflow on PRs
==========================

This guide helps contributors to understand the Continuous Integration (CI)
workflow on a PR. Here CI stands for the automated testing of the codebase
on the PR.

`microcheck`: default tests on your PR
--------------------------------------
With every commit on your PR, by default, we'll run a set of tests
called `microcheck`.

These tests are designed to be 90% accurate at catching bugs on your
PR while running only 10% of the full test suite. As a result,
microcheck typically finishes twice as fast and twice cheaper than
the full test suite. Some of the notable features of microcheck are:

* If a new test is added or an existing test is modified in a pull
  request, microcheck will ensure these tests are included.
* You can manually add more tests to microcheck by including the following line
  in the body of your git commit message:
  `@microcheck TEST_TARGET01 TEST_TARGET02 ....`. This line must be in the
  body of your message, starting from the second line or
  below (the first line is the commit message title). For example, here
  is how I manually add tests in my pull request::

    // git command to add commit message
    git commit -a -s

    // content of the commit message
    run other serve doc tests

    @microcheck //doc:source/serve/doc_code/distilbert //doc:source/serve/doc_code/object_detection //doc:source/serve/doc_code/stable_diffusion

    Signed-off-by: can <can@anyscale.com>

If microcheck passes, you'll see a green checkmark on your PR. If it
fails, you'll see a red cross. In either case, you'll see a summary of
the test run statuses in the github UI.


Additional tests at merge time
------------------------------
In this workflow, to merge your PR, simply click on the Enable auto-merge
button (or ask a committer to do so). This will trigger additional test
cases, and the PR will merge automatically once they finish and pass.

Alternatively, you can also add a `go` label to manually trigger the full
test suite on your PR (be mindful that this is less recommended but we
understand you know best about the need of your PR). While we anticipate
this will be rarely needed, if you do require it constantly, please let
us know. We are continuously improve the effectiveness of microcheck.
