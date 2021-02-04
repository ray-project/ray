XGBoost on Ray tests
====================

This directory contains various XGBoost on Ray release tests.

You should run these tests with the `releaser <https://github.com/ray-project/releaser>`_ tool.

Overview
--------
There are four kinds of tests:

1. ``distributed_api_test`` - checks general API functionality and should finish very quickly (< 1 minute)
2. ``train_*`` - checks single trial training on different setups.
3. ``tune_*`` - checks multi trial training via Ray Tune.
4. ``ft_*`` - checks fault tolerance. **These tests are currently flaky**

Generally the releaser tool will run all tests in parallel, but if you do
it sequentially, be sure to do it in the order above. If ``train_*`` fails,
``tune_*`` will fail, too.

Flaky fault tolerance tests
---------------------------
The fault tolerance tests are currently flaky. In some runs, more nodes die
than expected, causing the test to fail. In other cases, the re-scheduled
actors become available too soon after crashing, causing the assertions to
fail. Please consider re-running the test a couple of times or contact the
test owner with outputs from the tests for further questions.

Acceptance criteria
-------------------
These tests are considered passing when they throw no error at the end of
the output log.
