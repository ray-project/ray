LightGBM on Ray tests
====================

This directory contains various LightGBM on Ray release tests.

You should run these tests with the `releaser <https://github.com/ray-project/releaser>`_ tool.

Overview
--------
There are four kinds of tests:

1. ``distributed_api_test`` - checks general API functionality and should finish very quickly (< 1 minute)
2. ``train_*`` - checks single trial training on different setups.
3. ``tune_*`` - checks multi trial training via Ray Tune.
4. ``ft_*`` - checks fault tolerance.

Generally the releaser tool will run all tests in parallel, but if you do
it sequentially, be sure to do it in the order above. If ``train_*`` fails,
``tune_*`` will fail, too.

Acceptance criteria
-------------------
These tests are considered passing when they throw no error at the end of
the output log.
