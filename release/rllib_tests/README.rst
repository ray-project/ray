RLlib Tests
===========

This directory contains various RLlib release tests.

You should run these tests with the `releaser <https://github.com/ray-project/releaser>`_ tool.

Overview
--------
There are ??? kinds of tests:

1. ``learning_tests`` - Tests, whether major algos (tf+torch) can learn in Atari or PyBullet envs in ~30-60min.

Generally the releaser tool will run all tests in parallel, but if you do
it sequentially, be sure to do it in the order above. If ``train_*`` fails,
``tune_*`` will fail, too.

Acceptance criteria
-------------------
These tests are considered passing when they throw no error at the end of
the output log.
