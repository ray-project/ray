RLlib Tests
===========

This directory contains various RLlib release tests.

You should run these tests with the `releaser <https://github.com/ray-project/releaser>`_ tool.

Overview
--------
Currently, there are 3 RLlib tests:

1. ``learning_tests`` - Tests, whether major algos (tf+torch) can learn in Atari or PyBullet envs in ~30-60min.
1. ``stress_tests`` - Runs 4 IMPALA Atari jobs, each one using 1GPU and 128CPUs (needs autoscaling to succeed).
1. ``unit_gpu_tests`` - Tests, whether all of RLlib's example scripts can be run on a GPU.

Generally the releaser tool will run all tests in parallel.

Acceptance criteria
-------------------
These tests are considered passing when they throw no error at the end of the output log.
