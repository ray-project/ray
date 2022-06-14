# RLlib Hard Learning Test

Test most important RLlib algorithms with hard enough tasks to prevent performance regression.

Algorithms in this suite are split into multiple tests, so groups of tests can run in parallel. This is to ensure reasonable total runtime.

All learning tests have ``stop`` and ``pass_criteria`` configured, where ``stop`` specifies a fixed test duration, and ``pass_criteria`` specified performance goals like ``minimum reward`` and ``minimum throughput``.

Unlike normal tuned examples, these learning tests always run to the full specified test duration, and would NOT stop early when the ``pass_criteria`` is met.

This is so they can serve better as performance regression tests:

* By giving these tests more time, we get better idea of where they actually peak out (instead of simply stopping at a pre-specified reward). So we will have better ideas of minor peak performance regressions when they happen.
* By decoupling peak performance from ``pass_criteria``, we can specify a relatively conservative ``pass_criteria``, to avoid having flaky tests that pass and fail because of random fluctuations.
* These conservative passing thresholds help alert us when some algorithms are badly broken.
* Peak reward and throughput numbers gets save in DB, so we can see, hopefully step function, trends over time when we improve things.

TODO: we don't see progress right now in the time series chart, if an algorithm learns faster, but to the same peak performance.
For that, we need to plot multiple lines at different percentage time mark.

If you have any questions about these tests, ping jungong@.