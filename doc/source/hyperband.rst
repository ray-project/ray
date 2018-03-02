HyperBand and Early Stopping
============================

Ray Tune includes distributed implementations of early stopping algorithms such as `Median Stopping Rule <https://research.google.com/pubs/pub46180.html>`__ and `HyperBand <https://arxiv.org/abs/1603.06560>`__. These algorithms are very resource efficient and can outperform Bayesian Optimization methods in `many cases <https://people.eecs.berkeley.edu/~kjamieson/hyperband.html>`__.

HyperBand
---------

The HyperBand scheduler can be plugged in on top of an existing grid or random search. This can be done by setting the ``scheduler`` parameter of ``run_experiments``, e.g.

.. code-block:: python

    run_experiments({...}, scheduler=HyperBandScheduler())

An example of this can be found in `hyperband_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__. The progress of one such HyperBand run is shown below.

Note that the HyperBand scheduler requires your trainable to support checkpointing, which is described in `Ray Tune documentation <tune.html#trial-checkpointing>`__. Checkpointing enables the scheduler to multiplex many concurrent trials onto a limited size cluster.

::

    == Status ==
    Using HyperBand: num_stopped=0 total_brackets=5
    Round #0:
      Bracket(n=5, r=100, completed=80%): {'PAUSED': 4, 'PENDING': 1}
      Bracket(n=8, r=33, completed=23%): {'PAUSED': 4, 'PENDING': 4}
      Bracket(n=15, r=11, completed=4%): {'RUNNING': 2, 'PAUSED': 2, 'PENDING': 11}
      Bracket(n=34, r=3, completed=0%): {'RUNNING': 2, 'PENDING': 32}
      Bracket(n=81, r=1, completed=0%): {'PENDING': 38}
    Resources used: 4/4 CPUs, 0/0 GPUs
    Result logdir: ~/ray_results/hyperband_test
    PAUSED trials:
     - my_class_0_height=99,width=43:	PAUSED [pid=11664], 0 s, 100 ts, 97.1 rew
     - my_class_11_height=85,width=81:	PAUSED [pid=11771], 0 s, 33 ts, 32.8 rew
     - my_class_12_height=0,width=52:	PAUSED [pid=11785], 0 s, 33 ts, 0 rew
     - my_class_19_height=44,width=88:	PAUSED [pid=11811], 0 s, 11 ts, 5.47 rew
     - my_class_27_height=96,width=84:	PAUSED [pid=11840], 0 s, 11 ts, 12.5 rew
      ... 5 more not shown
    PENDING trials:
     - my_class_10_height=12,width=25:	PENDING
     - my_class_13_height=90,width=45:	PENDING
     - my_class_14_height=69,width=45:	PENDING
     - my_class_15_height=41,width=11:	PENDING
     - my_class_16_height=57,width=69:	PENDING
      ... 81 more not shown
    RUNNING trials:
     - my_class_23_height=75,width=51:	RUNNING [pid=11843], 0 s, 1 ts, 1.47 rew
     - my_class_26_height=16,width=48:	RUNNING
     - my_class_31_height=40,width=10:	RUNNING
     - my_class_53_height=28,width=96:	RUNNING

.. autoclass:: ray.tune.hyperband.HyperBandScheduler

Median Stopping Rule
--------------------

Tune also implements the Median Stopping Rule, which implements the simple strategy of stopping a trial if it's performance falls below the median of other trials at similar points in time.

.. autoclass:: ray.tune.median_stopping_rule.MedianStoppingRule
