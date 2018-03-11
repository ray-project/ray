HyperBand and Early Stopping
============================

Ray Tune includes distributed implementations of early stopping algorithms such as `Median Stopping Rule <https://research.google.com/pubs/pub46180.html>`__, `HyperBand <https://arxiv.org/abs/1603.06560>`__, and an `asynchronous version of HyperBand <https://openreview.net/forum?id=S1Y7OOlRZ>`__. These algorithms are very resource efficient and can outperform Bayesian Optimization methods in `many cases <https://people.eecs.berkeley.edu/~kjamieson/hyperband.html>`__.

Asynchronous HyperBand
----------------------

The `asynchronous version of HyperBand <https://openreview.net/forum?id=S1Y7OOlRZ>`__ scheduler can be plugged in on top of an existing grid or random search. This can be done by setting the ``scheduler`` parameter of ``run_experiments``, e.g.

.. code-block:: python

    run_experiments({...}, scheduler=AsyncHyperBandScheduler())

Compared to the original version of HyperBand, this implementation provides better parallelism and avoids straggler issues during eliminations. An example of this can be found in `async_hyperband_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/async_hyperband_example.py>`__. **We recommend using this over the standard HyperBand scheduler.**

.. autoclass:: ray.tune.async_hyperband.AsyncHyperBandScheduler

HyperBand
---------

.. note:: Note that the HyperBand scheduler requires your trainable to support checkpointing, which is described in `Ray Tune documentation <tune.html#trial-checkpointing>`__. Checkpointing enables the scheduler to multiplex many concurrent trials onto a limited size cluster.

Ray Tune also implements the `standard version of HyperBand <https://arxiv.org/abs/1603.06560>`__. You can use it as such:

.. code-block:: python

    run_experiments({...}, scheduler=HyperBandScheduler())

An example of this can be found in `hyperband_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperband_example.py>`__. The progress of one such HyperBand run is shown below.


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


HyperBand Implementation Details
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Implementation details may deviate slightly from theory but are focused on increasing usability. Note: ``R``, ``s_max``, and ``eta`` are parameters of HyperBand given by the paper. See `this post <https://people.eecs.berkeley.edu/~kjamieson/hyperband.html>`_ for context.

1. Both ``s_max`` (representing the ``number of brackets - 1``) and ``eta``, representing the downsampling rate, are fixed.  In many practical settings, ``R``, which represents some resource unit and often the number of training iterations, can be set reasonably large, like ``R >= 200``. For simplicity, assume ``eta = 3``. Varying ``R`` between ``R = 200`` and ``R = 1000`` creates a huge range of the number of trials needed to fill up all brackets.

.. image:: images/hyperband_bracket.png

On the other hand, holding ``R`` constant at ``R = 300`` and varying ``eta`` also leads to HyperBand configurations that are not very intuitive:

.. image:: images/hyperband_eta.png

The implementation takes the same configuration as the example given in the paper and exposes ``max_t``, which is not a parameter in the paper.

2. The example in the `post <https://people.eecs.berkeley.edu/~kjamieson/hyperband.html>`_ to calculate ``n_0`` is actually a little different than the algorithm given in the paper. In this implementation, we implement ``n_0`` according to the paper (which is `n` in the below example):

.. image:: images/hyperband_allocation.png


3. There are also implementation specific details like how trials are placed into brackets which are not covered in the paper. This implementation places trials within brackets according to smaller bracket first - meaning that with low number of trials, there will be less early stopping.

Median Stopping Rule
--------------------

The Median Stopping Rule implements the simple strategy of stopping a trial if its performance falls below the median of other trials at similar points in time. You can set the ``scheduler`` parameter as such:

.. code-block:: python

    run_experiments({...}, scheduler=MedianStoppingRule())

.. autoclass:: ray.tune.median_stopping_rule.MedianStoppingRule
