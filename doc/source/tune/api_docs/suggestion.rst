.. _searchalg-ref:

Search Algorithms (tune.suggest)
================================

Repeated Evaluations
--------------------

Use :ref:`repeater-doc` to average over multiple evaluations of the same
hyperparameter configurations. This is useful in cases where the evaluated
training procedure has high variance (i.e., in reinforcement learning).

By default, ``Repeater`` will take in a ``repeat`` parameter and a ``search_alg``.
The ``search_alg`` will suggest new configurations to try, and the ``Repeater``
will run ``repeat`` trials of the configuration. It will then average the
``search_alg.metric`` from the final results of each repeated trial.


.. note:: This does not apply for grid search and random search.

.. warning:: It is recommended to not use ``Repeater`` with a TrialScheduler.
    Early termination can negatively affect the average reported metric.


Warm Start/Reusing Results
--------------------------

Limiting Concurrency
--------------------

.. autoclass:: ray.tune.suggest.ConcurrencyLimiter

Ax (tune.suggest.ax.AxSearch)
-----------------------------

[`Link to Library <http://hyperopt.github.io/hyperopt>`_]

.. autoclass:: ray.tune.suggest.ax.AxSearch

BayesOpt (tune.suggest.bayesopt.BayesOptSearch)
-----------------------------------------------

[`Link to Library <http://hyperopt.github.io/hyperopt>`_]

.. autoclass:: ray.tune.suggest.bayesopt.BayesOptSearch

BOHB (tune.suggest.bohb.TuneBOHB)
---------------------------------

[`Link to Library <http://hyperopt.github.io/hyperopt>`_]

.. autoclass:: ray.tune.suggest.bohb.TuneBOHB

Dragonfly (tune.suggest.dragonfly.DragonflySearch)
--------------------------------------------------

[`Link to Library <http://hyperopt.github.io/hyperopt>`_]

.. autoclass:: ray.tune.suggest.dragonfly.DragonflySearch

HyperOpt (tune.suggest.hyperopt.HyperOptSearch)
-----------------------------------------------

[`Link to Library <http://hyperopt.github.io/hyperopt>`_]

.. autoclass:: ray.tune.suggest.hyperopt.HyperOptSearch

Nevergrad (tune.suggest.nevergrad.NevergradSearch)
--------------------------------------------------

[`Link to Library <http://hyperopt.github.io/hyperopt>`_]

.. autoclass:: ray.tune.suggest.nevergrad.NevergradSearch

SigOpt (tune.suggest.sigopt.SigOptSearch)
-----------------------------------------

[`Link to Library <https://sigopt.com/>`_] [`Example Usage <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/sigopt_example.py>`_].

**Notes**: You must install SigOpt and have a `SigOpt API key <https://app.sigopt.com/docs/overview/authentication>`__ to make requests to use this module. Store the API token as an environment variable named ``SIGOPT_KEY`` like follows:

.. code-block:: bash

    pip install -U sigopt
    export SIGOPT_KEY= ...

You will need to use the `SigOpt experiment and space specification <https://app.sigopt.com/docs/overview/create>`__ to specify your search space.

.. autoclass:: ray.tune.suggest.sigopt.SigOptSearch

Scikit-Optimize (tune.suggest.skopt.SkOptSearch)
------------------------------------------------

[`Link to Library <https://scikit-optimize.github.io>`_][`Example Usage <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/skopt_example.py>`_]

**Notes**: You will need to install Scikit-Optimize to use this module.

.. code-block:: bash

    pip install scikit-optimize

This algorithm requires using the `Scikit-Optimize ask and tell interface <https://scikit-optimize.github.io/stable/auto_examples/ask-and-tell.html>`__. This interface requires using the `Optimizer <https://scikit-optimize.github.io/#skopt.Optimizer>`__ provided by Scikit-Optimize.

.. autoclass:: ray.tune.suggest.skopt.SkOptSearch

ZOOpt (tune.suggest.zoopt.ZOOptSearch)
--------------------------------------

[`Link to Library <http://hyperopt.github.io/hyperopt>`_]

.. autoclass:: ray.tune.suggest.zoopt.ZOOptSearch

.. _repeater-doc:

Repeater
--------

.. autoclass:: ray.tune.suggest.Repeater


tune.suggest.SearchAlgorithm
----------------------------

.. autoclass:: ray.tune.suggest.SearchAlgorithm
    :members:

tune.suggest.Searcher
---------------------

Often times, hyperparameter search algorithms are model-based and may be quite simple to implement. For this, one can extend the following abstract class and implement ``on_trial_complete``, and ``suggest``.

.. autoclass:: ray.tune.suggest.Searcher
    :members:
    :private-members:
    :show-inheritance:
