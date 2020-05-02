.. _tune-search-alg:

Search Algorithms (tune.suggest)
================================

Tune integrates with a variety of open-source optimization libraries for efficient hyperparameter search.

Note that search algorithms will require a different search space declaration than the default Tune format - meaning that you will not be able to combine ``tune.grid_search`` with the below integrations.

You can utilize these search algorithms as follows:

.. code-block:: python

    tune.run(my_function, search_alg=SearchAlgorithm(...))

.. contents::
    :local:
    :depth: 1

.. _tune-ax:

AxSearch (tune.suggest.ax.AxSearch)
-----------------------------------

[`Ax Code Example`_]

.. _`Ax Code Example`: https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/ax_example.py

.. autoclass:: ray.tune.suggest.ax.AxSearch

Bayesian Optimization (tune.suggest.bayesopt.BayesOptSearch)
------------------------------------------------------------

[`Bayesian Optimization Code Example`_]

.. _`Bayesian Optimization Code Example`: https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/bayesopt_example.py

.. autoclass:: ray.tune.suggest.bayesopt.BayesOptSearch

.. _`BayesianOptimization search space specification`: https://github.com/fmfn/BayesianOptimization/blob/master/examples/advanced-tour.ipynb

BOHB (tune.suggest.bohb.TuneBOHB)
---------------------------------

[`Link to BOHB <https://github.com/automl/HpBandSter>`_][`BOHB Code Example`_]

.. _`BOHB Code Example`: https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/bohb_example.py

BOHB is an algorithm that both terminates bad trials and also uses Bayesian Optimization to improve the hyperparameter search. It is backed by the `HpBandSter library <https://github.com/automl/HpBandSter>`_.

Importantly, BOHB is intended to be paired with a specific scheduler class: `HyperBandForBOHB <tune-schedulers.html#hyperband-bohb>`__.

This algorithm requires using the `ConfigSpace search space specification <https://automl.github.io/HpBandSter/build/html/quickstart.html#searchspace>`_. In order to use this search algorithm, you will need to install ``HpBandSter`` and ``ConfigSpace``:

.. code-block:: bash

    $ pip install hpbandster ConfigSpace

See the `BOHB paper <https://arxiv.org/abs/1807.01774>`_ for more details.

.. autoclass:: ray.tune.suggest.bohb.TuneBOHB

Dragonfly (tune.suggest.dragonfly.DragonflySearch)
--------------------------------------------------

[`Dragonfly Code Example`_]

.. _`Dragonfly Code Example`: https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/dragonfly_example.py

.. autoclass:: ray.tune.suggest.dragonfly.DragonflySearch

.. _tune-hyperopt:

HyperOpt (tune.suggest.hyperopt.HyperOptSearch)
-----------------------------------------------

[`Hyperopt Code Example`_]

.. _`Hyperopt Code Example`: https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperopt_example.py

.. autoclass:: ray.tune.suggest.hyperopt.HyperOptSearch

.. _tune-nevergrad:

Nevergrad (tune.suggest.nevergrad.NevergradSearch)
--------------------------------------------------

[`Nevergrad Code Example`_]

.. _`Nevergrad Code Example`: https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/nevergrad_example.py

.. autoclass:: ray.tune.suggest.nevergrad.NevergradSearch

.. _`Nevergrad README's Optimization section`: https://github.com/facebookresearch/nevergrad/blob/master/docs/optimization.rst#choosing-an-optimizer

SigOpt (tune.suggest.sigopt.SigOptSearch)
-----------------------------------------

[`Link to SigOpt <https://sigopt.com/>`_] [`SigOpt Code Example`_]

.. _`SigOpt Code Example`: https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/sigopt_example.py

**Notes**: You must install SigOpt and have a `SigOpt API key <https://app.sigopt.com/docs/overview/authentication>`__ to make requests to use this module. Store the API token as an environment variable named ``SIGOPT_KEY`` like follows:

.. code-block:: bash

    pip install -U sigopt
    export SIGOPT_KEY= ...

You will need to use the `SigOpt experiment and space specification <https://app.sigopt.com/docs/overview/create>`__ to specify your search space.

.. autoclass:: ray.tune.suggest.sigopt.SigOptSearch

Scikit-Optimize (tune.suggest.skopt.SkOptSearch)
------------------------------------------------

[`Example Usage <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/skopt_example.py>`_]

.. autoclass:: ray.tune.suggest.skopt.SkOptSearch

.. _`skopt Optimizer object`: https://scikit-optimize.github.io/#skopt.Optimizer

ZOOpt (tune.suggest.zoopt.ZOOptSearch)
--------------------------------------

[`ZOOptSearch Code Example`_]

.. _`ZOOptSearch Code Example`: https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/zoopt_example.py

.. autoclass:: ray.tune.suggest.zoopt.ZOOptSearch

.. _repeater-doc:

Repeated Evaluations (tune.suggest.Repeater)
--------------------------------------------

Use ``ray.tune.suggest.Repeater`` to average over multiple evaluations of the same
hyperparameter configurations. This is useful in cases where the evaluated
training procedure has high variance (i.e., in reinforcement learning).

By default, ``Repeater`` will take in a ``repeat`` parameter and a ``search_alg``.
The ``search_alg`` will suggest new configurations to try, and the ``Repeater``
will run ``repeat`` trials of the configuration. It will then average the
``search_alg.metric`` from the final results of each repeated trial.


.. warning:: It is recommended to not use ``Repeater`` with a TrialScheduler.
    Early termination can negatively affect the average reported metric.

.. autoclass:: ray.tune.suggest.Repeater

ConcurrencyLimiter (tune.suggest.ConcurrencyLimiter)
----------------------------------------------------

Use ``ray.tune.suggest.Repeater`` to limit the amount of concurrency when using a search algorithm. This is useful when a given optimization algorithm does not parallelize very well (like a naive Bayesian Optimization).

.. autoclass:: ray.tune.suggest.ConcurrencyLimiter


Implementing your own Search Algorithm
--------------------------------------

If you are interested in implementing or contributing a new Search Algorithm, provide the following interface:

.. autoclass:: ray.tune.suggest.Searcher
    :members:
    :private-members:
    :show-inheritance:
