.. _tune-search-alg:

Search Algorithms (tune.suggest)
================================

Tune's Search Algorithms are wrappers around open-source optimization libraries for efficient hyperparameter selection. These integrations optimize the given trainable over a provided search space. You can utilize these search algorithms as follows:

.. code-block:: python

    from ray.tune.suggest.hyperopt import HyperOptSearch
    tune.run(my_function, search_alg=HyperOptSearch(...))

Summary
-------

.. list-table::
   :header-rows: 1

   * - SearchAlgorithm
     - Summary
     - Website
     - Code Example
   * - :ref:`AxSearch <tune-ax>`
     - Bayesian/Bandit Optimization
     - [`Ax <https://ax.dev/>`__]
     - `Link <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/ax_example.py>`__
   * - :ref:`DragonflySearch <Dragonfly>`
     - Scalable Bayesian Optimization
     - [`Dragonfly <https://dragonfly-opt.readthedocs.io/>`__]
     - `Link <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/dragonfly_example.py>`__
   * - :ref:`SkoptSearch <skopt>`
     - Bayesian Optimization
     - [`Scikit-Optimize <https://scikit-optimize.github.io>`__]
     - `Link <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/skopt_example.py>`__
   * - :ref:`HyperOptSearch <tune-hyperopt>`
     - Tree-Parzen Estimators
     - [`HyperOpt <http://hyperopt.github.io/hyperopt>`__]
     - `Link <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperopt_example.py>`__
   * - :ref:`BayesOptSearch <bayesopt>`
     - Bayesian Optimization
     - [`BayesianOptimization <https://github.com/fmfn/BayesianOptimization>`__]
     - `Link <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/bayesopt_example.py>`__
   * - :ref:`TuneBOHB <suggest-TuneBOHB>`
     - Bayesian Opt/HyperBand
     - [`BOHB <https://github.com/automl/HpBandSter>`__]
     - `Link <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/bohb_example.py>`__
   * - :ref:`NevergradSearch <nevergrad>`
     - Gradient-free Optimization
     - [`Nevergrad <https://github.com/facebookresearch/nevergrad>`__]
     - `Link <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/nevergrad_example.py>`__
   * - :ref:`ZOOptSearch <zoopt>`
     - Zeroth-order Optimization
     - [`ZOOpt <https://github.com/polixir/ZOOpt>`__]
     - `Link <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/zoopt_example.py>`__
   * - :ref:`SigOptSearch <sigopt>`
     - Closed source
     - [`SigOpt <https://sigopt.com/>`__]
     - `Link <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/sigopt_example.py>`__


.. note::Search algorithms will require a different search space declaration than the default Tune format - meaning that you will not be able to combine ``tune.grid_search`` with the below integrations.

.. note:: Unlike :ref:`Tune's Trial Schedulers <tune-schedulers>`, Tune SearchAlgorithms cannot affect or stop training processes. However, you can use them together to **early stop the evaluation of bad trials**.

**Want to use your own algorithm?** The interface is easy to implement. :ref:`Read instructions here <byo-algo>`.


Tune also provides helpful utilities to use with Search Algorithms:

 * :ref:`repeater`: Support for running each *sampled hyperparameter* with multiple random seeds.
 * :ref:`limiter`: Limits the amount of concurrent trials when running optimization.


.. _tune-ax:

Ax (tune.suggest.ax.AxSearch)
-----------------------------

.. autoclass:: ray.tune.suggest.ax.AxSearch

.. _bayesopt:

Bayesian Optimization (tune.suggest.bayesopt.BayesOptSearch)
------------------------------------------------------------


.. autoclass:: ray.tune.suggest.bayesopt.BayesOptSearch

.. _`BayesianOptimization search space specification`: https://github.com/fmfn/BayesianOptimization/blob/master/examples/advanced-tour.ipynb

.. _suggest-TuneBOHB:

BOHB (tune.suggest.bohb.TuneBOHB)
---------------------------------

BOHB (Bayesian Optimization HyperBand) is an algorithm that both terminates bad trials and also uses Bayesian Optimization to improve the hyperparameter search. It is backed by the `HpBandSter library <https://github.com/automl/HpBandSter>`_.

Importantly, BOHB is intended to be paired with a specific scheduler class: `HyperBandForBOHB <tune-schedulers.html#hyperband-bohb>`__.

This algorithm requires using the `ConfigSpace search space specification <https://automl.github.io/HpBandSter/build/html/quickstart.html#searchspace>`_. In order to use this search algorithm, you will need to install ``HpBandSter`` and ``ConfigSpace``:

.. code-block:: bash

    $ pip install hpbandster ConfigSpace

See the `BOHB paper <https://arxiv.org/abs/1807.01774>`_ for more details.

.. autoclass:: ray.tune.suggest.bohb.TuneBOHB

.. _Dragonfly:

Dragonfly (tune.suggest.dragonfly.DragonflySearch)
--------------------------------------------------

.. autoclass:: ray.tune.suggest.dragonfly.DragonflySearch

.. _tune-hyperopt:

HyperOpt (tune.suggest.hyperopt.HyperOptSearch)
-----------------------------------------------

.. autoclass:: ray.tune.suggest.hyperopt.HyperOptSearch

.. _nevergrad:

Nevergrad (tune.suggest.nevergrad.NevergradSearch)
--------------------------------------------------

.. autoclass:: ray.tune.suggest.nevergrad.NevergradSearch

.. _`Nevergrad README's Optimization section`: https://github.com/facebookresearch/nevergrad/blob/master/docs/optimization.rst#choosing-an-optimizer

.. _sigopt:

SigOpt (tune.suggest.sigopt.SigOptSearch)
-----------------------------------------

You will need to use the `SigOpt experiment and space specification <https://app.sigopt.com/docs/overview/create>`__ to specify your search space.

.. autoclass:: ray.tune.suggest.sigopt.SigOptSearch

.. _skopt:

Scikit-Optimize (tune.suggest.skopt.SkOptSearch)
------------------------------------------------

.. autoclass:: ray.tune.suggest.skopt.SkOptSearch

.. _`skopt Optimizer object`: https://scikit-optimize.github.io/#skopt.Optimizer

.. _zoopt:

ZOOpt (tune.suggest.zoopt.ZOOptSearch)
--------------------------------------

.. autoclass:: ray.tune.suggest.zoopt.ZOOptSearch

.. _repeater:

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

.. _limiter:

ConcurrencyLimiter (tune.suggest.ConcurrencyLimiter)
----------------------------------------------------

Use ``ray.tune.suggest.ConcurrencyLimiter`` to limit the amount of concurrency when using a search algorithm. This is useful when a given optimization algorithm does not parallelize very well (like a naive Bayesian Optimization).

.. autoclass:: ray.tune.suggest.ConcurrencyLimiter

.. _byo-algo:

Implementing your own Search Algorithm
--------------------------------------

If you are interested in implementing or contributing a new Search Algorithm, provide the following interface:

.. autoclass:: ray.tune.suggest.Searcher
    :members:
    :private-members:
    :show-inheritance:
