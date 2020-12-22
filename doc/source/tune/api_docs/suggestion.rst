.. _tune-search-alg:

Search Algorithms (tune.suggest)
================================

Tune's Search Algorithms are wrappers around open-source optimization libraries for efficient hyperparameter selection. Each library has a specific way of defining the search space - please refer to their documentation for more details.

You can utilize these search algorithms as follows:

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
   * - :ref:`Random search/grid search <tune-basicvariant>`
     - Random search/grid search
     -
     - :doc:`/tune/examples/tune_basic_example`
   * - :ref:`AxSearch <tune-ax>`
     - Bayesian/Bandit Optimization
     - [`Ax <https://ax.dev/>`__]
     - :doc:`/tune/examples/ax_example`
   * - :ref:`DragonflySearch <Dragonfly>`
     - Scalable Bayesian Optimization
     - [`Dragonfly <https://dragonfly-opt.readthedocs.io/>`__]
     - :doc:`/tune/examples/dragonfly_example`
   * - :ref:`SkoptSearch <skopt>`
     - Bayesian Optimization
     - [`Scikit-Optimize <https://scikit-optimize.github.io>`__]
     - :doc:`/tune/examples/skopt_example`
   * - :ref:`HyperOptSearch <tune-hyperopt>`
     - Tree-Parzen Estimators
     - [`HyperOpt <http://hyperopt.github.io/hyperopt>`__]
     - :doc:`/tune/examples/hyperopt_example`
   * - :ref:`BayesOptSearch <bayesopt>`
     - Bayesian Optimization
     - [`BayesianOptimization <https://github.com/fmfn/BayesianOptimization>`__]
     - :doc:`/tune/examples/bayesopt_example`
   * - :ref:`TuneBOHB <suggest-TuneBOHB>`
     - Bayesian Opt/HyperBand
     - [`BOHB <https://github.com/automl/HpBandSter>`__]
     - :doc:`/tune/examples/bohb_example`
   * - :ref:`NevergradSearch <nevergrad>`
     - Gradient-free Optimization
     - [`Nevergrad <https://github.com/facebookresearch/nevergrad>`__]
     - :doc:`/tune/examples/nevergrad_example`
   * - :ref:`OptunaSearch <tune-optuna>`
     - Optuna search algorithms
     - [`Optuna <https://optuna.org/>`__]
     - :doc:`/tune/examples/optuna_example`
   * - :ref:`ZOOptSearch <zoopt>`
     - Zeroth-order Optimization
     - [`ZOOpt <https://github.com/polixir/ZOOpt>`__]
     - :doc:`/tune/examples/zoopt_example`
   * - :ref:`SigOptSearch <sigopt>`
     - Closed source
     - [`SigOpt <https://sigopt.com/>`__]
     - :doc:`/tune/examples/sigopt_example`

.. note:: Unlike :ref:`Tune's Trial Schedulers <tune-schedulers>`, Tune SearchAlgorithms cannot affect or stop training processes. However, you can use them together to **early stop the evaluation of bad trials**.

**Want to use your own algorithm?** The interface is easy to implement. :ref:`Read instructions here <byo-algo>`.


Tune also provides helpful utilities to use with Search Algorithms:

 * :ref:`repeater`: Support for running each *sampled hyperparameter* with multiple random seeds.
 * :ref:`limiter`: Limits the amount of concurrent trials when running optimization.
 * :ref:`shim`: Allows creation of the search algorithm object given a string.

Saving and Restoring
--------------------

Certain search algorithms have ``save/restore`` implemented,
allowing reuse of learnings across multiple tuning runs.

.. code-block:: python

    search_alg = HyperOptSearch()

    experiment_1 = tune.run(
        trainable,
        search_alg=search_alg)

    search_alg.save("./my-checkpoint.pkl")

    # Restore the saved state onto another search algorithm

    search_alg2 = HyperOptSearch()
    search_alg2.restore("./my-checkpoint.pkl")

    experiment_2 = tune.run(
        trainable,
        search_alg=search_alg2)

Further, Tune automatically saves its state inside the current experiment folder ("Result Dir") during tuning.

Note that if you have two Tune runs with the same experiment folder,
the previous state checkpoint will be overwritten. You can
avoid this by making sure ``tune.run(name=...)`` is set to a unique
identifier.

.. code-block:: python

    search_alg = HyperOptSearch()
    experiment_1 = tune.run(
        cost,
        num_samples=5,
        search_alg=search_alg,
        verbose=0,
        name="my-experiment-1",
        local_dir="~/my_results")

    search_alg2 = HyperOptSearch()
    search_alg2.restore_from_dir(
      os.path.join("~/my_results", "my-experiment-1"))

.. note:: This is currently not implemented for: AxSearch, TuneBOHB, SigOptSearch, and DragonflySearch.

.. _tune-basicvariant:

Random search and grid search (tune.suggest.basic_variant.BasicVariantGenerator)
--------------------------------------------------------------------------------

The default and most basic way to do hyperparameter search is via random and grid search.
Ray Tune does this through the :class:`BasicVariantGenerator <ray.tune.suggest.basic_variant.BasicVariantGenerator>`
class that generates trial variants given a search space definition.

The :class:`BasicVariantGenerator <ray.tune.suggest.basic_variant.BasicVariantGenerator>` is used per
default if no search algorithm is passed to
:func:`tune.run() <ray.tune.run>`.

.. autoclass:: ray.tune.suggest.basic_variant.BasicVariantGenerator

.. _tune-ax:

Ax (tune.suggest.ax.AxSearch)
-----------------------------

.. autoclass:: ray.tune.suggest.ax.AxSearch

.. _bayesopt:

Bayesian Optimization (tune.suggest.bayesopt.BayesOptSearch)
------------------------------------------------------------


.. autoclass:: ray.tune.suggest.bayesopt.BayesOptSearch
  :members: save, restore

.. _`BayesianOptimization search space specification`: https://github.com/fmfn/BayesianOptimization/blob/master/examples/advanced-tour.ipynb

.. _suggest-TuneBOHB:

BOHB (tune.suggest.bohb.TuneBOHB)
---------------------------------

BOHB (Bayesian Optimization HyperBand) is an algorithm that both terminates bad trials and also uses Bayesian Optimization to improve the hyperparameter search. It is backed by the `HpBandSter library <https://github.com/automl/HpBandSter>`_.

Importantly, BOHB is intended to be paired with a specific scheduler class: :ref:`HyperBandForBOHB <tune-scheduler-bohb>`.

This algorithm requires using the `ConfigSpace search space specification <https://automl.github.io/HpBandSter/build/html/quickstart.html#searchspace>`_. In order to use this search algorithm, you will need to install ``HpBandSter`` and ``ConfigSpace``:

.. code-block:: bash

    $ pip install hpbandster ConfigSpace

See the `BOHB paper <https://arxiv.org/abs/1807.01774>`_ for more details.

.. autoclass:: ray.tune.suggest.bohb.TuneBOHB

.. _Dragonfly:

Dragonfly (tune.suggest.dragonfly.DragonflySearch)
--------------------------------------------------

.. autoclass:: ray.tune.suggest.dragonfly.DragonflySearch
  :members: save, restore

.. _tune-hyperopt:

HyperOpt (tune.suggest.hyperopt.HyperOptSearch)
-----------------------------------------------

.. autoclass:: ray.tune.suggest.hyperopt.HyperOptSearch
  :members: save, restore

.. _nevergrad:

Nevergrad (tune.suggest.nevergrad.NevergradSearch)
--------------------------------------------------

.. autoclass:: ray.tune.suggest.nevergrad.NevergradSearch
  :members: save, restore

.. _`Nevergrad README's Optimization section`: https://github.com/facebookresearch/nevergrad/blob/master/docs/optimization.rst#choosing-an-optimizer

.. _tune-optuna:

Optuna (tune.suggest.optuna.OptunaSearch)
-----------------------------------------

.. autoclass:: ray.tune.suggest.optuna.OptunaSearch

.. _`Optuna samplers`: https://optuna.readthedocs.io/en/stable/reference/samplers.html

.. _sigopt:

SigOpt (tune.suggest.sigopt.SigOptSearch)
-----------------------------------------

You will need to use the `SigOpt experiment and space specification <https://app.sigopt.com/docs/overview/create>`__ to specify your search space.

.. autoclass:: ray.tune.suggest.sigopt.SigOptSearch

.. _skopt:

Scikit-Optimize (tune.suggest.skopt.SkOptSearch)
------------------------------------------------

.. autoclass:: ray.tune.suggest.skopt.SkOptSearch
  :members: save, restore

.. _`skopt Optimizer object`: https://scikit-optimize.github.io/#skopt.Optimizer

.. _zoopt:

ZOOpt (tune.suggest.zoopt.ZOOptSearch)
--------------------------------------

.. autoclass:: ray.tune.suggest.zoopt.ZOOptSearch
  :members: save, restore

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

Custom Search Algorithms (tune.suggest.Searcher)
------------------------------------------------

If you are interested in implementing or contributing a new Search Algorithm, provide the following interface:

.. autoclass:: ray.tune.suggest.Searcher
    :members:
    :private-members:
    :show-inheritance:

.. _shim:

Shim Instantiation (tune.create_searcher)
-----------------------------------------
There is also a shim function that constructs the search algorithm based on the provided string. This can be useful if the search algorithm you want to use changes often (e.g., specifying the search algorithm via a CLI option or config file).

.. automethod:: ray.tune.create_searcher
