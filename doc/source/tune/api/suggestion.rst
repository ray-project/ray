.. _tune-search-alg:

Tune Search Algorithms (tune.search)
====================================

Tune's Search Algorithms are wrappers around open-source optimization libraries for efficient hyperparameter selection.
Each library has a specific way of defining the search space - refer to their documentation for more details.
Tune automatically converts search spaces passed to ``Tuner`` to the library format in most cases.

You can utilize these search algorithms as follows:

.. code-block:: python

    from ray import tune
    from ray.tune.search.optuna import OptunaSearch

    def train_fn(config):
        # This objective function is just for demonstration purposes
        tune.report({"loss": config["param"]})

    tuner = tune.Tuner(
        train_fn,
        tune_config=tune.TuneConfig(
            search_alg=OptunaSearch(),
            num_samples=100,
            metric="loss",
            mode="min",
        ),
        param_space={"param": tune.uniform(0, 1)},
    )
    results = tuner.fit()


Saving and Restoring Tune Search Algorithms
-------------------------------------------

.. TODO: what to do about this section? It doesn't really belong here and is not worth its own guide.
.. TODO: at least check that this pseudo-code runs.

Certain search algorithms have ``save/restore`` implemented,
allowing reuse of searchers that are fitted on the results of multiple tuning runs.

.. code-block:: python

    search_alg = HyperOptSearch()

    tuner_1 = tune.Tuner(
        train_fn,
        tune_config=tune.TuneConfig(search_alg=search_alg)
    )
    results_1 = tuner_1.fit()

    search_alg.save("./my-checkpoint.pkl")

    # Restore the saved state onto another search algorithm,
    # in a new tuning script

    search_alg2 = HyperOptSearch()
    search_alg2.restore("./my-checkpoint.pkl")

    tuner_2 = tune.Tuner(
        train_fn,
        tune_config=tune.TuneConfig(search_alg=search_alg2)
    )
    results_2 = tuner_2.fit()

Tune automatically saves searcher state inside the current experiment folder during tuning.
See ``Result logdir: ...`` in the output logs for this location.

Note that if you have two Tune runs with the same experiment folder,
the previous state checkpoint is overwritten. You can
avoid this by making sure ``RunConfig(name=...)`` is set to a unique
identifier:

.. code-block:: python

    search_alg = HyperOptSearch()
    tuner_1 = tune.Tuner(
        train_fn,
        tune_config=tune.TuneConfig(
            num_samples=5,
            search_alg=search_alg,
        ),
        run_config=tune.RunConfig(
            name="my-experiment-1",
            storage_path="~/my_results",
        )
    )
    results = tuner_1.fit()

    search_alg2 = HyperOptSearch()
    search_alg2.restore_from_dir(
      os.path.join("~/my_results", "my-experiment-1")
    )

.. _tune-basicvariant:

.. vale Google.Spacing = NO

Random search and grid search (tune.search.basic_variant.BasicVariantGenerator)
-------------------------------------------------------------------------------

.. vale Google.Spacing = YES

The default and most basic way to do hyperparameter search is through random and grid search.
Ray Tune does this through the :class:`BasicVariantGenerator <ray.tune.search.basic_variant.BasicVariantGenerator>`
class that generates trial variants given a search space definition.

The :class:`BasicVariantGenerator <ray.tune.search.basic_variant.BasicVariantGenerator>` is used per
default if no search algorithm is passed to
:func:`Tuner <ray.tune.Tuner>`.

.. currentmodule:: ray.tune.search

.. autosummary::
    :nosignatures:
    :toctree: doc/

    basic_variant.BasicVariantGenerator

.. _tune-ax:

.. vale Google.Spacing = NO

Ax (tune.search.ax.AxSearch)
----------------------------

.. vale Google.Spacing = YES

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ax.AxSearch

.. _bayesopt:

.. vale Google.Spacing = NO

Bayesian Optimization (tune.search.bayesopt.BayesOptSearch)
-----------------------------------------------------------

.. vale Google.Spacing = YES

.. autosummary::
    :nosignatures:
    :toctree: doc/

    bayesopt.BayesOptSearch

.. _suggest-TuneBOHB:

.. vale Google.Spacing = NO

Bayesian Optimization HyperBand (tune.search.bohb.TuneBOHB)
--------------------------------

.. vale Google.Spacing = YES

Bayesian Optimization HyperBand (BOHB) is an algorithm that both terminates bad trials
and also uses Bayesian Optimization to improve the hyperparameter search.
It's available from the `HpBandSter library <https://github.com/automl/HpBandSter>`_.

Importantly, BOHB is intended to be paired with a specific scheduler class: :ref:`HyperBandForBOHB <tune-scheduler-bohb>`.

To use this search algorithm, install ``HpBandSter`` and ``ConfigSpace``:

.. code-block:: bash

    $ pip install hpbandster ConfigSpace

See the `BOHB paper <https://arxiv.org/abs/1807.01774>`_ for more details.

.. autosummary::
    :nosignatures:
    :toctree: doc/

    bohb.TuneBOHB

.. _tune-hebo:

.. vale Google.Spacing = NO

Heteroscedastic and Evolutionary Bayesian Optimisation (tune.search.hebo.HEBOSearch)
----------------------------------

.. vale Google.Spacing = YES

Heteroscedastic and Evolutionary Bayesian Optimisation (HEBO) is a Bayesian optimization library.
.. autosummary::
    :nosignatures:
    :toctree: doc/

    hebo.HEBOSearch

.. _tune-hyperopt:

.. vale Google.Spacing = NO

Hyperopt (tune.search.hyperopt.HyperOptSearch)
----------------------------------------------

.. vale Google.Spacing = YES

.. autosummary::
    :nosignatures:
    :toctree: doc/

    hyperopt.HyperOptSearch

.. _nevergrad:

.. vale Google.Spacing = NO

Nevergrad (tune.search.nevergrad.NevergradSearch)
-------------------------------------------------

.. vale Google.Spacing = YES

.. autosummary::
    :nosignatures:
    :toctree: doc/

    nevergrad.NevergradSearch

.. _tune-optuna:

.. vale Google.Spacing = NO

Optuna (tune.search.optuna.OptunaSearch)
----------------------------------------

.. vale Google.Spacing = YES

.. autosummary::
    :nosignatures:
    :toctree: doc/

    optuna.OptunaSearch


.. _zoopt:

.. vale Google.Spacing = NO

ZOOpt (tune.search.zoopt.ZOOptSearch)
-------------------------------------

.. vale Google.Spacing = YES

.. autosummary::
    :nosignatures:
    :toctree: doc/

    zoopt.ZOOptSearch

.. _repeater:

.. vale Google.Spacing = NO

Repeated Evaluations (tune.search.Repeater)
-------------------------------------------

.. vale Google.Spacing = YES

Use ``ray.tune.search.Repeater`` to average over multiple evaluations of the same
hyperparameter configurations. This is useful in cases where the evaluated
training procedure has high variance (that's in reinforcement learning).

By default, ``Repeater`` takes in a ``repeat`` parameter and a ``search_alg``.
The ``search_alg`` suggests new configurations to try, and the ``Repeater``
runs ``repeat`` trials of the configuration. It then averages the
``search_alg.metric`` from the final results of each repeated trial.


.. warning:: It's recommended to not use ``Repeater`` with a TrialScheduler.
    Early termination can negatively affect the average reported metric.

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Repeater

.. _limiter:

.. vale Google.Spacing = NO

ConcurrencyLimiter (tune.search.ConcurrencyLimiter)
---------------------------------------------------

.. vale Google.Spacing = YES

Use ``ray.tune.search.ConcurrencyLimiter`` to limit the amount of concurrency when using a search algorithm.
This is useful when a given optimization algorithm doesn't parallelize very well (such as a naive Bayesian Optimization).

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ConcurrencyLimiter

.. _byo-algo:

.. vale Google.Spacing = NO

Custom Search Algorithms (tune.search.Searcher)
-----------------------------------------------

.. vale Google.Spacing = YES

If you are interested in implementing or contributing a new Search Algorithm, provide the following interface:

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Searcher

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Searcher.suggest
    Searcher.save
    Searcher.restore
    Searcher.on_trial_result
    Searcher.on_trial_complete

If contributing, make sure to add test cases and an entry in the function described below.

.. _shim:

Shim Instantiation (tune.create_searcher)
-----------------------------------------
There is also a shim function that constructs the search algorithm based on the provided string.
This can be useful if the search algorithm you want to use changes often
(for example, specifying the search algorithm through a CLI option or config file).

.. autosummary::
    :nosignatures:
    :toctree: doc/

    create_searcher
