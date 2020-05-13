.. _tune-search-alg:

Tune Search Algorithms
======================

Tune provides various hyperparameter search algorithms to efficiently optimize your model. Tune allows you to use different search algorithms in combination with different trial schedulers. Tune will by default implicitly use the Variant Generation algorithm to create trials.

You can utilize these search algorithms as follows:

.. code-block:: python

    tune.run(my_function, search_alg=SearchAlgorithm(...))

Currently, Tune offers the following search algorithms (and library integrations):

- `Grid Search and Random Search <tune-searchalg.html#variant-generation-grid-search-random-search>`__
- `BayesOpt <tune-searchalg.html#bayesopt-search>`__
- `HyperOpt <tune-searchalg.html#hyperopt-search-tree-structured-parzen-estimators>`__
- `SigOpt <tune-searchalg.html#sigopt-search>`__
- `Nevergrad <tune-searchalg.html#nevergrad-search>`__
- `Scikit-Optimize <tune-searchalg.html#scikit-optimize-search>`__
- `Ax <tune-searchalg.html#ax-search>`__
- `BOHB <tune-searchalg.html#bohb>`__


Variant Generation (Grid Search/Random Search)
----------------------------------------------

By default, Tune uses a BasicVariantGenerator to sample trials. This supports random search and grid search as specified by the ``config`` parameter of ``tune.run``.

.. autoclass:: ray.tune.suggest.BasicVariantGenerator
    :show-inheritance:
    :noindex:

Read about this in the :ref:`Grid/Random Search API <tune-grid-random>`.

Note that other search algorithms will require a different search space declaration than the default Tune format.


Repeated Evaluations
--------------------

Use ``ray.tune.suggest.Repeater`` to average over multiple evaluations of the same
hyperparameter configurations. This is useful in cases where the evaluated
training procedure has high variance (i.e., in reinforcement learning).

By default, ``Repeater`` will take in a ``repeat`` parameter and a ``search_alg``.
The ``search_alg`` will suggest new configurations to try, and the ``Repeater``
will run ``repeat`` trials of the configuration. It will then average the
``search_alg.metric`` from the final results of each repeated trial.

See the API documentation (:ref:`repeater-doc`) for more details.

.. code-block:: python

    from ray.tune.suggest import Repeater

    search_alg = BayesOpt(...)
    re_search_alg = Repeater(search_alg, repeat=10)

    # Repeat 2 samples 10 times each.
    tune.run(trainable, num_samples=20, search_alg=re_search_alg)

.. note:: This does not apply for grid search and random search.
.. warning:: It is recommended to not use ``Repeater`` with a TrialScheduler.
    Early termination can negatively affect the average reported metric.


BayesOpt Search
---------------

The ``BayesOptSearch`` is a SearchAlgorithm that is backed by the `bayesian-optimization <https://github.com/fmfn/BayesianOptimization>`__ package to perform sequential model-based hyperparameter optimization. Note that this class does not extend ``ray.tune.suggest.BasicVariantGenerator``, so you will not be able to use Tune's default variant generation/search space declaration when using BayesOptSearch.

In order to use this search algorithm, you will need to install Bayesian Optimization via the following command:

.. code-block:: bash

    $ pip install bayesian-optimization

This algorithm requires `setting a search space and defining a utility function <https://github.com/fmfn/BayesianOptimization/blob/master/examples/advanced-tour.ipynb>`__. You can use BayesOptSearch like follows:

.. code-block:: python

    tune.run(... , search_alg=BayesOptSearch(bayesopt_space, utility_kwargs=utility_params, ... ))

An example of this can be found in `bayesopt_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/bayesopt_example.py>`__.

.. autoclass:: ray.tune.suggest.bayesopt.BayesOptSearch
    :show-inheritance:
    :noindex:

.. _tune-hyperopt:

HyperOpt Search (Tree-structured Parzen Estimators)
---------------------------------------------------

The ``HyperOptSearch`` is a SearchAlgorithm that is backed by `HyperOpt <http://hyperopt.github.io/hyperopt>`__ to perform sequential model-based hyperparameter optimization. Note that this class does not extend ``ray.tune.suggest.BasicVariantGenerator``, so you will not be able to use Tune's default variant generation/search space declaration when using HyperOptSearch.

In order to use this search algorithm, you will need to install HyperOpt via the following command:

.. code-block:: bash

    $ pip install --upgrade git+git://github.com/hyperopt/hyperopt.git

This algorithm requires using the `HyperOpt search space specification <https://github.com/hyperopt/hyperopt/wiki/FMin>`__. You can use HyperOptSearch like follows:

.. code-block:: python

    tune.run(... , search_alg=HyperOptSearch(hyperopt_space, ... ))

An example of this can be found in `hyperopt_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperopt_example.py>`__.

.. autoclass:: ray.tune.suggest.hyperopt.HyperOptSearch
    :show-inheritance:
    :noindex:


SigOpt Search
-------------

The ``SigOptSearch`` is a SearchAlgorithm that is backed by `SigOpt <https://sigopt.com/>`__ to perform sequential model-based hyperparameter optimization. Note that this class does not extend ``ray.tune.suggest.BasicVariantGenerator``, so you will not be able to use Tune's default variant generation/search space declaration when using SigOptSearch.

In order to use this search algorithm, you will need to install SigOpt via the following command:

.. code-block:: bash

    $ pip install sigopt

This algorithm requires the user to have a `SigOpt API key <https://app.sigopt.com/docs/overview/authentication>`__ to make requests to the API. Store the API token as an environment variable named ``SIGOPT_KEY`` like follows:

.. code-block:: bash

    $ export SIGOPT_KEY= ...

This algorithm requires using the `SigOpt experiment and space specification <https://app.sigopt.com/docs/overview/create>`__. You can use SigOptSearch like follows:

.. code-block:: python

    tune.run(... , search_alg=SigOptSearch(sigopt_space, ... ))

An example of this can be found in `sigopt_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/sigopt_example.py>`__.

.. autoclass:: ray.tune.suggest.sigopt.SigOptSearch
    :show-inheritance:
    :noindex:

.. _tune-nevergrad:

Nevergrad Search
----------------

The ``NevergradSearch`` is a SearchAlgorithm that is backed by `Nevergrad <https://github.com/facebookresearch/nevergrad>`__ to perform sequential model-based hyperparameter optimization. Note that this class does not extend ``ray.tune.suggest.BasicVariantGenerator``, so you will not be able to use Tune's default variant generation/search space declaration when using NevergradSearch.

In order to use this search algorithm, you will need to install Nevergrad via the following command.:

.. code-block:: bash

    $ pip install nevergrad

Keep in mind that ``nevergrad`` is a Python 3.6+ library.

This algorithm requires using an optimizer provided by ``nevergrad``, of which there are many options. A good rundown can be found on their README's `Optimization <https://github.com/facebookresearch/nevergrad/blob/master/docs/optimization.rst#choosing-an-optimizer>`__ section. You can use ``NevergradSearch`` like follows:

.. code-block:: python

    tune.run(... , search_alg=NevergradSearch(optimizer, parameter_names, ... ))

An example of this can be found in `nevergrad_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/nevergrad_example.py>`__.

.. autoclass:: ray.tune.suggest.nevergrad.NevergradSearch
    :show-inheritance:
    :noindex:

Scikit-Optimize Search
----------------------

The ``SkOptSearch`` is a SearchAlgorithm that is backed by `Scikit-Optimize <https://scikit-optimize.github.io>`__ to perform sequential model-based hyperparameter optimization. Note that this class does not extend ``ray.tune.suggest.BasicVariantGenerator``, so you will not be able to use Tune's default variant generation/search space declaration when using SkOptSearch.

In order to use this search algorithm, you will need to install Scikit-Optimize via the following command:

.. code-block:: bash

    $ pip install scikit-optimize

This algorithm requires using the `Scikit-Optimize ask and tell interface <https://scikit-optimize.github.io/stable/auto_examples/ask-and-tell.html>`__. This interface requires using the `Optimizer <https://scikit-optimize.github.io/#skopt.Optimizer>`__ provided by Scikit-Optimize. You can use SkOptSearch like follows:

.. code-block:: python

    optimizer = Optimizer(dimension, ...)
    tune.run(... , search_alg=SkOptSearch(optimizer, parameter_names, ... ))

An example of this can be found in `skopt_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/skopt_example.py>`__.

.. autoclass:: ray.tune.suggest.skopt.SkOptSearch
    :show-inheritance:
    :noindex:

Dragonfly Search
----------------

The ``DragonflySearch`` is a SearchAlgorithm that is backed by `Dragonfly <https://github.com/dragonfly/dragonfly>`__ to perform sequential Bayesian optimization. Note that this class does not extend ``ray.tune.suggest.BasicVariantGenerator``, so you will not be able to use Tune's default variant generation/search space declaration when using DragonflySearch.

.. code-block:: bash

    $ pip install dragonfly-opt

This algorithm requires using the `Dragonfly ask and tell interface <https://dragonfly-opt.readthedocs.io/en/master/getting_started_ask_tell/>`__. This interface requires using FunctionCallers and optimizers provided by Dragonfly. You can use `DragonflySearch` like follows:

.. code-block:: python

    from dragonfly.opt.gp_bandit import EuclideanGPBandit
    from dragonfly.exd.experiment_caller import EuclideanFunctionCaller
    from dragonfly import load_config
    domain_config = load_config({'domain': ...})
    func_caller = EuclideanFunctionCaller(None, domain_config.domain.list_of_domains[0])
    optimizer = EuclideanGPBandit(func_caller, ask_tell_mode=True)
    algo = DragonflySearch(optimizer, ...)

An example of this can be found in `dragonfly_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/dragonfly_example.py>`__.

.. autoclass:: ray.tune.suggest.dragonfly.DragonflySearch
    :show-inheritance:
    :noindex:

.. _tune-ax:

Ax Search
---------

The ``AxSearch`` is a SearchAlgorithm that is backed by `Ax <https://ax.dev/>`__ to perform sequential model-based hyperparameter optimization. Ax is a platform for understanding, managing, deploying, and automating adaptive experiments. Ax provides an easy to use interface with BoTorch, a flexible, modern library for Bayesian optimization in PyTorch. Note that this class does not extend ``ray.tune.suggest.BasicVariantGenerator``, so you will not be able to use Tune's default variant generation/search space declaration when using AxSearch.

In order to use this search algorithm, you will need to install PyTorch, Ax, and sqlalchemy. Instructions to install PyTorch locally can be found `here <https://pytorch.org/get-started/locally/>`__. You can install Ax and sqlalchemy via the following command:

.. code-block:: bash

    $ pip install ax-platform sqlalchemy

This algorithm requires specifying a search space and objective. You can use `AxSearch` like follows:

.. code-block:: python

    client = AxClient(enforce_sequential_optimization=False)
    client.create_experiment( ... )
    tune.run(... , search_alg=AxSearch(client))

An example of this can be found in `ax_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/ax_example.py>`__.

.. autoclass:: ray.tune.suggest.ax.AxSearch
    :show-inheritance:
    :noindex:

BOHB
----

.. tip:: This implementation is still experimental. Please report issues on https://github.com/ray-project/ray/issues/. Thanks!

``BOHB`` (Bayesian Optimization HyperBand) is a SearchAlgorithm that is backed by `HpBandSter <https://github.com/automl/HpBandSter>`__ to perform sequential model-based hyperparameter optimization in conjunction with HyperBand. Note that this class does not extend ``ray.tune.suggest.BasicVariantGenerator``, so you will not be able to use Tune's default variant generation/search space declaration when using BOHB.

Importantly, BOHB is intended to be paired with a specific scheduler class: `HyperBandForBOHB <tune-schedulers.html#hyperband-bohb>`__.

This algorithm requires using the `ConfigSpace search space specification <https://automl.github.io/HpBandSter/build/html/quickstart.html#searchspace>`_. In order to use this search algorithm, you will need to install ``HpBandSter`` and ``ConfigSpace``:

.. code-block:: bash

    $ pip install hpbandster ConfigSpace


You can use ``TuneBOHB`` in conjunction with ``HyperBandForBOHB`` as follows:

.. code-block:: python

    # BOHB uses ConfigSpace for their hyperparameter search space
    import ConfigSpace as CS

    config_space = CS.ConfigurationSpace()
    config_space.add_hyperparameter(
        CS.UniformFloatHyperparameter("height", lower=10, upper=100))
    config_space.add_hyperparameter(
        CS.UniformFloatHyperparameter("width", lower=0, upper=100))

    experiment_metrics = dict(metric="episode_reward_mean", mode="min")
    bohb_hyperband = HyperBandForBOHB(
        time_attr="training_iteration", max_t=100, **experiment_metrics)
    bohb_search = TuneBOHB(
        config_space, max_concurrent=4, **experiment_metrics)

    tune.run(MyTrainableClass,
        name="bohb_test",
        scheduler=bohb_hyperband,
        search_alg=bohb_search,
        num_samples=5)

Take a look at `an example here <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/bohb_example.py>`_. See the `BOHB paper <https://arxiv.org/abs/1807.01774>`_ for more details.

.. autoclass:: ray.tune.suggest.bohb.TuneBOHB
    :show-inheritance:
    :noindex:

ZOOpt Search
------------

The ``ZOOptSearch`` is a SearchAlgorithm for derivative-free optimization. It is backed by the `ZOOpt <https://github.com/polixir/ZOOpt>`__ package. Currently, Asynchronous Sequential RAndomized COordinate Shrinking (ASRacos) algorithm is implemented in Tune. Note that this class does not extend ``ray.tune.suggest.BasicVariantGenerator``, so you will not be able to use Tune’s default variant generation/search space declaration when using ZOOptSearch.

In order to use this search algorithm, you will need to install the ZOOpt package **(>=0.4.0)** via the following command:

.. code-block:: bash

    $ pip install -U zoopt

Keep in mind that zoopt only supports Python 3.

This algorithm allows users to mix continuous dimensions and discrete dimensions, for example:

.. code-block:: python

    dim_dict = {
        # for continuous dimensions: (continuous, search_range, precision)
        "height": (ValueType.CONTINUOUS, [-10, 10], 1e-2),
        # for discrete dimensions: (discrete, search_range, has_order)
        "width": (ValueType.DISCRETE, [-10, 10], False)
    }

    config = {
        "num_samples": 200 if args.smoke_test else 1000,
        "config": {
            "iterations": 10,  # evaluation times
        },
        "stop": {
            "timesteps_total": 10  # cumstom stop rules
        }
    }

    zoopt_search = ZOOptSearch(
        algo="Asracos",  # only support ASRacos currently
        budget=config["num_samples"],
        dim_dict=dim_dict,
        max_concurrent=4,
        metric="mean_loss",
        mode="min")

    run(my_objective,
        search_alg=zoopt_search,
        name="zoopt_search",
        **config)

An example of this can be found in `zoopt_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/zoopt_example.py>`__.

.. autoclass:: ray.tune.suggest.zoopt.ZOOptSearch
    :show-inheritance:
    :noindex:

Contributing a New Algorithm
----------------------------

If you are interested in implementing or contributing a new Search Algorithm, the API is straightforward:

.. autoclass:: ray.tune.suggest.SearchAlgorithm
    :members:
    :noindex:

Model-Based Suggestion Algorithms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Often times, hyperparameter search algorithms are model-based and may be quite simple to implement. For this, one can extend the following abstract class and implement ``on_trial_complete``, and ``suggest``.

.. autoclass:: ray.tune.suggest.Searcher
    :show-inheritance:
    :noindex:
