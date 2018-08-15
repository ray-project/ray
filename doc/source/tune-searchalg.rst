Tune Search Algorithms
======================

Tune allows you to use different search algorithms in combination with different trial schedulers. Tune will by default implicitly use the Variant Generation algorithm to create trials.

Note that if you **explicitly** specify a Search Algorithm, you will need to pass in the experiment configuration into the Search Algorithm initializer. You can utilize these search algorithms as follows:

.. code-block:: python

    run_experiments(search_alg=SearchAlgorithm(experiments, ... ))


Currently, Tune offers the following search algorithms:

.. contents::
    :local:


Variant Generation (Grid Search/Random Search)
----------------------------------------------

By default, Tune uses the `Variant Generation process <tune-usage.html#config-variant-generation>`__ to create and queue trials. This supports random search and grid search as specified by the ``config`` parameter of the Experiment.

.. code-block:: python


    # The below two lines are equivalent

    run_experiments(experiment_config, ... )
    run_experiments(search_alg=BasicVariantGeneration(experiment_config))


.. autoclass:: ray.tune.suggest.BasicVariantGenerator
    :show-inheritance:


HyperOpt Search (Tree-structured Parzen Estimators)
---------------------------------------------------

The ``HyperOptSearch`` is a SearchAlgorithm that is backed by `HyperOpt <http://hyperopt.github.io/hyperopt>`__ to perform sequential model-based hyperparameter optimization.
In order to use this search algorithm, you will need to install HyperOpt via the following command:

.. code-block:: bash

    $ pip install --upgrade git+git://github.com/hyperopt/hyperopt.git

This algorithm would require using the HyperOpt default search space. You can use HyperOptSearch like follows:

.. code-block:: python

    run_experiments(search_alg=HyperOptSearch(experiment_config, hyperopt_space, ... ))

An example of this can be found in `hyperopt_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperopt_example.py>`__.

.. note::
    The HyperOptScheduler takes an *increasing* metric in the reward attribute.

.. autoclass:: ray.tune.suggest.HyperOptSearch
    :show-inheritance:


Contributing a New Algorithm
----------------------------

If you are interested in implementing or contributing a new Search Algorithm, the API is straightforward:

.. autoclass:: ray.tune.suggest.SearchAlgorithm
    :members:

Model-Based Suggestion Algorithms
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Often times, hyperparameter search algorithms are model-based and may be quite simple to implement. For this, one can extend the following abstract class, which takes care of Tune-specific boilerplate such as creating Trials and queuing trials:

.. autoclass:: ray.tune.suggest.SuggestionAlgorithm
    :show-inheritance:

    .. automethod:: ray.tune.suggest.SuggestionAlgorithm._suggest
