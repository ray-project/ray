Tune Search Algorithms
======================

Tune provides various hyperparameter search algorithms to efficiently optimize your model. Tune allows you to use different search algorithms in combination with different trial schedulers. Tune will by default implicitly use the Variant Generation algorithm to create trials.

.. important:: If you **explicitly** specify a Search Algorithm, you will need to pass in the experiment configuration into the Search Algorithm initializer.

You can utilize these search algorithms as follows:

.. code-block:: python

    run_experiments(search_alg=SearchAlgorithm(experiments, ... ))


Currently, Tune offers the following search algorithms:

.. contents::
    :local:
    :backlinks: none


Variant Generation (Grid Search/Random Search)
----------------------------------------------

By default, Tune uses the `Variant Generation process <tune-usage.html#tune-search-space-default>`__ to create and queue trials. This supports random search and grid search as specified by the ``config`` parameter of the Experiment.

The below two lines are equivalent:

.. code-block:: python

    run_experiments(experiment_config, ... )
    run_experiments(search_alg=BasicVariantGeneration(experiment_config))


.. autoclass:: ray.tune.suggest.BasicVariantGenerator
    :show-inheritance:
    :noindex:


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

.. autoclass:: ray.tune.suggest.HyperOptSearch
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

Often times, hyperparameter search algorithms are model-based and may be quite simple to implement. For this, one can extend the following abstract class and implement ``on_trial_result``, ``on_trial_complete``, and ``_suggest``. The abstract class will take care of Tune-specific boilerplate such as creating Trials and queuing trials:

.. autoclass:: ray.tune.suggest.SuggestionAlgorithm
    :show-inheritance:
    :noindex:

    .. automethod:: ray.tune.suggest.SuggestionAlgorithm._suggest
        :noindex:
