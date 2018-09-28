Tune Search Algorithms
======================

Tune provides various hyperparameter search algorithms to efficiently optimize your model. Tune allows you to use different search algorithms in combination with different trial schedulers. Tune will by default implicitly use the Variant Generation algorithm to create trials.

You can utilize these search algorithms as follows:

.. code-block:: python

    run_experiments(experiments, search_alg=SearchAlgorithm(...))

Currently, Tune offers the following search algorithms:

- `Grid Search and Random Search <tune-searchalg.html#variant-generation-grid-search-random-search>`__
- `HyperOpt <tune-searchalg.html#hyperopt-search-tree-structured-parzen-estimators>`__


Variant Generation (Grid Search/Random Search)
----------------------------------------------

By default, Tune uses the `default search space and variant generation process <tune-usage.html#tune-search-space-default>`__ to create and queue trials. This supports random search and grid search as specified by the ``config`` parameter of the Experiment.

.. autoclass:: ray.tune.suggest.BasicVariantGenerator
    :show-inheritance:
    :noindex:


Note that other search algorithms will not necessarily extend this class and may require a different search space declaration than the default Tune format.

HyperOpt Search (Tree-structured Parzen Estimators)
---------------------------------------------------

The ``HyperOptSearch`` is a SearchAlgorithm that is backed by `HyperOpt <http://hyperopt.github.io/hyperopt>`__ to perform sequential model-based hyperparameter optimization.
In order to use this search algorithm, you will need to install HyperOpt via the following command:

.. code-block:: bash

    $ pip install --upgrade git+git://github.com/hyperopt/hyperopt.git

This algorithm requires using the `HyperOpt search space specification <https://github.com/hyperopt/hyperopt/wiki/FMin>`__. You can use HyperOptSearch like follows:

.. code-block:: python

    run_experiments(experiment_config, search_alg=HyperOptSearch(hyperopt_space, ... ))

An example of this can be found in `hyperopt_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperopt_example.py>`__.

.. autoclass:: ray.tune.suggest.HyperOptSearch
    :show-inheritance:
    :noindex:

Note that this class does not extend ``ray.tune.suggest.BasicVariantGenerator``, so you will not be able to use Tune's default variant generation/search space declaration when using HyperOptSearch.

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
