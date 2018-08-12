Tune Search Algorithms
======================

Tune allows you to use different search algorithms in combination with different scheduling algorithms. Currently, Tune offers the following search algorithms:

  - Grid search / Random Search
  - Tree-structured Parzen Estimators (HyperOpt)

If you are interested in implementing or contributing a new Search Algorithm, the API is straightforward:

.. autoclass:: ray.tune.suggest.SearchAlgorithm


HyperOpt Integration
~~~~~~~~~~~~~~~~~~~~
The ``HyperOptSearch`` is a SearchAlgorithm that is backed by HyperOpt to perform sequential model-based hyperparameter optimization.
In order to use this search algorithm, you will need to install HyperOpt via the following command:

.. code-block:: bash

    $ pip install --upgrade git+git://github.com/hyperopt/hyperopt.git

An example of this can be found in `hyperopt_example.py <https://github.com/ray-project/ray/blob/master/python/ray/tune/examples/hyperopt_example.py>`__.

.. note::

    The HyperOptScheduler takes an *increasing* metric in the reward attribute. If trying to minimize a loss, be sure to
    specify *mean_loss* in the function/class reporting and *reward_attr=neg_mean_loss* in the HyperOptScheduler initializer.

.. autoclass:: ray.tune.suggest.HyperOptSearch
