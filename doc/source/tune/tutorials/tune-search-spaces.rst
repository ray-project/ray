.. _tune-search-space-tutorial:

Working with Tune Search Spaces
===============================

Tune has a native interface for specifying search spaces.
You can specify the search space via ``Tuner(param_space=...)``.

Thereby, you can either use the ``tune.grid_search`` primitive to use grid search:

.. code-block:: python

    tuner = tune.Tuner(
        trainable,
        param_space={"bar": tune.grid_search([True, False])})
    results = tuner.fit()


Or you can use one of the random sampling primitives to specify distributions (:ref:`tune-sample-docs`):

.. code-block:: python

    tuner = tune.Tuner(
        trainable,
        param_space={
            "param1": tune.choice([True, False]),
            "bar": tune.uniform(0, 10),
            "alpha": tune.sample_from(lambda _: np.random.uniform(100) ** 2),
            "const": "hello"  # It is also ok to specify constant values.
        })
    results = tuner.fit()

.. caution:: If you use a SearchAlgorithm, you may not be able to specify lambdas or grid search with this
    interface, as some search algorithms may not be compatible.


To sample multiple times/run multiple trials, specify ``tune.RunConfig(num_samples=N``.
If ``grid_search`` is provided as an argument, the *same* grid will be repeated ``N`` times.

.. code-block:: python

    # 13 different configs.
    tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=13), param_space={
        "x": tune.choice([0, 1, 2]),
        }
    )
    tuner.fit()

    # 13 different configs.
    tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=13), param_space={
        "x": tune.choice([0, 1, 2]),
        "y": tune.randn([0, 1, 2]),
        }
    )
    tuner.fit()

    # 4 different configs.
    tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=1), param_space={"x": tune.grid_search([1, 2, 3, 4])})
    tuner.fit()

    # 3 different configs.
    tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=1), param_space={"x": grid_search([1, 2, 3])})
    tuner.fit()

    # 6 different configs.
    tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=2), param_space={"x": tune.grid_search([1, 2, 3])})
    tuner.fit()

    # 9 different configs.
    tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=1), param_space={
        "x": tune.grid_search([1, 2, 3]),
        "y": tune.grid_search([a, b, c])}
    )
    tuner.fit()

    # 18 different configs.
    tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=2), param_space={
        "x": tune.grid_search([1, 2, 3]),
        "y": tune.grid_search([a, b, c])}
    )
    tuner.fit()

    # 45 different configs.
    tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=5), param_space={
        "x": tune.grid_search([1, 2, 3]),
        "y": tune.grid_search([a, b, c])}
    )
    tuner.fit()



Note that grid search and random search primitives are inter-operable.
Each can be used independently or in combination with each other.

.. code-block:: python

    # 6 different configs.
    tuner = tune.Tuner(trainable, tune_config=tune.TuneConfig(num_samples=2), param_space={
        "x": tune.sample_from(...),
        "y": tune.grid_search([a, b, c])
        }
    )
    tuner.fit()

In the below example, ``num_samples=10`` repeats the 3x3 grid search 10 times,
for a total of 90 trials, each with randomly sampled values of ``alpha`` and ``beta``.

.. code-block:: python
   :emphasize-lines: 12

    tuner = tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(name="my_trainable"),
        # num_samples will repeat the entire config 10 times.
        tune_config=tune.TuneConfig(num_samples=10),
        param_space={
            # ``sample_from`` creates a generator to call the lambda once per trial.
            "alpha": tune.sample_from(lambda spec: np.random.uniform(100)),
            # ``sample_from`` also supports "conditional search spaces"
            "beta": tune.sample_from(lambda spec: spec.config.alpha * np.random.normal()),
            "nn_layers": [
                # tune.grid_search will make it so that all values are evaluated.
                tune.grid_search([16, 64, 256]),
                tune.grid_search([16, 64, 256]),
            ],
        },
    )
    tuner.fit()

.. tip::

    Avoid passing large objects as values in the search space, as that will incur a performance overhead.
    Use :ref:`tune-with-parameters` to pass large objects in or load them inside your trainable
    from disk (making sure that all nodes have access to the files) or cloud storage.
    See :ref:`tune-bottlenecks` for more information.

.. _tune_custom-search:

How to use Custom and Conditional Search Spaces in Tune?
--------------------------------------------------------

You'll often run into awkward search spaces (i.e., when one hyperparameter depends on another).
Use ``tune.sample_from(func)`` to provide a **custom** callable function for generating a search space.

The parameter ``func`` should take in a ``spec`` object, which has a ``config`` namespace
from which you can access other hyperparameters.
This is useful for conditional distributions:

.. code-block:: python

    tuner = tune.Tuner(
        ...,
        param_space={
            # A random function
            "alpha": tune.sample_from(lambda _: np.random.uniform(100)),
            # Use the `spec.config` namespace to access other hyperparameters
            "beta": tune.sample_from(lambda spec: spec.config.alpha * np.random.normal())
        }
    )
    tuner.fit()

Here's an example showing a grid search over two nested parameters combined with random sampling from
two lambda functions, generating 9 different trials.
Note that the value of ``beta`` depends on the value of ``alpha``,
which is represented by referencing ``spec.config.alpha`` in the lambda function.
This lets you specify conditional parameter distributions.

.. code-block:: python
   :emphasize-lines: 4-11

    tuner = tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(name="my_trainable"),
        param_space={
            "alpha": tune.sample_from(lambda spec: np.random.uniform(100)),
            "beta": tune.sample_from(lambda spec: spec.config.alpha * np.random.normal()),
            "nn_layers": [
                tune.grid_search([16, 64, 256]),
                tune.grid_search([16, 64, 256]),
            ],
        }
    )

.. note::

    This format is not supported by every SearchAlgorithm, and only some SearchAlgorithms, like :ref:`HyperOpt <tune-hyperopt>`
    and :ref:`Optuna <tune-optuna>`, handle conditional search spaces at all.

    In order to use conditional search spaces with :ref:`HyperOpt <tune-hyperopt>`,
    a `Hyperopt search space <http://hyperopt.github.io/hyperopt/getting-started/search_spaces/>`_ isnecessary.
    :ref:`Optuna <tune-optuna>` supports conditional search spaces through its define-by-run
    interface (:doc:`/tune/examples/optuna_example`).
