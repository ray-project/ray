.. _tune-search-space-tutorial:

Working with Tune Search Spaces
===============================

Tune has a native interface for specifying search spaces.
You can specify the search space via ``tune.run(config=...)``.

Thereby, you can either use the ``tune.grid_search`` primitive to use grid search:

.. code-block:: python

    tune.run(
        trainable,
        config={"bar": tune.grid_search([True, False])})


Or you can use one of the random sampling primitives to specify distributions (:ref:`tune-sample-docs`):

.. code-block:: python

    tune.run(
        trainable,
        config={
            "param1": tune.choice([True, False]),
            "bar": tune.uniform(0, 10),
            "alpha": tune.sample_from(lambda _: np.random.uniform(100) ** 2),
            "const": "hello"  # It is also ok to specify constant values.
        })

.. caution:: If you use a SearchAlgorithm, you may not be able to specify lambdas or grid search with this
    interface, as some search algorithms may not be compatible.


To sample multiple times/run multiple trials, specify ``tune.run(num_samples=N``.
If ``grid_search`` is provided as an argument, the *same* grid will be repeated ``N`` times.

.. code-block:: python

    # 13 different configs.
    tune.run(trainable, num_samples=13, config={
        "x": tune.choice([0, 1, 2]),
        }
    )

    # 13 different configs.
    tune.run(trainable, num_samples=13, config={
        "x": tune.choice([0, 1, 2]),
        "y": tune.randn([0, 1, 2]),
        }
    )

    # 4 different configs.
    tune.run(trainable, config={"x": tune.grid_search([1, 2, 3, 4])}, num_samples=1)

    # 3 different configs.
    tune.run(trainable, config={"x": grid_search([1, 2, 3])}, num_samples=1)

    # 6 different configs.
    tune.run(trainable, config={"x": tune.grid_search([1, 2, 3])}, num_samples=2)

    # 9 different configs.
    tune.run(trainable, num_samples=1, config={
        "x": tune.grid_search([1, 2, 3]),
        "y": tune.grid_search([a, b, c])}
    )

    # 18 different configs.
    tune.run(trainable, num_samples=2, config={
        "x": tune.grid_search([1, 2, 3]),
        "y": tune.grid_search([a, b, c])}
    )

    # 45 different configs.
    tune.run(trainable, num_samples=5, config={
        "x": tune.grid_search([1, 2, 3]),
        "y": tune.grid_search([a, b, c])}
    )



Note that grid search and random search primitives are inter-operable.
Each can be used independently or in combination with each other.

.. code-block:: python

    # 6 different configs.
    tune.run(trainable, num_samples=2, config={
        "x": tune.sample_from(...),
        "y": tune.grid_search([a, b, c])
        }
    )

In the below example, ``num_samples=10`` repeats the 3x3 grid search 10 times,
for a total of 90 trials, each with randomly sampled values of ``alpha`` and ``beta``.

.. code-block:: python
   :emphasize-lines: 12

    tune.run(
        my_trainable,
        name="my_trainable",
        # num_samples will repeat the entire config 10 times.
        num_samples=10
        config={
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

.. _tune_custom-search:

How to use Custom and Conditional Search Spaces?
------------------------------------------------

You'll often run into awkward search spaces (i.e., when one hyperparameter depends on another).
Use ``tune.sample_from(func)`` to provide a **custom** callable function for generating a search space.

The parameter ``func`` should take in a ``spec`` object, which has a ``config`` namespace
from which you can access other hyperparameters.
This is useful for conditional distributions:

.. code-block:: python

    tune.run(
        ...,
        config={
            # A random function
            "alpha": tune.sample_from(lambda _: np.random.uniform(100)),
            # Use the `spec.config` namespace to access other hyperparameters
            "beta": tune.sample_from(lambda spec: spec.config.alpha * np.random.normal())
        }
    )

Here's an example showing a grid search over two nested parameters combined with random sampling from
two lambda functions, generating 9 different trials.
Note that the value of ``beta`` depends on the value of ``alpha``,
which is represented by referencing ``spec.config.alpha`` in the lambda function.
This lets you specify conditional parameter distributions.

.. code-block:: python
   :emphasize-lines: 4-11

    tune.run(
        my_trainable,
        name="my_trainable",
        config={
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
    interface (:doc:`/tune/examples/includes/optuna_define_by_run_example`).