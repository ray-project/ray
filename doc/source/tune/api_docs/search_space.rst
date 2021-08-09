.. _tune-search-space:

Search Space API
================
Overview
--------

Tune has a native interface for specifying search spaces. You can specify the search space via ``tune.run(config=...)``.

Thereby, you can either use the ``tune.grid_search`` primitive to specify an axis of a grid search...

.. code-block:: python

    tune.run(
        trainable,
        config={"bar": tune.grid_search([True, False])})


... or one of the random sampling primitives to specify distributions (:ref:`tune-sample-docs`):

.. code-block:: python

    tune.run(
        trainable,
        config={
            "param1": tune.choice([True, False]),
            "bar": tune.uniform(0, 10),
            "alpha": tune.sample_from(lambda _: np.random.uniform(100) ** 2),
            "const": "hello"  # It is also ok to specify constant values.
        })



.. caution:: If you use a Search Algorithm, you may not be able to specify lambdas or grid search with this
    interface, as some search algorithms may not be compatible.


To sample multiple times/run multiple trials, specify ``tune.run(num_samples=N``. If ``grid_search`` is provided as an argument, the *same* grid will be repeated ``N`` times.

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



Note that grid search and random search primitives are inter-operable. Each can be used independently or in combination with each other.

.. code-block:: python

    # 6 different configs.
    tune.run(trainable, num_samples=2, config={
        "x": tune.sample_from(...),
        "y": tune.grid_search([a, b, c])
        }
    )

In the below example, ``num_samples=10`` repeats the 3x3 grid search 10 times, for a total of 90 trials, each with randomly sampled values of ``alpha`` and ``beta``.

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

Custom/Conditional Search Spaces
--------------------------------

You'll often run into awkward search spaces (i.e., when one hyperparameter depends on another). Use ``tune.sample_from(func)`` to provide a **custom** callable function for generating a search space.

The parameter ``func`` should take in a ``spec`` object, which has a ``config`` namespace from which you can access other hyperparameters. This is useful for conditional distributions:

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

Here's an example showing a grid search over two nested parameters combined with random sampling from two lambda functions, generating 9 different trials. Note that the value of ``beta`` depends on the value of ``alpha``, which is represented by referencing ``spec.config.alpha`` in the lambda function. This lets you specify conditional parameter distributions.

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

.. _tune-sample-docs:

Random Distributions API
------------------------

This section covers the functions you can use to define your search spaces.

For a high-level overview, see this example:

.. code-block :: python

    config = {
        # Sample a float uniformly between -5.0 and -1.0
        "uniform": tune.uniform(-5, -1),

        # Sample a float uniformly between 3.2 and 5.4,
        # rounding to increments of 0.2
        "quniform": tune.quniform(3.2, 5.4, 0.2),

        # Sample a float uniformly between 0.0001 and 0.01, while
        # sampling in log space
        "loguniform": tune.loguniform(1e-4, 1e-2),

        # Sample a float uniformly between 0.0001 and 0.1, while
        # sampling in log space and rounding to increments of 0.00005
        "qloguniform": tune.qloguniform(1e-4, 1e-1, 5e-5),

        # Sample a random float from a normal distribution with
        # mean=10 and sd=2
        "randn": tune.randn(10, 2),

        # Sample a random float from a normal distribution with
        # mean=10 and sd=2, rounding to increments of 0.2
        "qrandn": tune.qrandn(10, 2, 0.2),

        # Sample a integer uniformly between -9 (inclusive) and 15 (exclusive)
        "randint": tune.randint(-9, 15),

        # Sample a random uniformly between -21 (inclusive) and 12 (inclusive (!))
        # rounding to increments of 3 (includes 12)
        "qrandint": tune.qrandint(-21, 12, 3),

        # Sample a integer uniformly between 1 (inclusive) and 10 (exclusive),
        # while sampling in log space
        "lograndint": tune.lograndint(1, 10),

        # Sample a integer uniformly between 1 (inclusive) and 10 (inclusive (!)),
        # while sampling in log space and rounding to increments of 2
        "qlograndint": tune.qlograndint(1, 10, 2),

        # Sample an option uniformly from the specified choices
        "choice": tune.choice(["a", "b", "c"]),

        # Sample from a random function, in this case one that
        # depends on another value from the search space
        "func": tune.sample_from(lambda spec: spec.config.uniform * 0.01),

        # Do a grid search over these values. Every value will be sampled
        # `num_samples` times (`num_samples` is the parameter you pass to `tune.run()`)
        "grid": tune.grid_search([32, 64, 128])
    }

tune.uniform
~~~~~~~~~~~~

.. autofunction:: ray.tune.uniform

tune.quniform
~~~~~~~~~~~~~

.. autofunction:: ray.tune.quniform

tune.loguniform
~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.loguniform

tune.qloguniform
~~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.qloguniform

tune.randn
~~~~~~~~~~

.. autofunction:: ray.tune.randn

tune.qrandn
~~~~~~~~~~~

.. autofunction:: ray.tune.qrandn

tune.randint
~~~~~~~~~~~~

.. autofunction:: ray.tune.randint

tune.qrandint
~~~~~~~~~~~~~

.. autofunction:: ray.tune.qrandint

tune.lograndint
~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.lograndint

tune.qlograndint
~~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.qlograndint

tune.choice
~~~~~~~~~~~

.. autofunction:: ray.tune.choice

tune.sample_from
~~~~~~~~~~~~~~~~

.. autofunction:: ray.tune.sample_from

Grid Search API
---------------

.. autofunction:: ray.tune.grid_search

References
----------

See also :ref:`tune-basicvariant`.
