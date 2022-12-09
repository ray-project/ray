.. _tune-search-space:

Search Space API
================

.. _tune-sample-docs:

Random Distributions API
------------------------

This section covers the functions you can use to define your search spaces.

.. caution::

    Not all Search Algorithms support all distributions. In particular,
    ``tune.sample_from`` and ``tune.grid_search`` are often unsupported.
    The default :ref:`tune-basicvariant` supports all distributions.

.. tip::

    Avoid passing large objects as values in the search space, as that will incur a performance overhead.
    Use :ref:`tune-with-parameters` to pass large objects in or load them inside your trainable
    from disk (making sure that all nodes have access to the files) or cloud storage.
    See :ref:`tune-bottlenecks` for more information.

For a high-level overview, see this example:

.. TODO: test this

.. code-block :: python

    config = {
        # Sample a float uniformly between -5.0 and -1.0
        "uniform": tune.uniform(-5, -1),

        # Sample a float uniformly between 3.2 and 5.4,
        # rounding to multiples of 0.2
        "quniform": tune.quniform(3.2, 5.4, 0.2),

        # Sample a float uniformly between 0.0001 and 0.01, while
        # sampling in log space
        "loguniform": tune.loguniform(1e-4, 1e-2),

        # Sample a float uniformly between 0.0001 and 0.1, while
        # sampling in log space and rounding to multiples of 0.00005
        "qloguniform": tune.qloguniform(1e-4, 1e-1, 5e-5),

        # Sample a random float from a normal distribution with
        # mean=10 and sd=2
        "randn": tune.randn(10, 2),

        # Sample a random float from a normal distribution with
        # mean=10 and sd=2, rounding to multiples of 0.2
        "qrandn": tune.qrandn(10, 2, 0.2),

        # Sample a integer uniformly between -9 (inclusive) and 15 (exclusive)
        "randint": tune.randint(-9, 15),

        # Sample a random uniformly between -21 (inclusive) and 12 (inclusive (!))
        # rounding to multiples of 3 (includes 12)
        # if q is 1, then randint is called instead with the upper bound exclusive
        "qrandint": tune.qrandint(-21, 12, 3),

        # Sample a integer uniformly between 1 (inclusive) and 10 (exclusive),
        # while sampling in log space
        "lograndint": tune.lograndint(1, 10),

        # Sample a integer uniformly between 1 (inclusive) and 10 (inclusive (!)),
        # while sampling in log space and rounding to multiples of 2
        # if q is 1, then lograndint is called instead with the upper bound exclusive
        "qlograndint": tune.qlograndint(1, 10, 2),

        # Sample an option uniformly from the specified choices
        "choice": tune.choice(["a", "b", "c"]),

        # Sample from a random function, in this case one that
        # depends on another value from the search space
        "func": tune.sample_from(lambda spec: spec.config.uniform * 0.01),

        # Do a grid search over these values. Every value will be sampled
        # ``num_samples`` times (``num_samples`` is the parameter you pass to ``tune.TuneConfig``,
        # which is taken in by ``Tuner``)
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
