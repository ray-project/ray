.. _writing-code-snippets:

==========================
How to write code snippets
==========================

Users learn from example. So, whether you're writing a docstring or a user guide,
include examples that illustrate the relevant APIs. Your examples should run
out-of-the-box so that users can copy them and adapt them to their own needs.

This page describes how to write code snippets so that they're tested in CI.

.. note::
    The examples in this guide use reStructuredText. If you're writing
    Markdown, use MyST syntax. To learn more, read the
    `MyST documentation <https://myst-parser.readthedocs.io/en/latest/syntax/roles-and-directives.html#directives-a-block-level-extension-point>`_.

-----------------
Types of examples
-----------------

There are three types of examples: *doctest-style*, *code-output-style*, and *literalinclude*.

*doctest-style* examples
========================

*doctest-style* examples mimic interactive Python sessions. ::

    .. doctest::

        >>> def is_even(x):
        ...     return (x % 2) == 0
        >>> is_even(0)
        True
        >>> is_even(1)
        False

They're rendered like this:

.. doctest::

    >>> def is_even(x):
    ...     return (x % 2) == 0
    >>> is_even(0)
    True
    >>> is_even(1)
    False

.. tip::

    If you're writing docstrings, exclude `.. doctest::` to simplify your code. ::

        Example:
            >>> def is_even(x):
            ...     return (x % 2) == 0
            >>> is_even(0)
            True
            >>> is_even(1)
            False

*code-output-style* examples
============================

*code-output-style* examples contain ordinary Python code. ::

    .. testcode::

        def is_even(x):
            return (x % 2) == 0

        print(is_even(0))
        print(is_even(1))

    .. testoutput::

        True
        False

They're rendered like this:

.. testcode::

    def is_even(x):
        return (x % 2) == 0

    print(is_even(0))
    print(is_even(1))

.. testoutput::

    True
    False

*literalinclude* examples
=========================

*literalinclude* examples display Python modules. ::

    .. literalinclude:: ./doc_code/example_module.py
        :language: python
        :start-after: __is_even_begin__
        :end-before: __is_even_end__

.. literalinclude:: ./doc_code/example_module.py
    :language: python

They're rendered like this:

.. literalinclude:: ./doc_code/example_module.py
    :language: python
    :start-after: __is_even_begin__
    :end-before: __is_even_end__

---------------------------------------
Which type of example should you write?
---------------------------------------

There's no hard rule about which style you should use. Choose the style that best
illustrates your API.

.. tip::
    If you're not sure which style to use, use *code-block-style*.

When to use *doctest-style*
===========================

If you're writing a small example that emphasizes object representations, or if you
want to print intermediate objects, use *doctest-style*. ::

    .. doctest::

        >>> import ray
        >>> ds = ray.data.range(100)
        >>> ds.schema()
        <class 'int'>
        >>> ds.take(5)
        [0, 1, 2, 3, 4]

When to use *code-block-style*
==============================

If you're writing a longer example, or if object representations aren't relevant to your example, use *code-block-style*. ::

    .. testcode::

        import pandas as pd
        import ray
        from ray.train.batch_predictor import BatchPredictor

        def calculate_accuracy(df):
            return pd.DataFrame({"correct": df["preds"] == df["label"]})

        # Create a batch predictor that returns identity as the predictions.
        batch_pred = BatchPredictor.from_pandas_udf(
        lambda data: pd.DataFrame({"preds": data["feature_1"]}))

        # Create a dummy dataset.
        ds = ray.data.from_pandas(pd.DataFrame({
        "feature_1": [1, 2, 3], "label": [1, 2, 3]}))

        # Execute batch prediction using this predictor.
        predictions = batch_pred.predict(ds,
        feature_columns=["feature_1"], keep_columns=["label"])

        # Calculate final accuracy
        correct = predictions.map_batches(calculate_accuracy)
        print(f"Final accuracy: {correct.sum(on='correct') / correct.count()}")

    .. testoutput::

        Final accuracy: 1.0

When to use *literalinclude*
============================
If you're writing an end-to-end examples and your examples doesn't contain outputs, use
*literalinclude*.

-----------------------------------
How to handle hard-to-test examples
-----------------------------------

When is it okay to not test an example?
=======================================

You don't need to test examples that require GPUs, or examples that depend on external
systems like Weights and Biases.

Skipping *doctest-style* examples
=================================

To skip a *doctest-style* example, append `# doctest: +SKIP` to your Python code. ::

    .. doctest::

        >>> import ray
        >>> ray.data.read_images("s3://private-bucket")  # doctest: +SKIP

Skipping *code-block-style* examples
====================================

To skip a *code-block-style* example, add `:skipif: True` to the `testoutput` block. ::

    .. testcode::
        :skipif: True

        from ray.air.integrations.wandb import WandbLoggerCallback
        callback = WandbLoggerCallback(
            project="Optimization_Project",
            api_key_file=...,
            log_config=True
        )

----------------------------------------------
How to handle long or non-determnistic outputs
----------------------------------------------

If your Python code is non-deterministic, or if your output is excessively long, you may want to skip all or part of an output.

Ignoring *doctest-style* outputs
================================

To ignore parts of a *doctest-style* output, append `# doctest: +ELLIPSIS` to your Python code and replace problematic sections with ellipsis. ::

    .. doctest::

        >>> import ray
        >>> ray.data.read_images("s3://anonymous@air-example-data-2/imagenet-sample-images")  # doctest: +ELLIPSIS
        Datastream(
            num_blocks=...,
            num_rows=...,
            schema={image: numpy.ndarray(shape=..., dtype=uint8)}
        )

To ignore an output altogether, write a *code-block-style* snippet. Don't use `# doctest: +SKIP`.

Ignoring *code-block-style* outputs
===================================

To ignore parts of a *code-block-style* output, add `:options: +ELLIPSIS` to the `testoutput` block and replace problematic sections with ellipsis. ::

    .. testcode::

        import ray
        ds = ray.data.read_images("s3://anonymous@air-example-data-2/imagenet-sample-images")
        print(ds)

    .. testoutput::
        :options: +ELLIPSIS

        Datastream(
            num_blocks=...,
            num_rows=...,
            schema={image: numpy.ndarray(shape=..., dtype=uint8)}
        )

To ignore an output altogether, replace the output with a single elipsis. ::

    .. testoutput::
        :hide:
        :options: +ELLIPSIS

        ...

--------------------
How to test examples
--------------------

Testing specific examples
=========================

To test specific examples, install `pytest-sphinx`.

.. code-block:: bash

    pip install pytest-sphinx

Then, run pytest on a module, docstring, or user guide.

.. code-block:: bash

    pytest --doctest-modules python/ray/data/read_api.py
    pytest --doctest-modules python/ray/data/read_api.py::ray.data.read_api.range
    pytest --doctest-modules doc/source/data/getting-started.rst

Testing all examples
====================

To test all code snippets, run

.. code-block:: bash

    RAY_MOCK_MODULES=0 make doctest

in the `ray/doc` directory.