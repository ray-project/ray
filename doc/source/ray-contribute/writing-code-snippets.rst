.. _writing-code-snippets:

==========================
How to write code snippets
==========================

Docstrings and user guides should contain examples that illustrate Ray's APIs.

This page describes how to write code snippets so that they're tested in CI.

.. note::
    The examples in this guide use reStructuredText. If you're writing
    Markdown, use MyST syntax. To learn more, read the
    `MyST documentation <https://myst-parser.readthedocs.io/en/latest/syntax/roles-and-directives.html#directives-a-block-level-extension-point>`_.

-----------------
Types of examples
-----------------

There are two types of examples: *doctest-style* and *code-output-style*.

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

---------------------------------------
Which type of example should you write?
---------------------------------------

If you're writing a small example that emphasizes object representations, use *doctest-style*. ::

    .. doctest::

        >>> import ray
        >>> dataset = ray.data.read_csv("s3://air-example-data/iris.csv")
        >>> dataset.input_files()
        ['air-example-data/iris.csv']

If you're writing a longer example, or if object represenations aren't relevant to your example, use *code-block-style*. ::

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

There's no hard rule about which style you should use. In general, use *code-block-style* if you're writing end-to-end workflows. Otherwise, use your best judgement as to which style illustrates the API better.

----------------------------------------------
How to handle long or non-determnistic outputs
----------------------------------------------

If your Python code is non-deterministic, or if your output is excessively long, you may want to skip all or part of an output.

Ignoring *doctest-style* outputs
================================

To ignore parts of a *doctest-style* output, append `# doctest: +ELLIPSIS` to  your Python code and replace problematic sections with ellipsis. ::

    .. doctest::

        >>> import ray
        >>> ray.data.read_images("s3://air-example-data-2/imagenet-sample-images")  # doctest: +ELLIPSIS
        Dataset(num_blocks=..., num_rows=..., schema={image: ArrowTensorType(shape=..., dtype=uint8)})

To ignore an output altogether, write a *code-block-style* snippet. Don't use `# DOCTEST: +SKIP`.

Ignoring *code-block-style* outputs
===================================

To ignore parts of a *code-block-style* output, add `:options: +ELLIPSIS` to the `testoutput` block and replace problematic sections with ellipsis. ::

    .. testcode::

        import ray
        ds = ray.data.read_images("s3://air-example-data-2/imagenet-sample-images")
        print(ds)

    .. testoutput::
        :options: +ELLIPSIS

        Dataset(num_blocks=..., num_rows=..., schema={image: ArrowTensorType(shape=..., dtype=uint8)})

To ignore an output altogether, replace the output with a single elipsis. ::

    .. testoutput::
        :hide:
        :options: +ELLIPSIS

        ...

--------------------
How to test examples
--------------------

To test code snippets, run::

    RAY_MOCK_MODULES=0 make doctest

in the `ray/doc` directory.