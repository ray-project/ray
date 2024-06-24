.. _writing-code-snippets_ref:

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
        Column  Type
        ------  ----
        id      int64
        >>> ds.take(5)
        [{'id': 0}, {'id': 1}, {'id': 2}, {'id': 3}, {'id': 4}]

When to use *code-block-style*
==============================

If you're writing a longer example, or if object representations aren't relevant to your example, use *code-block-style*. ::

    .. testcode::

        from typing import Dict
        import numpy as np
        import ray

        ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

        # Compute a "petal area" attribute.
        def transform_batch(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
            vec_a = batch["petal length (cm)"]
            vec_b = batch["petal width (cm)"]
            batch["petal area (cm^2)"] = vec_a * vec_b
            return batch

        transformed_ds = ds.map_batches(transform_batch)
        print(transformed_ds.materialize())

    .. testoutput::

        MaterializedDataset(
           num_blocks=...,
           num_rows=150,
           schema={
              sepal length (cm): double,
              sepal width (cm): double,
              petal length (cm): double,
              petal width (cm): double,
              target: int64,
              petal area (cm^2): double
           }
        )

When to use *literalinclude*
============================
If you're writing an end-to-end examples and your examples doesn't contain outputs, use
*literalinclude*.

-----------------------------------
How to handle hard-to-test examples
-----------------------------------

When is it okay to not test an example?
=======================================

You don't need to test examples that depend on external systems like Weights and Biases.

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

To ignore parts of a *doctest-style* output, replace problematic sections with ellipses. ::

    >>> import ray
    >>> ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
    Dataset(
       num_rows=...,
       schema={image: numpy.ndarray(shape=(32, 32, 3), dtype=uint8)}
    )

To ignore an output altogether, write a *code-block-style* snippet. Don't use `# doctest: +SKIP`.

Ignoring *code-block-style* outputs
===================================

If parts of your output are long or non-deterministic, replace problematic sections
with ellipses. ::

    .. testcode::

        import ray
        ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
        print(ds)

    .. testoutput::

        Dataset(
           num_rows=...,
           schema={image: numpy.ndarray(shape=(32, 32, 3), dtype=uint8)}
        )

If your output is nondeterministic and you want to display a sample output, add
`:options: +MOCK`. ::

    .. testcode::

        import random
        print(random.random())

    .. testoutput::
        :options: +MOCK

        0.969461416250246

If your output is hard to test and you don't want to display a sample output, exclude
the ``testoutput``. ::

    .. testcode::

        print("This output is hidden and untested")


------------------------------
How to test examples with GPUs
------------------------------

To configure Bazel to run an example with GPUs, complete the following steps:

#. Open the corresponding ``BUILD`` file. If your example is in the ``doc/`` folder,
   open ``doc/BUILD``. If your example is in the ``python/`` folder, open a file like
   ``python/ray/train/BUILD``.

#. Locate the ``doctest`` rule. It looks like this: ::

    doctest(
        files = glob(
            include=["source/**/*.rst"],
        ),
        size = "large",
        tags = ["team:none"]
    )

#. Add the file that contains your example to the list of excluded files. ::

    doctest(
        files = glob(
            include=["source/**/*.rst"],
            exclude=["source/data/requires-gpus.rst"]
        ),
        tags = ["team:none"]
    )

#. If it doesn't already exist, create a ``doctest`` rule with ``gpu`` set to ``True``. ::

    doctest(
        files = [],
        tags = ["team:none"],
        gpu = True
    )

#. Add the file that contains your example to the GPU rule. ::

    doctest(
        files = ["source/data/requires-gpus.rst"]
        size = "large",
        tags = ["team:none"],
        gpu = True
    )

For a practical example, see ``doc/BUILD`` or ``python/ray/train/BUILD``.

----------------------------
How to locally test examples
----------------------------

To locally test examples, install the Ray fork of `pytest-sphinx`.

.. code-block:: bash

    pip install git+https://github.com/ray-project/pytest-sphinx

Then, run pytest on a module, docstring, or user guide.

.. code-block:: bash

    pytest --doctest-modules python/ray/data/read_api.py
    pytest --doctest-modules python/ray/data/read_api.py::ray.data.read_api.range
    pytest --doctest-modules doc/source/data/getting-started.rst
