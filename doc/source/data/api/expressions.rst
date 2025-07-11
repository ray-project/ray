.. _expressions-api:

Expressions API
===============

Expressions provide a way to specify column-based operations on datasets.
Use :func:`col` to reference columns and :func:`lit` to create literal values.
These can be combined with operators to create complex expressions for filtering,
transformations, and computations.

Examples:

.. code-block:: python

    import ray
    from ray.data.expressions import col, lit

    # Create a dataset
    ds = ray.data.from_items([
        {"name": "Alice", "age": 30, "score": 85.5},
        {"name": "Bob", "age": 25, "score": 92.0},
        {"name": "Charlie", "age": 35, "score": 78.5}
    ])

    # Use expressions in transformations
    ds = ds.with_columns({
        "age_plus_one": col("age") + lit(1),
        "high_score": col("score") > lit(80)
    })

    # Use expressions in filtering
    ds = ds.filter(col("age") >= lit(30))

.. currentmodule:: ray.data.expressions

Public API
----------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    col
    lit

Expression Classes
------------------

These classes represent the structure of expressions. You typically don't need to 
instantiate them directly, but you may encounter them when working with expressions.

.. autosummary::
    :nosignatures:
    :toctree: doc/

    Expr
    ColumnExpr
    LiteralExpr
    BinaryExpr
    Operation