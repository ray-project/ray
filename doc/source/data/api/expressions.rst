.. _expressions-api:

Expressions API
================

.. currentmodule:: ray.data.expressions

Expressions provide a way to specify column-based operations on datasets.
Use :func:`col` to reference columns and :func:`lit` to create literal values.
These can be combined with operators to create complex expressions for filtering,
transformations, and computations.

Public API
----------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    col
    lit
    udf
    download

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