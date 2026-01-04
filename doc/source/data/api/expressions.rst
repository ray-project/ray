.. _expressions-api:

Expressions API
================

.. currentmodule:: ray.data.expressions

Expressions provide a way to specify column-based operations on datasets.
Use :func:`col` to reference columns and :func:`lit` to create literal values.
You can combine these with operators to create complex expressions for filtering,
transformations, and computations.

Public API
----------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    star
    col
    lit
    udf
    pyarrow_udf
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
    UnaryExpr
    UDFExpr
    StarExpr

Expression namespaces
------------------------------------

These namespace classes provide specialized operations for list, string, and struct columns.
You access them through properties on expressions: ``.list``, ``.str``, and ``.struct``.

The following example shows how to use the string namespace to transform text columns:

.. testcode::

    import ray
    from ray.data.expressions import col

    # Create a dataset with a text column
    ds = ray.data.from_items([
        {"name": "alice"},
        {"name": "bob"},
        {"name": "charlie"}
    ])

    # Use the string namespace to uppercase the names
    ds = ds.with_column("upper_name", col("name").str.upper())
    ds.show()

.. testoutput::

    {'name': 'alice', 'upper_name': 'ALICE'}
    {'name': 'bob', 'upper_name': 'BOB'}
    {'name': 'charlie', 'upper_name': 'CHARLIE'}

The following example demonstrates using the list namespace to work with array columns:

.. testcode::

    import ray
    from ray.data.expressions import col

    # Create a dataset with list columns
    ds = ray.data.from_items([
        {"scores": [85, 90, 78]},
        {"scores": [92, 88]},
        {"scores": [76, 82, 88, 91]}
    ])

    # Use the list namespace to get the length of each list
    ds = ds.with_column("num_scores", col("scores").list.len())
    ds.show()

.. testoutput::

    {'scores': [85, 90, 78], 'num_scores': 3}
    {'scores': [92, 88], 'num_scores': 2}
    {'scores': [76, 82, 88, 91], 'num_scores': 4}

The following example shows how to use the struct namespace to access nested fields:

.. testcode::

    import ray
    from ray.data.expressions import col

    # Create a dataset with struct columns
    ds = ray.data.from_items([
        {"user": {"name": "alice", "age": 25}},
        {"user": {"name": "bob", "age": 30}},
        {"user": {"name": "charlie", "age": 35}}
    ])

    # Use the struct namespace to extract a specific field
    ds = ds.with_column("user_name", col("user").struct.field("name"))
    ds.show()

.. testoutput::

    {'user': {'name': 'alice', 'age': 25}, 'user_name': 'alice'}
    {'user': {'name': 'bob', 'age': 30}, 'user_name': 'bob'}
    {'user': {'name': 'charlie', 'age': 35}, 'user_name': 'charlie'}

.. autoclass:: _ListNamespace
    :members:
    :exclude-members: _expr

.. autoclass:: _StringNamespace
    :members:
    :exclude-members: _expr

.. autoclass:: _StructNamespace
    :members:
    :exclude-members: _expr