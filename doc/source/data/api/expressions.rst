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
    monotonically_increasing_id

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

These namespace classes provide specialized operations for list, string, struct, array, and
`datetime` columns. You access them through properties on expressions: ``.list``, ``.str``,
``.struct``, ``.arr``, and ``.dt``.

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

You can also perform list-specific transformations like sorting and flattening:

.. testcode::

    import ray
    from ray.data.expressions import col

    ds = ray.data.from_items([
        {"values": [3, 1, 2], "nested": [[1, 2], [3]]},
        {"values": [2, None, 5], "nested": [[4], []]}
    ])

    ds = ds.with_column(
        "sorted_values", col("values").list.sort(order="descending")
    )
    ds = ds.with_column(
        "flattened_nested", col("nested").list.flatten()
    )
    ds.show()

.. testoutput::

    {'values': [3, 1, 2], 'nested': [[1, 2], [3]], 'sorted_values': [3, 2, 1], 'flattened_nested': [1, 2, 3]}
    {'values': [2, None, 5], 'nested': [[4], []], 'sorted_values': [5, 2, None], 'flattened_nested': [4]}

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

The following example shows how to use the array namespace to convert fixed-size
list columns to variable-length lists:

.. testcode::

    import pyarrow as pa
    import ray
    from ray.data.expressions import col

    values = pa.array([1, 2, 3, 4])
    fixed = pa.FixedSizeListArray.from_arrays(values, 2)
    table = pa.table({"features": fixed})

    ds = ray.data.from_arrow(table)
    ds = ds.with_column("features_list", col("features").arr.to_list())
    ds.show()

.. testoutput::

    {'features': [1, 2], 'features_list': [1, 2]}
    {'features': [3, 4], 'features_list': [3, 4]}

The following example shows how to use the `datetime` namespace to extract components:

.. testcode::

    import datetime
    import pandas as pd
    import ray
    from ray.data.expressions import col

    ds = ray.data.from_items([
        {"ts": pd.Timestamp("2024-01-02 03:04:05")},
        {"ts": pd.Timestamp("2024-02-03 04:05:06")}
    ])

    ds = ds.with_column("year", col("ts").dt.year())
    ds.show()

.. testoutput::

    {'ts': datetime.datetime(2024, 1, 2, 3, 4, 5), 'year': 2024}
    {'ts': datetime.datetime(2024, 2, 3, 4, 5, 6), 'year': 2024}

.. autoclass:: _ListNamespace
    :members:
    :exclude-members: _expr

.. autoclass:: _StringNamespace
    :members:
    :exclude-members: _expr

.. autoclass:: _StructNamespace
    :members:
    :exclude-members: _expr

.. autoclass:: _ArrayNamespace
    :members:
    :exclude-members: _expr

.. autoclass:: _DatetimeNamespace
    :members:
    :exclude-members: _expr
