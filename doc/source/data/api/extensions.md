---
myst:
  html_meta:
    description: "Public utilities for working with Ray Data extension types in PyArrow tables."
---

(data-extensions-api)=

# Extension utilities

Use {py:func}`ray.data.extensions.take_table` to select rows from PyArrow tables containing Ray tensor or Python object extension columns. You can also use it in {py:meth}`Dataset.map_batches <ray.data.Dataset.map_batches>` with `batch_format="pyarrow"`.

```{eval-rst}
.. currentmodule:: ray.data.extensions

.. autosummary::
   :nosignatures:
   :toctree: doc/

   take_table
```
