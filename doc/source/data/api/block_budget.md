(block_budget_api_ref)=

# Block Budget API

Pass a {class}`BlockBudget <ray.data.block_budget.BlockBudget>` to
{meth}`Dataset.repartition() <ray.data.Dataset.repartition>` to shape output blocks
during a streaming, order-preserving repartition (no shuffle):
{class}`RowCount <ray.data.block_budget.RowCount>` caps each block at a number of
rows, and {class}`ByteSize <ray.data.block_budget.ByteSize>` caps each block at a
target size in bytes.

```{eval-rst}
.. currentmodule:: ray.data.block_budget

.. autosummary::
    :nosignatures:
    :toctree: doc/

    BlockBudget
    RowCount
    ByteSize
```
