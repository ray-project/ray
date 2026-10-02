---
myst:
  html_meta:
    description: "Pattern: use a supervisor actor to create and manage a tree of worker actors, centralizing lifecycle and failure handling."
---

# Pattern: Using a supervisor actor to manage a tree of actors

Actor supervision is a pattern in which a supervising actor manages a collection of worker actors. The supervisor delegates tasks to the worker actors and handles their failures. This pattern simplifies the driver, because the driver manages only a few supervisors and doesn't handle failures from worker actors directly. Multiple supervisors can also run in parallel to parallelize more work.

```{figure} ../images/tree-of-actors.svg
Tree of actors
```

:::{note}
- If the supervisor or the driver dies, Ray automatically terminates the worker actors because of actor reference counting.
- You can nest actors to multiple levels to form a tree.
:::

## Example use case

You want to do data parallel training and train the same model with different hyperparameters in parallel. For each hyperparameter, you can launch a supervisor actor to orchestrate the training. The supervisor creates worker actors that train on each data shard.

:::{note}
For data parallel training, use {py:class}`~ray.train.data_parallel_trainer.DataParallelTrainer` from {ref}`Ray Train <train-key-concepts>`. For hyperparameter tuning, use {ref}`Ray Tune's Tuner <tune-main>`. Both apply this pattern.
:::

## Code example

```{literalinclude} ../doc_code/pattern_tree_of_actors.py
:language: python
```
