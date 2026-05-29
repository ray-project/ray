.. _train-cross-validation:

Cross Validation
================

Cross-validation utilities for Ray Train provide convenient splitters and a
helper function to run cross-validation by instantiating a new Trainer
for each fold. This guide introduces the available APIs and shows examples for
common splitters.

Overview
--------

- ``ray.train.cross_validation.cross_validate`` runs cross-validation by
  instantiating a new Trainer for each fold.
- For each fold, ``cross_validate`` injects the fold datasets as ``"train"``
  and ``"val"`` entries in the Trainer ``datasets`` argument.
- Cross-validation folds are executed sequentially. Since each Trainer may
  already launch distributed workers, sequential execution avoids resource
  contention between folds.
- Available splitters include ``KFoldSplitter``,
  ``StratifiedKFoldSplitter``, ``GroupedKFoldSplitter``, and
  ``TimeSeriesSplitter``.

Quick example
-------------

Run a 3-fold K-fold cross-validation using a trainer class (this example uses
``TorchTrainer``). This example is illustrative; real training code is
required in ``train_loop_per_worker``.

.. code-block:: python

    import ray

    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer
    from ray.train.cross_validation import KFoldSplitter, cross_validate

    def train_loop_per_worker():
        # Training loop that reads from the provided "train" and "val"
        # datasets via the Trainer dataset API.
        ...

    dataset = ray.data.from_items([...])

    result = cross_validate(
        trainer_cls=TorchTrainer,
        dataset=dataset,
        splitter=KFoldSplitter(n_splits=3, seed=123),
        train_loop_per_worker=train_loop_per_worker,
        scaling_config=ScalingConfig(num_workers=2),
    )

    print(result.mean_metrics)
    print(result.std_metrics)

Time Series splitting
---------------------

Use ``TimeSeriesSplitter`` when sample ordering matters. If ``time_column``
is provided the dataset is sorted by that column before splitting. 
Otherwise the dataset is assumed to already be in temporal order.

.. code-block:: python

    from ray.train.cross_validation import TimeSeriesSplitter

    splitter = TimeSeriesSplitter(
        n_splits=3,
        time_column="timestamp",
        gap=1,
    )

    folds = splitter.split(dataset)

Stratified K-Fold
-----------------

Preserve class proportions across folds with
``StratifiedKFoldSplitter``. Supply the target column with
``stratify_column``.

.. code-block:: python

    from ray.train.cross_validation import StratifiedKFoldSplitter

    splitter = StratifiedKFoldSplitter(
        n_splits=5,
        stratify_column="label",
    )

    folds = splitter.split(dataset)

Grouped K-Fold
--------------

Grouped splitting keeps all rows from the same group in the same fold.

.. code-block:: python

    from ray.train.cross_validation import GroupedKFoldSplitter

    splitter = GroupedKFoldSplitter(
        n_splits=4,
        group_columns=["user_id"],
    )

    folds = splitter.split(dataset)

Notes
-----

- Splitters yield ``(train, val)`` dataset pairs.
- Fold sizes and ordering depend on the splitter configuration.
- Each fold launches an independent Trainer run.
- Dataset materialization:
    - ``StratifiedKFoldSplitter`` materializes the dataset at an intermediate step to avoid recomputing ``groupby + map_groups`` for every train/val split per fold.
    - ``TimeSeriesSplitter`` materializes after sorting by ``time_column`` when one is provided, to establish temporal order. Without this, re-executing the sort on a dataset with identical timestamps could produce a different ordering.

