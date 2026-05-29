import statistics
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Type

from ray.data import Dataset
from ray.train import Result
from ray.train.cross_validation.splitter import Splitter
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.util.annotations import PublicAPI


@PublicAPI
@dataclass
class CVResult:
    """Results from a cross-validation run.

    Attributes:
        fold_results: A list of per-fold results returned by each trainer's fit().
        mean_metrics: A dictionary mapping metric names to their mean value across folds.
        std_metrics: A dictionary mapping metric names to their standard deviation across folds.
    """

    fold_results: List[Result]
    mean_metrics: Dict[str, float]
    std_metrics: Dict[str, float]


@PublicAPI
def cross_validate(
    trainer_cls: Type[DataParallelTrainer],
    dataset: Dataset,
    splitter: Splitter,
    **trainer_kwargs: Any,
) -> CVResult:
    """Run k-fold cross-validation using the given trainer class.

    For each fold produced by ``splitter``, a new trainer instance is created
    with the fold's train and validation splits injected as the ``"train"`` and
    ``"val"`` dataset keys respectively. All folds are trained sequentially and
    their results are aggregated into a :class:`CVResult`.

    Example:

    .. testcode::
        :skipif: True

        import ray
        from ray.train import ScalingConfig
        from ray.train.torch import TorchTrainer
        from ray.train.cross_validation import CVResult, KFoldSplitter, cross_validate

        def train_func():
            ...

        dataset = ray.data.from_items(range(100))
        result: CVResult = cross_validate(
            trainer_cls=TorchTrainer,
            dataset=dataset,
            splitter=KFoldSplitter(n_splits=5),
            train_loop_per_worker=train_func,
            scaling_config=ScalingConfig(num_workers=2),
        )
        print(result.mean_metrics)

    Args:
        trainer_cls: The trainer class to instantiate for each fold.
        dataset: The full dataset to be split into train and validation folds by
            ``splitter``.
        splitter: A :class:`~ray.train.cross_validation.Splitter` that defines how
            ``dataset`` is partitioned into folds.
        **trainer_kwargs: Additional keyword arguments forwarded to ``trainer_cls``
            on each fold. These are the same arguments you would pass to
            ``trainer_cls`` directly, such as ``train_loop_per_worker``,
            ``scaling_config``, and ``run_config``. Do not include ``datasets``
            here; it is managed internally by ``cross_validate``.

    Returns:
        A :class:`CVResult` containing per-fold results and aggregated metrics.

    Raises:
        ValueError: If ``datasets`` is passed in ``trainer_kwargs``, or if
            ``splitter`` produces no folds.
    """
    if "datasets" in trainer_kwargs:
        raise ValueError(
            "Do not pass `datasets` to cross_validate. "
            "Pass the full dataset via the `dataset` argument instead."
        )

    fold_results = []

    for train_ds, val_ds in splitter.split(dataset):
        trainer = trainer_cls(
            datasets={"train": train_ds, "val": val_ds},
            **trainer_kwargs,
        )
        result = trainer.fit()
        fold_results.append(result)

    if not fold_results:
        raise ValueError("Splitter produced no folds.")

    mean_metrics, std_metrics = _aggregate_metrics(fold_results)
    return CVResult(
        fold_results=fold_results,
        mean_metrics=mean_metrics,
        std_metrics=std_metrics,
    )


def _aggregate_metrics(
    fold_results: List[Result],
) -> Tuple[Dict[str, float], Dict[str, float]]:
    all_metrics: Dict[str, List[float]] = {}

    for result in fold_results:
        if result.metrics is None:
            continue
        for key, value in result.metrics.items():
            if not isinstance(value, (int, float)):
                continue
            all_metrics.setdefault(key, []).append(float(value))

    mean_metrics = {k: statistics.mean(v) for k, v in all_metrics.items()}
    std_metrics = {
        k: statistics.stdev(v) if len(v) > 1 else 0.0 for k, v in all_metrics.items()
    }

    return mean_metrics, std_metrics
