from typing import Union, Callable, Dict, Iterable, Optional

import numpy as np
import pandas as pd
from ray.ml.trainer import GenDataset
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.preprocessor import Preprocessor
from ray.util import PublicAPI
from ray.ml.trainer import Trainer
from ray.ml.checkpoint import Checkpoint

from sklearn.base import BaseEstimator
from sklearn.model_selection import BaseCrossValidator

# some nice properties of Sklearn models:
# - they are always pickleable
# - no logic is present in __init__

ArrayType = Union[pd.DataFrame, np.ndarray]
MetricType = Union[str, Callable[[BaseEstimator, ArrayType, ArrayType], float]]
ScoringType = Union[MetricType, Iterable[MetricType], Dict[str, MetricType]]
CVType = Union[int, Iterable, BaseCrossValidator]


@PublicAPI(stability="alpha")
class SklearnTrainer(Trainer):
    """A Trainer for scikit-learn estimator training.

    This Trainer runs the ``fit`` method of the given estimator in a
    non-distributed manner on a single Ray Actor.

    The ``n_jobs`` (or ``thread_count``) parameters will be set to match
    the number of CPUs assigned to the Ray Actor. Please note that
    parallelism for joblib-based estimators is not yet implemented
    (though technically possible).

    Example:
        .. code-block:: python

            import ray

            from ray.ml.train.integrations.sklearn import SklearnTrainer
            from sklearn.ensemble import RandomForestRegressor

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = SklearnTrainer(
                sklearn_estimator=RandomForestRegressor,
                label_column="y",
                scaling_config={
                    "trainer_resources": {"CPU": 4}
                },
                datasets={"train": train_dataset}
            )
            result = trainer.fit()

    Args:
        estimator: A scikit-learn compatible estimator to use.
        datasets: Ray Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset. If a ``preprocessor``
            is provided and has not already been fit, it will be fit on the training
            dataset. All datasets will be transformed by the ``preprocessor`` if
            one is provided. All non-training datasets will be used as separate
            validation sets, each reporting separate metrics.
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        scoring: Strategy to evaluate the performance of the model on
            the validation sets and for cross-validation. Same as in
            ``sklearn.model_selection.cross_validation``.
            If ``scoring`` represents a single score, one can use:

            - a single string;
            - a callable that returns a single value.

            If ``scoring`` represents multiple scores, one can use:

            - a list or tuple of unique strings;
            - a callable returning a dictionary where the keys are the metric
              names and the values are the metric scores;
            - a dictionary with metric names as keys and callables a values.

        cv: Determines the cross-validation splitting strategy. If specified,
            cross-validation will be run on the train dataset, in addition to
            computing metrics for validation datasets. Same as in
            ``sklearn.model_selection.cross_validation``, with the exception of
            None.
            Possible inputs for ``cv`` are:

            - None, to skip cross-validation.
            - int, to specify the number of folds in a ``(Stratified)KFold``,
            - ``CV splitter``,
            - An iterable yielding (train, test) splits as arrays of indices.

            For int/None inputs, if the estimator is a classifier and ``y`` is
            either binary or multiclass, ``StratifiedKFold`` is used. In all
            other cases, ``KFold`` is used. These splitters are instantiated
            with ``shuffle=False`` so the splits will be the same across calls.
        scaling_config: Configuration for how to scale training.
            Only the ``trainer_resources`` key can be provided,
            as the training is not distributed.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
        **fit_params: Additional kwargs passed to ``estimator.fit()``
            method.
    """

    def __init__(
        self,
        *,
        estimator: BaseEstimator,
        datasets: Dict[str, GenDataset],
        label_column: Optional[str] = None,
        scoring: Optional[ScoringType] = None,
        cv: Optional[CVType] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        **fit_params
    ):
        self.estimator = estimator
        self.label_column = label_column
        self.fit_params = fit_params
        self.scoring = scoring
        self.cv = cv
        super().__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def training_loop(self) -> None:
        return super().training_loop()
