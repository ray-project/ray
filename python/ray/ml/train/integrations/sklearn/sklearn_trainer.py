from collections import defaultdict
from typing import Any, Tuple, Type, Union, Callable, Dict, Iterable, Optional
from time import time
import os
import logging

import numpy as np
import pandas as pd
from joblib import parallel_backend

from ray import tune
import ray.cloudpickle as cpickle
from ray.ml.trainer import GenDataset
from ray.ml.config import ScalingConfig, RunConfig, ScalingConfigDataClass
from ray.ml.preprocessor import Preprocessor
from ray.tune import Trainable
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.util import PublicAPI
from ray.ml.trainer import Trainer
from ray.ml.constants import MODEL_KEY, PREPROCESSOR_KEY, TRAIN_DATASET_KEY
from ray.util.joblib import register_ray
from ray.ml.train.integrations.sklearn.score import score

from sklearn.base import BaseEstimator, clone
from sklearn.model_selection import BaseCrossValidator, cross_validate
from sklearn.metrics import check_scoring

# we are using a private API here, but it's consistent across versions
from sklearn.model_selection._validation import _check_multimetric_scoring

logger = logging.getLogger(__name__)

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
    the number of CPUs assigned to the Ray Actor.

    If you wish to use GPU-enabled estimators (eg. cuML), make sure to
    set ``"GPU": 1`` in ``scaling_config.trainer_resources``.

    The results are reported all at once and not in an iterative fashion.
    No checkpointing is done during training. This may be changed
    in the future.

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
            must be present in the training dataset. If None, no validation
            will be performed.
        params: Optional dict of params to be set on the estimator before
            fitting. Useful for hyperparameter tuning.
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

            If you provide a "cv_groups" column in the train dataset, it will be
            used as group labels for the samples used while splitting the dataset into
            train/test set. Only used in conjunction with a "Group" ``cv``
            instance (e.g., ``GroupKFold``). This corresponds to the ``groups``
            argument in ``sklearn.model_selection.cross_validation``.
        parallelize_cv: If set to True, will parallelize cross-validation
            instead of the estimator. If set to None, will detect if the estimator
            has any parallelism-related params (``n_jobs`` or ``thread_count``)
            and parallelize cross-validation if there are none.
            If False, will not parallelize cross-validation. Cannot be
            set to True if there are any GPUs assigned to the trainer.
            Ignored if ``cv`` is None.
        return_train_score_cv: Whether to also return train scores during
            cross-validation. Ignored if ``cv`` is None.
        scaling_config: Configuration for how to scale training.
            Only the ``trainer_resources`` key can be provided,
            as the training is not distributed.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.ml.preprocessor.Preprocessor to preprocess the
            provided datasets.
        **fit_params: Additional kwargs passed to ``estimator.fit()``
            method.
    """

    def __init__(
        self,
        *,
        estimator: BaseEstimator,
        datasets: Dict[str, GenDataset],
        label_column: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        scoring: Optional[ScoringType] = None,
        cv: Optional[CVType] = None,
        parallelize_cv: Optional[bool] = None,
        return_train_score_cv: bool = False,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        preprocessor: Optional[Preprocessor] = None,
        **fit_params,
    ):
        if fit_params.pop("resume_from_checkpoint", None):
            raise AttributeError(
                "SklearnTrainer does not support resuming from checkpoints. "
                "Remove the `resume_from_checkpoint` argument."
            )

        self.estimator = clone(estimator)
        self.label_column = label_column
        self.params = params or {}
        self.fit_params = fit_params
        self.scoring = scoring
        self.cv = cv
        self.parallelize_cv = parallelize_cv
        self.return_train_score_cv = return_train_score_cv
        super().__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=None,
        )

    def _validate_attributes(self):
        super()._validate_attributes()
        if TRAIN_DATASET_KEY not in self.datasets:
            raise KeyError(
                f"'{TRAIN_DATASET_KEY}' key must be preset in `datasets`. "
                f"Got {list(self.datasets.keys())}"
            )
        if "cv" in self.datasets:
            raise KeyError(
                "'cv' is a reserved key. Please choose a different key "
                "for the dataset."
            )
        if (
            not isinstance(self.parallelize_cv, bool)
            and self.parallelize_cv is not None
        ):
            raise ValueError(
                "`parallelize_cv` must be a bool or None, got "
                f"'{self.parallelize_cv}'"
            )
        scaling_config = ScalingConfigDataClass(**self.scaling_config)
        ScalingConfigDataClass.validate_config(
            config=scaling_config,
            allowed_keys=["trainer_resources"],
            scaling_config_arg_name="scaling_config",
            caller_name=self.__class__.__name__,
        )
        if (
            self.cv
            and self.parallelize_cv
            and scaling_config.trainer_resources.get("GPU", 0)
        ):
            raise ValueError(
                "`parallelize_cv` cannot be True if there are GPUs assigned to the "
                "trainer."
            )

    def _set_estimator_cpus(self, estimator: BaseEstimator, num_cpus: int) -> bool:
        """Returns True if there are any params related to CPUs."""
        cpu_params = {
            param: num_cpus
            for param in estimator.get_params(deep=True)
            if param.endswith("n_jobs") or param.endswith("thread_count")
        }
        estimator_has_parallelism_params = bool(cpu_params)
        estimator.set_params(**cpu_params)
        return estimator_has_parallelism_params

    def _get_datasets(self) -> Dict[str, Tuple[pd.DataFrame, pd.Series]]:
        pd_datasets = {}
        for key, ray_dataset in self.datasets.items():
            pd_dataset = ray_dataset.to_pandas(limit=float("inf"))
            if self.label_column:
                pd_datasets[key] = (
                    pd_dataset.drop(self.label_column, axis=1),
                    pd_dataset[self.label_column],
                )
            else:
                pd_datasets[key] = (pd_dataset, None)
        return pd_datasets

    def _score_on_validation_sets(
        self,
        estimator: BaseEstimator,
        datasets: Dict[str, Tuple[pd.DataFrame, pd.Series]],
    ) -> Dict[str, Dict[str, Any]]:
        results = defaultdict(dict)

        if not datasets:
            return results

        if callable(self.scoring):
            scorers = self.scoring
        elif self.scoring is None or isinstance(self.scoring, str):
            scorers = check_scoring(estimator, self.scoring)
        else:
            scorers = _check_multimetric_scoring(estimator, self.scoring)
        for key, X_y_tuple in datasets.items():
            X_test, y_test = X_y_tuple
            start_time = time()
            test_scores = score(estimator, X_test, y_test, scorers, np.nan)
            score_time = time() - start_time
            results[key]["score_time"] = score_time
            if not isinstance(test_scores, dict):
                test_scores = {"score": test_scores}

            for name in test_scores:
                results[key][f"test_{name}"] = test_scores[name]
        return results

    def _score_cv(
        self,
        estimator: BaseEstimator,
        X: pd.DataFrame,
        y: pd.Series,
        groups: pd.Series,
        n_jobs: int = 1,
    ) -> Dict[str, Any]:
        if not self.cv:
            return {}

        cv_results = cross_validate(
            estimator=estimator,
            X=X,
            y=y,
            groups=groups,
            scoring=self.scoring,
            cv=self.cv,
            n_jobs=n_jobs,
            fit_params=self.fit_params,
            return_train_score=self.return_train_score_cv,
        )
        cv_aggregates = {}
        for k, v in cv_results.items():
            if not isinstance(v, np.ndarray):
                continue
            try:
                cv_aggregates[f"{k}_mean"] = np.mean(v)
                cv_aggregates[f"{k}_std"] = np.std(v)
            except Exception:
                # if we can't average for some reason
                pass

        return {"cv": {**cv_results, **cv_aggregates}}

    def _set_estimator_cv_parallelism(self, num_cpus: int, has_gpus: bool) -> bool:
        parallelize_cv = False

        assert not (has_gpus and self.parallelize_cv)

        if self.cv and self.parallelize_cv is True:
            estimator_has_parallelism_params = self._set_estimator_cpus(
                self.estimator, 1
            )
            parallelize_cv = True
        else:
            estimator_has_parallelism_params = self._set_estimator_cpus(
                self.estimator, num_cpus
            )

        if (
            not has_gpus
            and self.cv
            and self.parallelize_cv is None
            and not estimator_has_parallelism_params
        ):
            logger.info(
                "No parallelism-related params detected in estimator, "
                "will parallelize cross-validation instead."
            )
            parallelize_cv = True
        return parallelize_cv

    def training_loop(self) -> None:
        register_ray()

        self.estimator.set_params(**self.params)

        datasets = self._get_datasets()
        X_train, y_train = datasets.pop(TRAIN_DATASET_KEY)
        groups = None
        if "cv_groups" in X_train.columns:
            groups = X_train["cv_groups"]
            X_train = X_train.drop("cv_groups", axis=1)

        trial_resources = tune.get_trial_resources().required_resources
        num_cpus = int(trial_resources.get("CPU", 0))
        has_gpus = bool(trial_resources.get("GPU", 0))

        # see https://scikit-learn.org/stable/computing/parallelism.html
        os.environ["OMP_NUM_THREADS"] = str(num_cpus)
        os.environ["MKL_NUM_THREADS"] = str(num_cpus)
        os.environ["OPENBLAS_NUM_THREADS"] = str(num_cpus)
        os.environ["BLIS_NUM_THREADS"] = str(num_cpus)

        parallelize_cv = self._set_estimator_cv_parallelism(num_cpus, has_gpus)

        with parallel_backend("ray", n_jobs=num_cpus):
            start_time = time()
            self.estimator.fit(X_train, y_train, **self.fit_params)
            fit_time = time() - start_time

            with tune.checkpoint_dir(step=1) as checkpoint_dir:
                with open(os.path.join(checkpoint_dir, MODEL_KEY), "wb") as f:
                    cpickle.dump(self.estimator, f)

            if self.label_column:
                validation_set_scores = self._score_on_validation_sets(
                    self.estimator, datasets
                )
                cv_scores = self._score_cv(
                    self.estimator,
                    X_train,
                    y_train,
                    groups,
                    # if estimator has parallelism, use that. Otherwise,
                    # parallelize CV
                    n_jobs=1 if not parallelize_cv else num_cpus,
                )
            else:
                validation_set_scores = {}
                cv_scores = {}

        results = {
            **validation_set_scores,
            **cv_scores,
            "fit_time": fit_time,
        }
        tune.report(**results)

    def as_trainable(self) -> Type[Trainable]:
        trainable_cls = super().as_trainable()
        scaling_config = self.scaling_config

        class SklearnTrainable(trainable_cls):
            @classmethod
            def default_resource_request(cls, config):
                updated_scaling_config = config.get("scaling_config", scaling_config)
                updated_scaling_config = ScalingConfigDataClass(
                    **updated_scaling_config
                )
                ScalingConfigDataClass.validate_config(
                    config=updated_scaling_config,
                    allowed_keys=["trainer_resources"],
                    scaling_config_arg_name="scaling_config",
                    caller_name=self.__class__.__name__,
                )

                head_resources = (
                    updated_scaling_config.trainer_resources
                    if updated_scaling_config.trainer_resources
                    else {"CPU": 1}
                )
                return PlacementGroupFactory([head_resources])

            def _postprocess_checkpoint(self, checkpoint_path: str):
                preprocessor = self._merged_config.get("preprocessor", None)
                if not checkpoint_path or preprocessor is None:
                    return
                with open(os.path.join(checkpoint_path, PREPROCESSOR_KEY), "wb") as f:
                    cpickle.dump(preprocessor, f)

        return SklearnTrainable
