import logging
import os
import warnings
from collections import defaultdict
from time import time
from traceback import format_exc
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Optional, Union, Tuple

import numpy as np
import pandas as pd
from joblib import parallel_backend
from sklearn.base import BaseEstimator, clone
from sklearn.metrics import check_scoring
from sklearn.model_selection import BaseCrossValidator, cross_validate

# we are using a private API here, but it's consistent across versions
from sklearn.model_selection._validation import _check_multimetric_scoring, _score

import ray.cloudpickle as cpickle
from ray import tune
from ray.air._internal.checkpointing import (
    save_preprocessor_to_dir,
)
from ray.air.config import RunConfig, ScalingConfig
from ray.train.constants import MODEL_KEY, TRAIN_DATASET_KEY
from ray.train.sklearn._sklearn_utils import _has_cpu_params, _set_cpu_params
from ray.train.trainer import BaseTrainer, GenDataset
from ray.util import PublicAPI
from ray.util.joblib import register_ray

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

logger = logging.getLogger(__name__)

# some nice properties of Sklearn models:
# - they are always pickleable
# - no logic is present in __init__

ArrayType = Union[pd.DataFrame, np.ndarray]
MetricType = Union[str, Callable[[BaseEstimator, ArrayType, ArrayType], float]]
ScoringType = Union[MetricType, Iterable[MetricType], Dict[str, MetricType]]
CVType = Union[int, Iterable, BaseCrossValidator]


@PublicAPI(stability="alpha")
class SklearnTrainer(BaseTrainer):
    """A Trainer for scikit-learn estimator training.

    This Trainer runs the ``fit`` method of the given estimator in a
    non-distributed manner on a single Ray Actor.

    By default, the ``n_jobs`` (or ``thread_count``) estimator parameters will be set
    to match the number of CPUs assigned to the Ray Actor. This behavior can be
    disabled by setting ``set_estimator_cpus=False``.

    If you wish to use GPU-enabled estimators (eg. cuML), make sure to
    set ``"GPU": 1`` in ``scaling_config.trainer_resources``.

    The results are reported all at once and not in an iterative fashion.
    No checkpointing is done during training. This may be changed
    in the future.

    Example:
        .. code-block:: python

            import ray

            from ray.train.sklearn import SklearnTrainer
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
        return_train_score_cv: Whether to also return train scores during
            cross-validation. Ignored if ``cv`` is None.
        parallelize_cv: If set to True, will parallelize cross-validation
            instead of the estimator. If set to None, will detect if the estimator
            has any parallelism-related params (``n_jobs`` or ``thread_count``)
            and parallelize cross-validation if there are none.
            If False, will not parallelize cross-validation. Cannot be
            set to True if there are any GPUs assigned to the trainer.
            Ignored if ``cv`` is None.
        set_estimator_cpus: If set to True, will automatically set
            the values of all ``n_jobs`` and ``thread_count`` parameters
            in the estimator (including in nested objects) to match
            the number of available CPUs.
        scaling_config: Configuration for how to scale training.
            Only the ``trainer_resources`` key can be provided,
            as the training is not distributed.
        run_config: Configuration for the execution of the training run.
        preprocessor: A ray.data.Preprocessor to preprocess the
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
        return_train_score_cv: bool = False,
        parallelize_cv: Optional[bool] = None,
        set_estimator_cpus: bool = True,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        preprocessor: Optional["Preprocessor"] = None,
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
        self.set_estimator_cpus = set_estimator_cpus
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

        if self.label_column is not None and not isinstance(self.label_column, str):
            raise ValueError(
                f"`label_column` must be a string or None, got '{self.label_column}'"
            )

        if self.params is not None and not isinstance(self.params, dict):
            raise ValueError(f"`params` must be a dict or None, got '{self.params}'")

        # Don't validate self.scoring for now as many types are supported
        # Don't validate self.cv for now as many types are supported

        if not isinstance(self.return_train_score_cv, bool):
            raise ValueError(
                f"`return_train_score_cv` must be a boolean, got "
                f"'{self.return_train_score_cv}'"
            )

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
        scaling_config = self._validate_and_get_scaling_config_data_class(
            self.scaling_config
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
            try:
                test_scores = _score(estimator, X_test, y_test, scorers)
            except Exception:
                if isinstance(scorers, dict):
                    test_scores = {k: np.nan for k in scorers}
                else:
                    test_scores = np.nan
                warnings.warn(
                    f"Scoring on validation set {key} failed. The score(s) for "
                    f"this set will be set to nan. Details: \n"
                    f"{format_exc()}",
                    UserWarning,
                )
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
                cv_aggregates[f"{k}_mean"] = np.nanmean(v)
                cv_aggregates[f"{k}_std"] = np.nanstd(v)
            except Exception as e:
                logger.warning(
                    f"Couldn't calculate aggregate metrics for CV folds! {e}"
                )
                cv_aggregates[f"{k}_mean"] = np.nan
                cv_aggregates[f"{k}_std"] = np.nan
                pass

        return {"cv": {**cv_results, **cv_aggregates}}

    def _get_cv_parallelism(self, has_gpus: bool) -> bool:
        parallelize_cv = False

        assert not (has_gpus and self.parallelize_cv)

        estimator_has_parallelism_params = _has_cpu_params(self.estimator)

        if self.cv and self.parallelize_cv is True:
            parallelize_cv = True

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

        scaling_config_dataclass = self._validate_and_get_scaling_config_data_class(
            self.scaling_config
        )

        num_workers = scaling_config_dataclass.num_workers or 0
        assert num_workers == 0  # num_workers is not in scaling config allowed_keys

        trainer_resources = scaling_config_dataclass.trainer_resources or {"CPU": 1}
        has_gpus = bool(trainer_resources.get("GPU", 0))
        num_cpus = int(trainer_resources.get("CPU", 1))

        # see https://scikit-learn.org/stable/computing/parallelism.html
        os.environ["OMP_NUM_THREADS"] = str(num_cpus)
        os.environ["MKL_NUM_THREADS"] = str(num_cpus)
        os.environ["OPENBLAS_NUM_THREADS"] = str(num_cpus)
        os.environ["BLIS_NUM_THREADS"] = str(num_cpus)

        parallelize_cv = self._get_cv_parallelism(has_gpus)
        if self.set_estimator_cpus:
            num_estimator_cpus = 1 if parallelize_cv else num_cpus
            _set_cpu_params(self.estimator, num_estimator_cpus)

        with parallel_backend("ray", n_jobs=num_cpus):
            start_time = time()
            self.estimator.fit(X_train, y_train, **self.fit_params)
            fit_time = time() - start_time

            with tune.checkpoint_dir(step=1) as checkpoint_dir:
                with open(os.path.join(checkpoint_dir, MODEL_KEY), "wb") as f:
                    cpickle.dump(self.estimator, f)

                if self.preprocessor:
                    save_preprocessor_to_dir(self.preprocessor, checkpoint_dir)

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

        # cv_scores will not override validation_set_scores as we
        # check for that during initialization
        results = {
            **validation_set_scores,
            **cv_scores,
            "fit_time": fit_time,
        }
        tune.report(**results)
