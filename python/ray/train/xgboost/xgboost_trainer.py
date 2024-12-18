import logging
from functools import partial
from typing import Any, Dict, Optional

import xgboost
from packaging.version import Version

import ray.train
from ray.train import Checkpoint
from ray.train.constants import _DEPRECATED_VALUE, TRAIN_DATASET_KEY
from ray.train.trainer import GenDataset
from ray.train.xgboost import RayTrainReportCallback
from ray.train.xgboost.v2 import XGBoostTrainer as SimpleXGBoostTrainer
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


def _xgboost_train_fn_per_worker(
    config: dict,
    label_column: str,
    num_boost_round: int,
    dataset_keys: set,
    xgboost_train_kwargs: dict,
):
    checkpoint = ray.train.get_checkpoint()
    starting_model = None
    remaining_iters = num_boost_round
    if checkpoint:
        starting_model = RayTrainReportCallback.get_model(checkpoint)
        starting_iter = starting_model.num_boosted_rounds()
        remaining_iters = num_boost_round - starting_iter
        logger.info(
            f"Model loaded from checkpoint will train for "
            f"additional {remaining_iters} iterations (trees) in order "
            "to achieve the target number of iterations "
            f"({num_boost_round=})."
        )

    train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)
    train_df = train_ds_iter.materialize().to_pandas()

    eval_ds_iters = {
        k: ray.train.get_dataset_shard(k)
        for k in dataset_keys
        if k != TRAIN_DATASET_KEY
    }
    eval_dfs = {k: d.materialize().to_pandas() for k, d in eval_ds_iters.items()}

    train_X, train_y = train_df.drop(label_column, axis=1), train_df[label_column]
    dtrain = xgboost.DMatrix(train_X, label=train_y)

    # NOTE: Include the training dataset in the evaluation datasets.
    # This allows `train-*` metrics to be calculated and reported.
    evals = [(dtrain, TRAIN_DATASET_KEY)]

    for eval_name, eval_df in eval_dfs.items():
        eval_X, eval_y = eval_df.drop(label_column, axis=1), eval_df[label_column]
        evals.append((xgboost.DMatrix(eval_X, label=eval_y), eval_name))

    evals_result = {}
    xgboost.train(
        config,
        dtrain=dtrain,
        evals=evals,
        evals_result=evals_result,
        num_boost_round=remaining_iters,
        xgb_model=starting_model,
        **xgboost_train_kwargs,
    )


@PublicAPI(stability="beta")
class XGBoostTrainer(SimpleXGBoostTrainer):
    """A Trainer for data parallel XGBoost training.

    This Trainer runs the XGBoost training loop in a distributed manner
    using multiple Ray Actors.

    .. note::
        ``XGBoostTrainer`` does not modify or otherwise alter the working
        of the XGBoost distributed training algorithm.
        Ray only provides orchestration, data ingest and fault tolerance.
        For more information on XGBoost distributed training, refer to
        `XGBoost documentation <https://xgboost.readthedocs.io>`__.

    Example:
        .. testcode::

            import ray

            from ray.train.xgboost import XGBoostTrainer
            from ray.train import ScalingConfig

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)])
            trainer = XGBoostTrainer(
                label_column="y",
                params={"objective": "reg:squarederror"},
                scaling_config=ScalingConfig(num_workers=3),
                datasets={"train": train_dataset},
            )
            result = trainer.fit()

        .. testoutput::
            :hide:

            ...

    Args:
        datasets: The Ray Datasets to use for training and validation. Must include a
            "train" key denoting the training dataset. All non-training datasets will
            be used as separate validation sets, each reporting a separate metric.
        label_column: Name of the label column. A column with this name
            must be present in the training dataset.
        params: XGBoost training parameters.
            Refer to `XGBoost documentation <https://xgboost.readthedocs.io/>`_
            for a list of possible parameters.
        num_boost_round: Target number of boosting iterations (trees in the model).
            Note that unlike in ``xgboost.train``, this is the target number
            of trees, meaning that if you set ``num_boost_round=10`` and pass a model
            that has already been trained for 5 iterations, it will be trained for 5
            iterations more, instead of 10 more.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        dataset_config: The configuration for ingesting the input ``datasets``.
            By default, all the Ray Datasets are split equally across workers.
            See :class:`~ray.train.DataConfig` for more details.
        resume_from_checkpoint: A checkpoint to resume training from.
        metadata: Dict that should be made available in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
        **train_kwargs: Additional kwargs passed to ``xgboost.train()`` function.
    """

    _handles_checkpoint_freq = True
    _handles_checkpoint_at_end = True

    def __init__(
        self,
        *,
        datasets: Dict[str, GenDataset],
        label_column: str,
        params: Dict[str, Any],
        dmatrix_params: Optional[Dict[str, Dict[str, Any]]] = _DEPRECATED_VALUE,
        num_boost_round: int = 10,
        scaling_config: Optional[ray.train.ScalingConfig] = None,
        run_config: Optional[ray.train.RunConfig] = None,
        dataset_config: Optional[ray.train.DataConfig] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **train_kwargs,
    ):
        if Version(xgboost.__version__) < Version("1.7.0"):
            raise ImportError(
                "`XGBoostTrainer` requires the `xgboost` version to be >= 1.7.0. "
                'Upgrade with: `pip install -U "xgboost>=1.7"`'
            )

        # TODO(justinvyu): [Deprecated] Remove in 2.11
        if dmatrix_params != _DEPRECATED_VALUE:
            raise DeprecationWarning(
                "`dmatrix_params` is deprecated, since XGBoostTrainer no longer "
                "depends on the `xgboost_ray.RayDMatrix` utility. "
                "You can remove this argument and use `dataset_config` instead "
                "to customize Ray Dataset ingestion."
            )

        # Initialize a default Ray Train metrics/checkpoint reporting callback if needed
        callbacks = train_kwargs.get("callbacks", [])
        user_supplied_callback = any(
            isinstance(callback, RayTrainReportCallback) for callback in callbacks
        )
        callback_kwargs = {}
        if run_config:
            checkpoint_frequency = run_config.checkpoint_config.checkpoint_frequency
            checkpoint_at_end = run_config.checkpoint_config.checkpoint_at_end

            callback_kwargs["frequency"] = checkpoint_frequency
            # Default `checkpoint_at_end=True` unless the user explicitly sets it.
            callback_kwargs["checkpoint_at_end"] = (
                checkpoint_at_end if checkpoint_at_end is not None else True
            )

        if not user_supplied_callback:
            callbacks.append(RayTrainReportCallback(**callback_kwargs))
        train_kwargs["callbacks"] = callbacks

        train_fn_per_worker = partial(
            _xgboost_train_fn_per_worker,
            label_column=label_column,
            num_boost_round=num_boost_round,
            dataset_keys=set(datasets),
            xgboost_train_kwargs=train_kwargs,
        )

        super(XGBoostTrainer, self).__init__(
            train_loop_per_worker=train_fn_per_worker,
            train_loop_config=params,
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            dataset_config=dataset_config,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )

    @classmethod
    def get_model(
        cls,
        checkpoint: Checkpoint,
    ) -> xgboost.Booster:
        """Retrieve the XGBoost model stored in this checkpoint."""
        return RayTrainReportCallback.get_model(checkpoint)

    def _validate_attributes(self):
        super()._validate_attributes()

        if TRAIN_DATASET_KEY not in self.datasets:
            raise KeyError(
                f"'{TRAIN_DATASET_KEY}' key must be preset in `datasets`. "
                f"Got {list(self.datasets.keys())}"
            )
