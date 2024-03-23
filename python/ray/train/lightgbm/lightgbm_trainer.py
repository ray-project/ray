import logging
from functools import partial
from typing import Any, Dict, Optional

import lightgbm

import ray
from ray.train import Checkpoint
from ray.train.constants import _DEPRECATED_VALUE, TRAIN_DATASET_KEY
from ray.train.lightgbm import RayTrainReportCallback
from ray.train.lightgbm.v2 import LightGBMTrainer as SimpleLightGBMTrainer
from ray.train.trainer import GenDataset
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


def _lightgbm_train_fn_per_worker(
    config: dict,
    label_column: str,
    num_boost_round: int,
    dataset_keys: set,
    lightgbm_train_kwargs: dict,
):
    checkpoint = ray.train.get_checkpoint()
    starting_model = None
    remaining_iters = num_boost_round
    if checkpoint:
        starting_model = RayTrainReportCallback.get_model(checkpoint)
        starting_iter = starting_model.current_iteration()
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
    train_set = lightgbm.Dataset(train_X, label=train_y)

    # NOTE: Include the training dataset in the evaluation datasets.
    # This allows `train-*` metrics to be calculated and reported.
    valid_sets = [train_set]
    valid_names = [TRAIN_DATASET_KEY]

    for eval_name, eval_df in eval_dfs.items():
        eval_X, eval_y = eval_df.drop(label_column, axis=1), eval_df[label_column]
        valid_sets.append(lightgbm.Dataset(eval_X, label=eval_y))
        valid_names.append(eval_name)

    # Add network params of the worker group to enable distributed training.
    config.update(ray.train.lightgbm.v2.get_network_params())

    lightgbm.train(
        params=config,
        train_set=train_set,
        num_boost_round=remaining_iters,
        valid_sets=valid_sets,
        valid_names=valid_names,
        init_model=starting_model,
        **lightgbm_train_kwargs,
    )


@PublicAPI(stability="beta")
class LightGBMTrainer(SimpleLightGBMTrainer):
    """A Trainer for data parallel LightGBM training.

    This Trainer runs the LightGBM training loop in a distributed manner
    using multiple Ray Actors.

    If you would like to take advantage of LightGBM's built-in handling
    for features with the categorical data type, consider applying the
    :class:`Categorizer` preprocessor to set the dtypes in the dataset.

    .. note::
        ``LightGBMTrainer`` does not modify or otherwise alter the working
        of the LightGBM distributed training algorithm.
        Ray only provides orchestration, data ingest and fault tolerance.
        For more information on LightGBM distributed training, refer to
        `LightGBM documentation <https://lightgbm.readthedocs.io/>`__.

    Example:
        .. testcode::

            import ray

            from ray.train.lightgbm import LightGBMTrainer
            from ray.train import ScalingConfig

            train_dataset = ray.data.from_items(
                [{"x": x, "y": x + 1} for x in range(32)]
            )
            trainer = LightGBMTrainer(
                label_column="y",
                params={"objective": "regression"},
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
        params: LightGBM training parameters passed to ``lightgbm.train()``.
            Refer to `LightGBM documentation <https://lightgbm.readthedocs.io>`_
            for a list of possible parameters.
        num_boost_round: Target number of boosting iterations (trees in the model).
            Note that unlike in ``lightgbm.train``, this is the target number
            of trees, meaning that if you set ``num_boost_round=10`` and pass a model
            that has already been trained for 5 iterations, it will be trained for 5
            iterations more, instead of 10 more.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        resume_from_checkpoint: A checkpoint to resume training from.
        metadata: Dict that should be made available in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
        **train_kwargs: Additional kwargs passed to ``lightgbm.train()`` function.
    """

    _handles_checkpoint_freq = True
    _handles_checkpoint_at_end = True

    def __init__(
        self,
        *,
        datasets: Dict[str, GenDataset],
        label_column: str,
        params: Dict[str, Any],
        num_boost_round: int = 10,
        scaling_config: Optional[ray.train.ScalingConfig] = None,
        run_config: Optional[ray.train.RunConfig] = None,
        dataset_config: Optional[ray.train.DataConfig] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        metadata: Optional[Dict[str, Any]] = None,
        dmatrix_params: Optional[Dict[str, Dict[str, Any]]] = _DEPRECATED_VALUE,
        **train_kwargs,
    ):
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
            _lightgbm_train_fn_per_worker,
            label_column=label_column,
            num_boost_round=num_boost_round,
            dataset_keys=set(datasets),
            lightgbm_train_kwargs=train_kwargs,
        )

        super(LightGBMTrainer, self).__init__(
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
    ) -> lightgbm.Booster:
        """Retrieve the LightGBM model stored in this checkpoint."""
        return RayTrainReportCallback.get_model(checkpoint)

    def _validate_attributes(self):
        super()._validate_attributes()

        if TRAIN_DATASET_KEY not in self.datasets:
            raise KeyError(
                f"'{TRAIN_DATASET_KEY}' key must be preset in `datasets`. "
                f"Got {list(self.datasets.keys())}"
            )
