import logging
from functools import partial
from typing import Any, Callable, Dict, Optional, Union

import lightgbm

import ray
from ray.train import Checkpoint
from ray.train.constants import _DEPRECATED_VALUE, TRAIN_DATASET_KEY
from ray.train.lightgbm import RayTrainReportCallback, LightGBMConfig
from ray.train.lightgbm.v2 import LightGBMTrainer as SimpleLightGBMTrainer
from ray.train.trainer import GenDataset
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


LEGACY_LightGBM_TRAINER_DEPRECATION_MESSAGE = (
    "Passing in `lightgbm.train` kwargs such as `params`, `num_boost_round`, "
    "`label_column`, etc. to `LightGBMTrainer` is deprecated "
    "in favor of the new API which accepts a ``train_loop_per_worker`` argument, "
    "similar to the other DataParallelTrainer APIs (ex: TorchTrainer). "
    "See this issue for more context: "
    "https://github.com/ray-project/ray/issues/50042"
)


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
    """A Trainer for distributed data-parallel LightGBM training.

        Example
        -------
        .. testcode::

            import lightgbm

            import ray.data
            import ray.train
            from ray.train.lightgbm import RayTrainReportCallback, LightGBMTrainer

            def train_fn_per_worker(config: dict):
                # (Optional) Add logic to resume training state from a checkpoint.
                # ray.train.get_checkpoint()
                
                # 1. Get the dataset shard for the worker and convert to a `xgboost.DMatrix`
                train_ds_iter, eval_ds_iter = (
                    ray.train.get_dataset_shard("train"),
                    ray.train.get_dataset_shard("validation"),
                )
                train_ds, eval_ds = train_ds_iter.materialize(), eval_ds_iter.materialize()
                train_df, eval_df = train_ds.to_pandas(), eval_ds.to_pandas()
                train_X, train_y = train_df.drop("y", axis=1), train_df["y"]
                eval_X, eval_y = eval_df.drop("y", axis=1), eval_df["y"]
                dtrain = xgboost.DMatrix(train_X, label=train_y)
                deval = xgboost.DMatrix(eval_X, label=eval_y)
                params = {
                    "tree_method": "approx",
                    "objective": "reg:squarederror",
                    "eta": 1e-4,
                    "subsample": 0.5,
                    "max_depth": 2,
                }
                # 2. Do distributed data-parallel training.
                # Ray Train sets up the necessary coordinator processes and
                # environment variables for your workers to communicate with each other.
                bst = xgboost.train(
                    params,
                    dtrain=dtrain,
                    evals=[(deval, "validation")],
                    num_boost_round=10,
                    callbacks=[RayTrainReportCallback()],
                )
                
            train_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
            eval_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(16)])
            trainer = LightGBMTrainer(
                train_fn_per_worker,
                datasets={"train": train_ds, "validation": eval_ds},
                scaling_config=ray.train.ScalingConfig(num_workers=4),
            )
            result = trainer.fit()
            booster = RayTrainReportCallback.get_model(result.checkpoint)

        .. testoutput::
            :hide:

            ...

    Args:
        train_loop_per_worker: A function to execute on each worker for training.
            If None, will use the legacy API parameters.
        train_loop_config: Configuration to pass to train_loop_per_worker.
        lightgbm_config: LightGBM-specific configuration.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        datasets: The Ray Datasets to use for training and validation.
        dataset_config: Configuration for dataset ingest.
        resume_from_checkpoint: A checkpoint to resume training from.
        metadata: Metadata to be saved in checkpoints.
        label_column: (Legacy) Name of the label column.
        params: (Legacy) LightGBM training parameters.
        num_boost_round: (Legacy) Number of boosting rounds.
        **train_kwargs: (Legacy) Additional kwargs passed to lightgbm.train().
    """

    _handles_checkpoint_freq = True
    _handles_checkpoint_at_end = True

    def __init__(
        self,
        train_loop_per_worker: Optional[
            Union[Callable[[], None], Callable[[Dict], None]]
        ] = None,
        *,        
        train_loop_config: Optional[Dict] = None,
        lightgbm_config: Optional[LightGBMConfig] = None,
        scaling_config: Optional[ray.train.ScalingConfig] = None,
        run_config: Optional[ray.train.RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        dataset_config: Optional[ray.train.DataConfig] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        metadata: Optional[Dict[str, Any]] = None,
        # TODO: [Deprecated] Legacy LightGBMTrainer API
        label_column: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        num_boost_round: Optional[int] = None,
        **train_kwargs,
    ):
        """Initialize a LightGBM trainer.
        
        This trainer supports both the legacy API and new v2 API:
        """
        if (train_loop_per_worker is not None or train_loop_config is not None) and (
            label_column is not None or params is not None or num_boost_round is not None
        ):
            raise ValueError(
                "Cannot mix v2 API parameters (train_loop_per_worker, train_loop_config) "
                "with legacy API parameters (label_column, params, num_boost_round)"
            )

        if label_column is not None or params is not None or num_boost_round is not None:
            import warnings
            warnings.warn(
                LEGACY_LightGBM_TRAINER_DEPRECATION_MESSAGE,
                DeprecationWarning,
                stacklevel=2,
            )

            # Initialize legacy callback handling
            callbacks = train_kwargs.get("callbacks", [])
            user_supplied_callback = any(
                isinstance(callback, RayTrainReportCallback) for callback in callbacks
            )
            callback_kwargs = {}
            if run_config:
                checkpoint_frequency = run_config.checkpoint_config.checkpoint_frequency
                checkpoint_at_end = run_config.checkpoint_config.checkpoint_at_end
                callback_kwargs["frequency"] = checkpoint_frequency
                callback_kwargs["checkpoint_at_end"] = (
                    checkpoint_at_end if checkpoint_at_end is not None else True
                )

            if not user_supplied_callback:
                callbacks.append(RayTrainReportCallback(**callback_kwargs))
            train_kwargs["callbacks"] = callbacks

            train_loop_per_worker = partial(
                _lightgbm_train_fn_per_worker,
                label_column=label_column,
                num_boost_round=num_boost_round or 10,
                dataset_keys=set(datasets or {}),
                lightgbm_train_kwargs=train_kwargs,
            )
            train_loop_config = params

        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
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
