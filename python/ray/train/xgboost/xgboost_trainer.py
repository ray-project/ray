import logging
from functools import partial
from typing import Any, Callable, Dict, Optional, Union

import xgboost
from packaging.version import Version

import ray.train
from ray.train import Checkpoint
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.scaling_config import ScalingConfig
from ray.train.run_config import RunConfig
from ray.train.trainer import GenDataset
from ray.train.xgboost import RayTrainReportCallback, XGBoostConfig
from ray.train.xgboost.v2 import XGBoostTrainer as SimpleXGBoostTrainer
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


LEGACY_XGBOOST_TRAINER_DEPRECATION_MESSAGE = (
    "Passing in `xgboost.train` kwargs such as `params`, `num_boost_round`, "
    "`label_column`, etc. to `XGBoostTrainer` is deprecated "
    "in favor of the new API which accepts a ``train_loop_per_worker`` argument, "
    "similar to the other DataParallelTrainer APIs (ex: TorchTrainer). "
    "See this issue for more context: "
    "https://github.com/ray-project/ray/issues/50042"
)


def _xgboost_train_fn_per_worker(
    config: dict,
    label_column: str,
    num_boost_round: int,
    dataset_keys: set,
    xgboost_train_kwargs: dict,
    use_external_memory: bool = False,
    external_memory_cache_dir: Optional[str] = None,
    external_memory_device: str = "cpu",
    external_memory_batch_size: Optional[int] = None,
):
    """Training function executed on each worker for XGBoost training.

    This function handles both standard and external memory training modes,
    automatically selecting the appropriate DMatrix creation method based on
    the configuration.

    Args:
        config: XGBoost training configuration parameters.
        label_column: Name of the label column in the dataset.
        num_boost_round: Number of boosting rounds for training.
        dataset_keys: Set of dataset names available for training.
        xgboost_train_kwargs: Additional XGBoost training arguments.
        use_external_memory: Whether to use external memory for DMatrix creation.
        external_memory_cache_dir: Directory for caching external memory files.
        external_memory_device: Device to use for external memory training.
        external_memory_batch_size: Batch size for external memory iteration.
    """
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

    if use_external_memory:
        # Use external memory for large datasets
        import xgboost as xgb

        # Create external memory DMatrix using shared utilities
        from ._external_memory_utils import create_external_memory_dmatrix

        dtrain = create_external_memory_dmatrix(
            dataset_shard=train_ds_iter,
            label_column=label_column,
            batch_size=external_memory_batch_size,
            cache_dir=external_memory_cache_dir,
            device=external_memory_device,
        )

        # Create evaluation datasets with external memory
        evals = [(dtrain, TRAIN_DATASET_KEY)]

        for eval_name in dataset_keys:
            if eval_name != TRAIN_DATASET_KEY:
                eval_ds_iter = ray.train.get_dataset_shard(eval_name)
                deval = create_external_memory_dmatrix(
                    dataset_shard=eval_ds_iter,
                    label_column=label_column,
                    batch_size=external_memory_batch_size,
                    cache_dir=external_memory_cache_dir,
                    device=external_memory_device,
                )
                evals.append((deval, eval_name))

            # External memory requires hist tree method for optimal performance
            # This is a requirement from XGBoost's official external memory API
            if "tree_method" not in config:
                config["tree_method"] = "hist"
            elif config["tree_method"] != "hist":
                logger.warning(
                    f"External memory training requires tree_method='hist' for optimal performance. "
                    f"Current setting: {config['tree_method']}. "
                    "Consider changing to 'hist' for better external memory performance."
                )

            # Recommend depthwise grow policy for external memory
            if "grow_policy" not in config:
                config["grow_policy"] = "depthwise"
            elif config["grow_policy"] == "lossguide":
                logger.warning(
                    "Using grow_policy='lossguide' with external memory can significantly "
                    "slow down training. Consider using 'depthwise' for better performance."
                )

    else:
        # Use standard DMatrix for smaller datasets
        train_ds = train_ds_iter.materialize()
        train_df = train_ds.to_pandas()

        # Separate features and labels
        train_X = train_df.drop(columns=[label_column])
        train_y = train_df[label_column]

        # Create standard DMatrix
        dtrain = xgb.DMatrix(train_X, label=train_y)

        # Create evaluation datasets
        evals = [(dtrain, TRAIN_DATASET_KEY)]

        for eval_name in dataset_keys:
            if eval_name != TRAIN_DATASET_KEY:
                eval_ds_iter = ray.train.get_dataset_shard(eval_name)
                eval_ds = eval_ds_iter.materialize()
                eval_df = eval_ds.to_pandas()

                eval_X = eval_df.drop(columns=[label_column])
                eval_y = eval_df[label_column]

                deval = xgb.DMatrix(eval_X, label=eval_y)
                evals.append((deval, eval_name))

    # Train the model
    bst = xgb.train(
        config,
        dtrain=dtrain,
        evals=evals,
        num_boost_round=remaining_iters,
        xgb_model=starting_model,
        callbacks=[RayTrainReportCallback()],
        **xgboost_train_kwargs,
    )

    # Report final metrics
    ray.train.report({"model": bst})


@PublicAPI(stability="beta")
class XGBoostTrainer(SimpleXGBoostTrainer):
    """A Trainer for distributed data-parallel XGBoost training.

    This trainer supports both standard DMatrix creation for smaller datasets
    and external memory optimization for large datasets that don't fit in RAM.

    Examples:
        .. testcode::

            import ray
            import ray.data
            from ray.train.xgboost import XGBoostTrainer

            # Create sample datasets
            train_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(1000)])
            val_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(100)])

            # Standard training (in-memory)
            trainer = XGBoostTrainer(
                scaling_config=ray.train.ScalingConfig(num_workers=2),
                run_config=ray.train.RunConfig(),
                datasets={"train": train_ds, "validation": val_ds},
                label_column="y",
                params={"objective": "reg:squarederror", "max_depth": 3},
                num_boost_round=10,
            )
            result = trainer.fit()

            # External memory training for large datasets
            large_trainer = XGBoostTrainer(
                scaling_config=ray.train.ScalingConfig(num_workers=2),
                run_config=ray.train.RunConfig(),
                datasets={"train": large_train_ds, "validation": large_val_ds},
                label_column="y",
                params={"objective": "reg:squarederror", "max_depth": 3},
                num_boost_round=10,
                use_external_memory=True,
                external_memory_cache_dir="/mnt/cluster_storage",
                external_memory_device="cpu",
                external_memory_batch_size=50000,
            )
            result = large_trainer.fit()

    Args:
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        datasets: The Ray Datasets to ingest for training.
        label_column: Name of the label column in the dataset.
        params: XGBoost training parameters.
        num_boost_round: Number of boosting rounds for training.
        use_external_memory: Whether to use external memory for DMatrix creation.
            If True, uses ExtMemQuantileDMatrix for large datasets that don't fit in RAM.
            If False (default), uses standard DMatrix for in-memory training.
        external_memory_cache_dir: Directory for caching external memory files.
            If None, automatically selects the best available directory.
        external_memory_device: Device to use for external memory training.
            Options: "cpu" (default) or "cuda" for GPU training.
        external_memory_batch_size: Batch size for external memory iteration.
            If None, uses optimal default based on device type.
        **kwargs: Additional arguments passed to the base trainer.
    """

    def __init__(
        self,
        *,
        scaling_config: ScalingConfig,
        run_config: RunConfig,
        datasets: Dict[str, GenDataset],
        label_column: str,
        params: Dict[str, Any],
        num_boost_round: int,
        use_external_memory: bool = False,
        external_memory_cache_dir: Optional[str] = None,
        external_memory_device: str = "cpu",
        external_memory_batch_size: Optional[int] = None,
        **kwargs,
    ):
        """Initialize the XGBoost trainer.

        Args:
            scaling_config: Configuration for how to scale data parallel training.
            run_config: Configuration for the execution of the training run.
            datasets: The Ray Datasets to ingest for training.
            label_column: Name of the label column in the dataset.
            params: XGBoost training parameters.
            num_boost_round: Number of boosting rounds for training.
            use_external_memory: Whether to use external memory for DMatrix creation.
            external_memory_cache_dir: Directory for caching external memory files.
            external_memory_device: Device to use for external memory training.
            external_memory_batch_size: Batch size for external memory iteration.
            **kwargs: Additional arguments passed to the base trainer.
        """
        # Store external memory configuration
        self.use_external_memory = use_external_memory
        self.external_memory_cache_dir = external_memory_cache_dir
        self.external_memory_device = external_memory_device
        self.external_memory_batch_size = external_memory_batch_size

        # Create training function with external memory support
        train_fn_per_worker = partial(
            _xgboost_train_fn_per_worker,
            label_column=label_column,
            num_boost_round=num_boost_round,
            dataset_keys=set(datasets.keys()),
            xgboost_train_kwargs=params,
            use_external_memory=use_external_memory,
            external_memory_cache_dir=external_memory_cache_dir,
            external_memory_device=external_memory_device,
            external_memory_batch_size=external_memory_batch_size,
        )

        # Initialize the base trainer
        super().__init__(
            train_loop_per_worker=train_fn_per_worker,
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            **kwargs,
        )

    def get_external_memory_config(self) -> Dict[str, Any]:
        """Get external memory configuration.

        Returns:
            Dictionary containing external memory configuration settings.

        Examples:
            .. testcode::

                config = trainer.get_external_memory_config()
                print(f"External memory enabled: {config['use_external_memory']}")
                print(f"Cache directory: {config['cache_dir']}")
                print(f"Device: {config['device']}")
                print(f"Batch size: {config['batch_size']}")
        """
        return {
            "use_external_memory": self.use_external_memory,
            "cache_dir": self.external_memory_cache_dir,
            "device": self.external_memory_device,
            "batch_size": self.external_memory_batch_size,
        }

    def is_external_memory_enabled(self) -> bool:
        """Check if external memory is enabled.

        Returns:
            True if external memory is enabled, False otherwise.

        Examples:
            .. testcode::

                if trainer.is_external_memory_enabled():
                    print("Using external memory for large dataset training")
                else:
                    print("Using standard in-memory training")
        """
        return self.use_external_memory
