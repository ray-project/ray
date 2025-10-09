import logging
from functools import partial
from typing import Any, Dict, Optional

import ray.train
from ray.train.constants import TRAIN_DATASET_KEY
from ray.train.scaling_config import ScalingConfig
from ray.train.run_config import RunConfig
from ray.train.trainer import GenDataset
from ray.train.xgboost import RayTrainReportCallback
from ray.train.xgboost.v2 import XGBoostTrainer as SimpleXGBoostTrainer
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


# Constants for external memory configuration
DEFAULT_EXTERNAL_MEMORY_DEVICE = "cpu"
MAX_EXTERNAL_MEMORY_RETRIES = 3

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
    the configuration. It manages checkpointing, dataset iteration, and
    training progress tracking.

    Args:
        config: XGBoost training configuration parameters. Should include
            tree_method, objective, and evaluation metrics.
        label_column: Name of the label column in the dataset. Must exist
            in all datasets.
        num_boost_round: Target number of boosting rounds for training.
            When resuming from checkpoint, trains for remaining rounds.
        dataset_keys: Set of dataset names available for training. Should
            include at least TRAIN_DATASET_KEY.
        xgboost_train_kwargs: Additional XGBoost training arguments such as
            callbacks, verbose settings, etc.
        use_external_memory: Whether to use external memory for DMatrix creation.
            Required for large datasets that don't fit in RAM.
        external_memory_cache_dir: Directory for caching external memory files.
            Should be on fast storage with sufficient space.
        external_memory_device: Device to use for external memory training
            ("cpu" or "cuda").
        external_memory_batch_size: Batch size for external memory iteration.
            Larger values improve I/O efficiency but use more memory.

    Raises:
        ValueError: If required datasets or columns are missing.
        RuntimeError: If DMatrix creation or training fails.

    Note:
        This function runs on each distributed worker. It automatically handles:
        - Checkpoint resumption
        - Dataset sharding
        - DMatrix creation (standard or external memory)
        - Model training and reporting
    """
    # Handle checkpoint resumption
    checkpoint = ray.train.get_checkpoint()
    starting_model = None
    remaining_iters = num_boost_round

    if checkpoint:
        try:
            starting_model = RayTrainReportCallback.get_model(checkpoint)
            starting_iter = starting_model.num_boosted_rounds()
            remaining_iters = num_boost_round - starting_iter

            if remaining_iters <= 0:
                logger.warning(
                    f"Model from checkpoint already has {starting_iter} rounds, "
                    f"which meets or exceeds target ({num_boost_round}). "
                    "No additional training will be performed."
                )
                return
            
            logger.info(
                f"Resuming from checkpoint: model has {starting_iter} rounds, "
                f"will train {remaining_iters} more to reach {num_boost_round}"
            )
        except Exception as e:
            logger.error(f"Failed to load model from checkpoint: {e}")
            raise RuntimeError(
                f"Checkpoint loading failed: {e}. "
                "Ensure checkpoint is compatible with current XGBoost version."
            ) from e

    train_ds_iter = ray.train.get_dataset_shard(TRAIN_DATASET_KEY)

    if use_external_memory:
        # Use external memory for large datasets
        import xgboost as xgb

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

        # Create external memory DMatrix using shared utilities
        from ._external_memory_utils import create_external_memory_dmatrix

        logger.info(
            f"Creating external memory DMatrix for training "
            f"(device={external_memory_device}, "
            f"batch_size={external_memory_batch_size})"
        )

        try:
            dtrain = create_external_memory_dmatrix(
                dataset_shard=train_ds_iter,
                label_column=label_column,
                batch_size=external_memory_batch_size,
                cache_dir=external_memory_cache_dir,
                device=external_memory_device,
            )
        except Exception as e:
            logger.error(f"Failed to create training DMatrix: {e}")
            raise RuntimeError(
                f"Training DMatrix creation failed: {e}. "
                "Check dataset format and external memory configuration."
            ) from e

        # Create evaluation datasets with external memory
        evals = [(dtrain, TRAIN_DATASET_KEY)]

        for eval_name in dataset_keys:
            if eval_name != TRAIN_DATASET_KEY:
                try:
                    eval_ds_iter = ray.train.get_dataset_shard(eval_name)
                    logger.debug(f"Creating DMatrix for evaluation dataset: {eval_name}")
                    deval = create_external_memory_dmatrix(
                        dataset_shard=eval_ds_iter,
                        label_column=label_column,
                        batch_size=external_memory_batch_size,
                        cache_dir=external_memory_cache_dir,
                        device=external_memory_device,
                    )
                    evals.append((deval, eval_name))
                except Exception as e:
                    logger.error(
                        f"Failed to create DMatrix for '{eval_name}': {e}"
                    )
                    raise RuntimeError(
                        f"Evaluation DMatrix creation failed for '{eval_name}': {e}"
                    ) from e

        logger.info(
            f"Successfully created {len(evals)} DMatrix objects "
            f"(1 training + {len(evals)-1} evaluation)"
        )

    else:
        # Use standard DMatrix for smaller datasets
        import xgboost as xgb

        logger.info("Creating standard in-memory DMatrix for training")

        try:
            train_ds = train_ds_iter.materialize()
            train_df = train_ds.to_pandas()

            # Validate training data
            if train_df.empty:
                raise ValueError("Training dataset is empty")

            if label_column not in train_df.columns:
                raise ValueError(
                    f"Label column '{label_column}' not found in training data. "
                    f"Available columns: {list(train_df.columns)}"
                )

            # Separate features and labels
            train_X = train_df.drop(columns=[label_column])
            train_y = train_df[label_column]

            logger.debug(
                f"Training data: {len(train_df)} samples, "
                f"{len(train_X.columns)} features"
            )

            # Create standard DMatrix
            dtrain = xgb.DMatrix(train_X, label=train_y)

        except Exception as e:
            logger.error(f"Failed to create training DMatrix: {e}")
            raise RuntimeError(
                f"Training DMatrix creation failed: {e}. "
                "Check dataset format and label column name."
            ) from e

        # Create evaluation datasets
        evals = [(dtrain, TRAIN_DATASET_KEY)]

        for eval_name in dataset_keys:
            if eval_name != TRAIN_DATASET_KEY:
                try:
                    eval_ds_iter = ray.train.get_dataset_shard(eval_name)
                    eval_ds = eval_ds_iter.materialize()
                    eval_df = eval_ds.to_pandas()

                    if eval_df.empty:
                        logger.warning(f"Evaluation dataset '{eval_name}' is empty")
                        continue

                    if label_column not in eval_df.columns:
                        raise ValueError(
                            f"Label column '{label_column}' not found in '{eval_name}'. "
                            f"Available: {list(eval_df.columns)}"
                        )

                    eval_X = eval_df.drop(columns=[label_column])
                    eval_y = eval_df[label_column]

                    deval = xgb.DMatrix(eval_X, label=eval_y)
                    evals.append((deval, eval_name))

                    logger.debug(
                        f"Evaluation dataset '{eval_name}': {len(eval_df)} samples"
                    )

                except Exception as e:
                    logger.error(f"Failed to create DMatrix for '{eval_name}': {e}")
                    raise RuntimeError(
                        f"Evaluation DMatrix creation failed for '{eval_name}': {e}"
                    ) from e

        logger.info(
            f"Successfully created {len(evals)} DMatrix objects "
            f"(1 training + {len(evals)-1} evaluation)"
        )

    # Train the model
    logger.info(
        f"Starting XGBoost training: {remaining_iters} rounds, "
        f"{len(evals)} evaluation sets"
    )

    try:
        bst = xgb.train(
            config,
            dtrain=dtrain,
            evals=evals,
            num_boost_round=remaining_iters,
            xgb_model=starting_model,
            callbacks=[RayTrainReportCallback()],
            **xgboost_train_kwargs,
        )

        if bst is None:
            raise RuntimeError("xgb.train returned None")

        logger.info(
            f"Training completed successfully: "
            f"{bst.num_boosted_rounds()} total rounds"
        )

        # Report final metrics
        ray.train.report({"model": bst})

    except Exception as e:
        logger.error(f"Training failed: {e}")
        raise RuntimeError(
            f"XGBoost training failed: {e}. "
            "Check parameters, data quality, and system resources."
        ) from e


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

    @staticmethod
    def setup_gpu_external_memory() -> bool:
        """Setup GPU external memory training with RMM optimization.

        This method configures RAPIDS Memory Manager (RMM) for optimal GPU external
        memory performance. It should be called before creating external memory DMatrix
        objects for GPU training.

        Returns:
            True if GPU setup was successful, False otherwise.

        Examples:
            .. testcode::

                # Setup GPU external memory before training
                if XGBoostTrainer.setup_gpu_external_memory():
                    print("GPU external memory setup successful")

        Note:
            This method requires XGBoost, RMM, and CuPy to be installed for GPU training.
            For CPU training, this method is not required.
        """
        from ._external_memory_utils import setup_gpu_external_memory

        return setup_gpu_external_memory()

    @staticmethod
    def get_external_memory_recommendations() -> Dict[str, Any]:
        """Get recommendations for external memory training configuration.

        Returns:
            Dictionary containing recommended configuration settings and best practices.

        Examples:
            .. testcode::

                recommendations = XGBoostTrainer.get_external_memory_recommendations()
                print(f"Recommended parameters: {recommendations['parameters']}")
        """
        from ._external_memory_utils import get_external_memory_recommendations

        return get_external_memory_recommendations()

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
