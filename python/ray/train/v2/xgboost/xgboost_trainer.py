"""V2 XGBoost Trainer with External Memory Support.

This module provides a V2-compliant XGBoost trainer that supports both standard
DMatrix creation for smaller datasets and external memory optimization for large
datasets that don't fit in RAM.
"""

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import ray.train
from ray.train import Checkpoint
from ray.train.trainer import GenDataset
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.util.annotations import Deprecated

if TYPE_CHECKING:
    from ray.train.xgboost import XGBoostConfig

logger = logging.getLogger(__name__)


class XGBoostTrainer(DataParallelTrainer):
    """A Trainer for distributed data-parallel XGBoost training.

    This trainer supports both standard DMatrix creation for smaller datasets
    and external memory optimization for large datasets that don't fit in RAM.

    Examples:
        .. testcode::

            import xgboost

            import ray.data
            import ray.train
            from ray.train.xgboost import RayTrainReportCallback
            from ray.train.v2.xgboost import XGBoostTrainer

            def train_fn_per_worker(config: dict):
                # (Optional) Add logic to resume training state from a checkpoint.
                # ray.train.get_checkpoint()

                # 1. Get the dataset shard for the worker and convert to a `xgboost.DMatrix`
                train_ds_iter, eval_ds_iter = (
                    ray.train.get_dataset_shard("train"),
                    ray.train.get_dataset_shard("validation"),
                )

                # Check if external memory is enabled via config
                use_external_memory = config.get("use_external_memory", False)
                external_memory_cache_dir = config.get("external_memory_cache_dir")
                external_memory_device = config.get("external_memory_device", "cpu")
                external_memory_batch_size = config.get("external_memory_batch_size")

                if use_external_memory:
                    # Option 2: External memory DMatrix for large datasets
                    import xgboost as xgb

                    # Create external memory DMatrix using the trainer's method
                    dtrain = trainer.create_external_memory_dmatrix(
                        dataset_shard=train_ds_iter,
                        label_column="y",
                        batch_size=external_memory_batch_size,
                        cache_dir=external_memory_cache_dir,
                        device=external_memory_device,
                    )
                    deval = trainer.create_external_memory_dmatrix(
                        dataset_shard=eval_ds_iter,
                        label_column="y",
                        batch_size=external_memory_batch_size,
                        cache_dir=external_memory_cache_dir,
                        device=external_memory_device,
                    )

                    # Use hist tree method (required for external memory)
                    params = {
                        "tree_method": "hist",  # Required for external memory
                        "objective": "reg:squarederror",
                        "eta": 1e-4,
                        "subsample": 0.5,
                        "max_depth": 2,
                    }
                else:
                    # Option 1: Standard DMatrix for smaller datasets (default)
                    train_ds, eval_ds = train_ds_iter.materialize(), eval_ds_iter.materialize()
                    train_df, eval_df = train_ds.to_pandas(), eval_ds.to_pandas()
                    train_X, train_y = train_df.drop("y", axis=1), train_df["y"]
                    eval_X, eval_y = eval_df.drop("y", axis=1), eval_df["y"]

                    dtrain = xgboost.DMatrix(train_X, label=train_y)
                    deval = xgboost.DMatrix(eval_X, label=eval_y)

                    # Standard parameters
                    params = {
                        "tree_method": "approx",  # Can use approx for standard DMatrix
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

            # Standard training (in-memory)
            train_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(32)])
            eval_ds = ray.data.from_items([{"x": x, "y": x + 1} for x in range(16)])
            trainer = XGBoostTrainer(
                train_loop_per_worker=train_fn_per_worker,
                datasets={"train": train_ds, "validation": eval_ds},
                scaling_config=ray.train.ScalingConfig(num_workers=4),
            )
            result = trainer.fit()
            booster = RayTrainReportCallback.get_model(result.checkpoint)

            # External memory training for large datasets
            large_trainer = XGBoostTrainer(
                train_loop_per_worker=train_fn_per_worker,
                datasets={"train": large_train_ds, "validation": large_eval_ds},
                scaling_config=ray.train.ScalingConfig(num_workers=4),
                use_external_memory=True,
                external_memory_cache_dir="/mnt/cluster_storage",  # Shared storage
                external_memory_device="cpu",  # or "cuda" for GPU
                external_memory_batch_size=50000,  # Optimal batch size
            )
            result = large_trainer.fit()

    Args:
        train_loop_per_worker: The training function to execute on each worker.
            This function can either take in zero arguments or a single ``Dict``
            argument which is set by defining ``train_loop_config``.
            Within this function you can use any of the
            :ref:`Ray Train Loop utilities <train-loop-api>`.
        train_loop_config: A configuration ``Dict`` to pass in as an argument to
            ``train_loop_per_worker``.
            This is typically used for specifying hyperparameters.
        xgboost_config: The configuration for setting up the distributed xgboost
            backend. Defaults to using the "rabit" backend.
            See :class:`~ray.train.xgboost.XGBoostConfig` for more info.
        scaling_config: The configuration for how to scale data parallel training.
            ``num_workers`` determines how many Python processes are used for training,
            and ``use_gpu`` determines whether or not each process should use GPUs.
            See :class:`~ray.train.ScalingConfig` for more info.
        run_config: The configuration for the execution of the training run.
            See :class:`~ray.train.RunConfig` for more info.
        datasets: The Ray Datasets to ingest for training.
            Datasets are keyed by name (``{name: dataset}``).
            Each dataset can be accessed from within the ``train_loop_per_worker``
            by calling ``ray.train.get_dataset_shard(name)``.
            Sharding and additional configuration can be done by
            passing in a ``dataset_config``.
        dataset_config: The configuration for ingesting the input ``datasets``.
            By default, all the Ray Dataset are split equally across workers.
            See :class:`~ray.train.DataConfig` for more details.
        resume_from_checkpoint: A checkpoint to resume training from.
            This checkpoint can be accessed from within ``train_loop_per_worker``
            by calling ``ray.train.get_checkpoint()``.
        metadata: Dict that should be made available via
            `ray.train.get_context().get_metadata()` and in `checkpoint.get_metadata()`
            for checkpoints saved from this Trainer. Must be JSON-serializable.
        use_external_memory: Whether to use external memory for DMatrix creation.
            If True, uses ExtMemQuantileDMatrix for large datasets that don't fit in RAM.
            If False (default), uses standard DMatrix for in-memory training.
        external_memory_cache_dir: Directory for caching external memory files.
            If None, automatically selects the best available directory.
        external_memory_device: Device to use for external memory training.
            Options: "cpu" (default) or "cuda" for GPU training.
        external_memory_batch_size: Batch size for external memory iteration.
            If None, uses optimal default based on device type.
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        xgboost_config: Optional["XGBoostConfig"] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        dataset_config: Optional[ray.train.DataConfig] = None,
        # TODO: [Deprecated]
        metadata: Optional[Dict[str, Any]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        # External memory configuration
        use_external_memory: bool = False,
        external_memory_cache_dir: Optional[str] = None,
        external_memory_device: str = "cpu",
        external_memory_batch_size: Optional[int] = None,
    ):
        # Legacy API parameters were removed from V2 trainer
        # V2 trainer only supports train_loop_per_worker pattern

        # Store external memory configuration
        self.use_external_memory = use_external_memory
        self.external_memory_cache_dir = external_memory_cache_dir
        self.external_memory_device = external_memory_device
        self.external_memory_batch_size = external_memory_batch_size

        # Inject external memory configuration into train_loop_config
        if train_loop_config is None:
            train_loop_config = {}

        # Add external memory settings to config so training function can access them
        train_loop_config.update({
            "use_external_memory": use_external_memory,
            "external_memory_cache_dir": external_memory_cache_dir,
            "external_memory_device": external_memory_device,
            "external_memory_batch_size": external_memory_batch_size,
        })

        # Handle XGBoostConfig import conditionally
        if xgboost_config is None:
            try:
                from ray.train.xgboost import XGBoostConfig

                backend_config = XGBoostConfig()
            except ImportError:
                # If XGBoost is not available, use None as backend
                backend_config = None
        else:
            backend_config = xgboost_config

        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=backend_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )

    @classmethod
    @Deprecated
    def get_model(cls, checkpoint: Checkpoint):
        """[Deprecated] Retrieve the XGBoost model stored in this checkpoint."""
        raise DeprecationWarning(
            "`XGBoostTrainer.get_model` is deprecated. "
            "Use `RayTrainReportCallback.get_model` instead."
        )

    def create_dmatrix(
        self,
        dataset_shard,
        label_column: Union[str, List[str]],
        feature_columns: Optional[List[str]] = None,
        **kwargs,
    ):
        """Create an XGBoost DMatrix using the trainer's configuration.

        This method automatically chooses between standard DMatrix and external memory
        DMatrix based on the trainer's `use_external_memory` setting.

        Args:
            dataset_shard: Ray dataset shard to convert to DMatrix.
            label_column: Name(s) of the label column(s).
            feature_columns: Names of feature columns. If None, all non-label columns are used.
            **kwargs: Additional arguments passed to DMatrix creation.

        Returns:
            XGBoost DMatrix object (either standard or external memory).

        Raises:
            ImportError: If XGBoost is not properly installed.
            RuntimeError: If DMatrix creation fails.

        Examples:
            .. testcode::

                # Inside train_loop_per_worker
                train_dmatrix = trainer.create_dmatrix(
                    ray.train.get_dataset_shard("train"),
                    label_column="target",
                )

        Note:
            This method requires XGBoost to be installed and the trainer to be
            properly configured. For external memory training, ensure
            `use_external_memory=True` is set in the trainer constructor.
        """
        if self.use_external_memory:
            return self.create_external_memory_dmatrix(
                dataset_shard=dataset_shard,
                label_column=label_column,
                feature_columns=feature_columns,
                **kwargs,
            )
        else:
            return self.create_standard_dmatrix(
                dataset_shard=dataset_shard,
                label_column=label_column,
                feature_columns=feature_columns,
                **kwargs,
            )

    def create_standard_dmatrix(
        self,
        dataset_shard,
        label_column: Union[str, List[str]],
        feature_columns: Optional[List[str]] = None,
        **kwargs,
    ):
        """Create a standard XGBoost DMatrix for in-memory training.

        Args:
            dataset_shard: Ray dataset shard to convert to DMatrix.
            label_column: Name(s) of the label column(s).
            feature_columns: Names of feature columns. If None, all non-label columns are used.
            **kwargs: Additional arguments passed to DMatrix creation.

        Returns:
            Standard XGBoost DMatrix object.

        Raises:
            ImportError: If XGBoost is not properly installed.
            RuntimeError: If DMatrix creation fails.
        """
        try:
            import xgboost as xgb
        except ImportError:
            raise ImportError(
                "XGBoost is required for standard DMatrix creation. "
                "Install with: pip install xgboost"
            )

        # Materialize the dataset shard
        ds = dataset_shard.materialize()
        df = ds.to_pandas()

        # Separate features and labels
        if isinstance(label_column, str):
            labels = df[label_column]
            features = df.drop(columns=[label_column])
        else:
            labels = df[label_column]
            features = df.drop(columns=label_column)

        # Handle feature columns selection
        if feature_columns is not None:
            features = features[feature_columns]

        # Create standard DMatrix
        dmatrix = xgb.DMatrix(features, label=labels, **kwargs)

        logger.info(
            f"Created standard DMatrix with {features.shape[0]} samples and "
            f"{features.shape[1]} features"
        )

        return dmatrix

    def create_external_memory_dmatrix(
        self,
        dataset_shard,
        label_column: Union[str, List[str]],
        feature_columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
        cache_dir: Optional[str] = None,
        device: Optional[str] = None,
        max_bin: Optional[int] = None,
        **kwargs,
    ) -> "xgboost.DMatrix":
        """Create an XGBoost ExtMemQuantileDMatrix with external memory optimization.

        This method creates an XGBoost ExtMemQuantileDMatrix that uses external memory
        for training on large Ray datasets that don't fit in memory.

        Following XGBoost's official external memory API:
        - Uses ExtMemQuantileDMatrix for hist tree method (required)
        - Supports both CPU and GPU training
        - Implements proper DataIter interface
        - Caches data in external memory and fetches on-demand

        Args:
            dataset_shard: Ray dataset shard to convert.
            label_column: Name(s) of the label column(s).
            feature_columns: Names of feature columns. If None, all non-label columns are used.
            batch_size: Batch size for external memory iteration. If None, uses trainer's default.
            cache_dir: Directory for caching external memory files. If None, uses trainer's default.
            device: Device to use for external memory training. If None, uses trainer's default.
            max_bin: Maximum number of bins for histogram construction.
            **kwargs: Additional arguments passed to ExtMemQuantileDMatrix constructor.

        Returns:
            XGBoost ExtMemQuantileDMatrix object optimized for external memory training.

        Examples:
            .. testcode::

                def train_fn_per_worker(config: dict):
                    train_ds_iter = ray.train.get_dataset_shard("train")

                    # Use external memory DMatrix
                    dtrain = trainer.create_external_memory_dmatrix(
                        train_ds_iter, label_column="target"
                    )

                    # Train as usual
                    bst = xgboost.train(config, dtrain=dtrain, ...)

        Note:
            This method requires XGBoost 3.0+ and the hist tree method.
            The trainer must be configured with use_external_memory=True.
            For optimal performance, use tree_method="hist" and grow_policy="depthwise".
        """
        # Use trainer's configuration if not explicitly provided
        if batch_size is None:
            batch_size = self.external_memory_batch_size
        if cache_dir is None:
            cache_dir = self.external_memory_cache_dir
        if device is None:
            device = self.external_memory_device

        # Import shared utilities
        from ray.train.xgboost._external_memory_utils import create_external_memory_dmatrix

        return create_external_memory_dmatrix(
            dataset_shard=dataset_shard,
            label_column=label_column,
            feature_columns=feature_columns,
            batch_size=batch_size,
            cache_dir=cache_dir,
            device=device,
            max_bin=max_bin,
            **kwargs,
        )

    def setup_gpu_external_memory(self) -> bool:
        """Setup GPU external memory training with RMM optimization.

        This method configures RAPIDS Memory Manager (RMM) for optimal GPU external
        memory performance. It should be called before creating external memory DMatrix
        objects for GPU training.

        Returns:
            True if GPU setup was successful, False otherwise.

        Examples:
            .. testcode::

                # Setup GPU external memory before training
                if trainer.external_memory_device == "cuda":
                    trainer.setup_gpu_external_memory()

        Note:
            This method requires XGBoost, RMM, and CuPy to be installed for GPU training.
            For CPU training, this method is not required.
        """
        from ray.train.xgboost._external_memory_utils import setup_gpu_external_memory

        return setup_gpu_external_memory()

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
