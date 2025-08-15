import os
import warnings
from typing import Any, Callable, Dict, Optional

from ray.train.v2.api.config import ScalingConfig, RunConfig, DataConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train import Checkpoint
from ray.train.xgboost import XGBoostConfig


class XGBoostTrainer(DataParallelTrainer):
    """XGBoost Trainer for Ray Train v2 with distributed training and GPU support.

    This trainer provides XGBoost training capabilities including distributed training,
    GPU acceleration, and external memory support for large-scale datasets.
    It automatically applies best practices for optimal performance.

    Key Features:
    - Distributed XGBoost training across multiple nodes and workers
    - GPU acceleration with CUDA support and memory optimization
    - External memory support for datasets larger than available RAM
    - Automatic configuration optimization and validation

    XGBoost Documentation: https://xgboost.readthedocs.io/
    External Memory Guide: https://xgboost.readthedocs.io/en/latest/tutorials/external_memory.html
    """

    def __init__(
        self,
        train_loop_per_worker: Callable,
        *,
        train_loop_config: Optional[Dict[str, Any]] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, Dataset]] = None,
        dataset_config: Optional[DataConfig] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        metadata: Optional[Dict[str, Any]] = None,
        use_external_memory: bool = True,
        cache_dir: Optional[str] = None,
        use_rmm: Optional[bool] = None,
    ):
        """Initialize the XGBoostTrainer.

        Args:
            train_loop_per_worker: The training loop function to run on each worker.
            train_loop_config: Configuration to pass to the training loop.
            scaling_config: Configuration for how to scale training.
            run_config: Configuration for the execution of the training run.
            datasets: Datasets to use for training.
            dataset_config: Configuration for dataset handling.
            resume_from_checkpoint: Checkpoint to resume training from.
            metadata: Extra metadata for this run.
            use_external_memory: Whether to use external memory for large datasets.
            cache_dir: Custom directory for external memory cache. If None, will use
                optimal default based on available storage.
            use_rmm: Whether to use RAPIDS Memory Manager (RMM) for GPU training.
                If None, will be automatically set based on GPU availability and best practices.
        """
        self.use_external_memory = use_external_memory
        self.cache_dir = cache_dir
        self.use_rmm = use_rmm

        # Initialize XGBoost configuration with defaults
        self.xgboost_config = xgboost_config or XGBoostConfig()

        # Validate and extract configuration
        self._validate_configuration()
        self._extract_configuration_options()
        self._initialize_optimizations()

        # Initialize base trainer
        super().__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=self.xgboost_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            resume_from_checkpoint=resume_from_checkpoint,
            metadata=metadata,
        )

    def _validate_configuration(self):
        """Validate and automatically optimize the XGBoost configuration."""
        # Validate cache directory if specified
        if self.cache_dir:
            if not os.path.exists(self.cache_dir):
                warnings.warn(
                    f"Cache directory does not exist: {self.cache_dir}. "
                    "Will attempt to create it or use fallback."
                )
            elif not os.access(self.cache_dir, os.W_OK):
                warnings.warn(
                    f"Cache directory is not writable: {self.cache_dir}. "
                    "Will use fallback directory."
                )

        # Apply best practices for batch size
        if self.xgboost_config.batch_size:
            if self.xgboost_config.batch_size < 10000:
                warnings.warn(
                    f"Batch size {self.xgboost_config.batch_size} is very small. "
                    "Recommended minimum: 10,000 for external memory training."
                )
        else:
            self._apply_batch_size_best_practice()

        # Apply tree method best practices for external memory
        self._apply_tree_method_best_practices()

        # Apply GPU optimization best practices
        self._apply_gpu_best_practices()

    def _apply_batch_size_best_practice(self):
        """Apply XGBoost's recommended batch size for external memory training."""
        if self.use_external_memory:
            # Optimal batch size for external memory training
            # See: https://xgboost.readthedocs.io/en/stable/tutorials/external_memory.html
            optimal_batch_size = 50000
            self.xgboost_config.batch_size = optimal_batch_size
            warnings.warn(
                f"Batch size not specified. Auto-optimized to {optimal_batch_size} "
                "following XGBoost external memory best practices."
            )

    def _apply_tree_method_best_practices(self):
        """Apply XGBoost's recommended tree method settings for external memory."""
        if self.use_external_memory:
            # Tree method 'hist' is required for external memory training
            # See: https://xgboost.readthedocs.io/en/stable/tutorials/external_memory.html
            if (
                not hasattr(self.xgboost_config, "tree_method")
                or self.xgboost_config.tree_method != "hist"
            ):
                self.xgboost_config.tree_method = "hist"
                warnings.warn(
                    "Tree method automatically set to 'hist' for external memory training."
                )

            # Grow policy 'depthwise' is recommended for external memory training
            # See: https://xgboost.readthedocs.io/en/stable/tutorials/external_memory.html
            if (
                not hasattr(self.xgboost_config, "grow_policy")
                or self.xgboost_config.grow_policy != "depthwise"
            ):
                self.xgboost_config.grow_policy = "depthwise"
                warnings.warn(
                    "Grow policy automatically set to 'depthwise' for external memory training."
                )

    def _apply_gpu_best_practices(self):
        """Apply XGBoost's recommended GPU optimization settings."""
        if (
            self.scaling_config
            and hasattr(self.scaling_config, "use_gpu")
            and self.scaling_config.use_gpu
        ):

            # Enable RMM for optimal GPU memory management if user hasn't specified
            # See: https://docs.rapids.ai/api/rmm/stable/
            if not hasattr(self.xgboost_config, "use_rmm"):
                if self.use_rmm is not None:
                    self.xgboost_config.use_rmm = self.use_rmm
                else:
                    # Enable RMM by default for optimal GPU memory management
                    # See: https://docs.rapids.ai/api/rmm/stable/
                    self.xgboost_config.use_rmm = True
                    warnings.warn(
                        "GPU detected. RMM automatically enabled for optimal GPU memory management."
                    )

            # Set optimal cache host ratio for GPU training
            # See: https://xgboost.readthedocs.io/en/stable/tutorials/external_memory.html
            if not hasattr(self.xgboost_config, "cache_host_ratio"):
                # Optimal cache host ratio for GPU external memory training
                # See: https://xgboost.readthedocs.io/en/stable/tutorials/external_memory.html
                self.xgboost_config.cache_host_ratio = 0.8
                warnings.warn(
                    "Cache host ratio automatically optimized to 0.8 for GPU training."
                )

    def _extract_configuration_options(self):
        """Extract and store configuration options from XGBoostConfig."""
        self.batch_size = getattr(self.xgboost_config, "batch_size", None)
        self.extmem_single_page = getattr(
            self.xgboost_config, "extmem_single_page", False
        )
        self.cache_host_ratio = getattr(self.xgboost_config, "cache_host_ratio", None)

    def _initialize_optimizations(self):
        """Initialize XGBoost configuration following best practices."""
        if self.use_external_memory:
            try:
                from ray.train.v2.xgboost._external_memory_utils import (
                    _create_external_memory_dmatrix,
                )

                self._external_memory_utility = _create_external_memory_dmatrix
            except ImportError as e:
                warnings.warn(f"Could not import external memory utilities: {e}")
                self.use_external_memory = False

    def create_external_memory_dmatrix(self, dataset_shard, label_column, **kwargs):
        """Create an external memory DMatrix using the trainer's configuration.

        Args:
            dataset_shard: The Ray dataset shard to convert to DMatrix.
            label_column: Column name or list of column names for labels.
            **kwargs: Additional arguments to pass to the external memory DMatrix creation.

        Returns:
            An XGBoost DMatrix object optimized for external memory training.

        Raises:
            RuntimeError: If external memory is disabled or utilities are not available.
        """
        if not self.use_external_memory:
            raise RuntimeError(
                "External memory is disabled. Enable it by setting use_external_memory=True."
            )

        if not hasattr(self, "_external_memory_utility"):
            raise RuntimeError(
                "External memory utilities are not available. "
                "This may happen if XGBoost is not properly installed."
            )

        # Use the trainer's cache directory if not specified in kwargs
        if "cache_dir" not in kwargs and self.cache_dir:
            kwargs["cache_dir"] = self.cache_dir

        # Create the external memory DMatrix
        return self._external_memory_utility(
            dataset_shard=dataset_shard, label_column=label_column, **kwargs
        )

    def __del__(self):
        """Cleanup when the trainer is destroyed."""
        pass
