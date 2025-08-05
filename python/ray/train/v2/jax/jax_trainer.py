import logging
from typing import TYPE_CHECKING, Callable, Dict, Optional, Union

from ray.air._internal.config import ensure_only_allowed_dataclass_keys_updated
from ray.train import Checkpoint, DataConfig
from ray.train.trainer import GenDataset
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.jax.config import JaxConfig
from ray.util import PublicAPI

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class JaxTrainer(DataParallelTrainer):
    """A Trainer for Single-Program Multi-Data (SPMD) JAX training.
    Currently only supports TPUs. GPUs will be supported in a future version.

    This Trainer runs the function ``train_loop_per_worker`` on multiple Ray
    Actors. These actors are expected to be scheduled on TPU VMs within the same
    TPU slice, connected via inter-chip interconnects (ICI). The ``train_loop_per_worker``
    function is expected to take in either 0 or 1 arguments:

    .. code-block:: python

        def train_loop_per_worker():
            ...

        def train_loop_per_worker(config: Dict):
            ...

    If ``train_loop_per_worker`` accepts an argument, then
    ``train_loop_config`` will be passed in as the argument.

    If the ``datasets`` dict contains a training dataset (denoted by
    the "train" key), then it will be split into multiple dataset
    shards that can then be accessed by ``session.get_dataset_shard("train")``.

    Note:
        * Only TPU-based distributed training is supported.
        * Each worker must be assigned one TPU device via
          ``resources_per_worker={"TPU": 1}``.
        * Placement strategy is automatically set to ``SPREAD`` to ensure
          TPU workers are placed on separate VMs.
        * Importing `jax` should occur within `train_loop_per_worker` to
          avoid driver-side TPU lock issues.

    Args:
        train_loop_per_worker: The training function to execute.
        train_loop_config: Configurations to pass into
            ``train_loop_per_worker`` if it accepts an argument.
        jax_config: Configuration for setting up the JAX backend.
        scaling_config: Configuration for how to scale data parallel training
            with SPMD.
        dataset_config: Optional configuration for datasets.
        run_config: Configuration for the execution of the training run.
        datasets: Any Datasets to use for training. Use
            the key "train" to denote which dataset is the training dataset.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        jax_config: Optional[JaxConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DataConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not jax_config:
            jax_config = JaxConfig(
                use_tpu=scaling_config.use_tpu,
                topology=scaling_config.topology,
                accelerator_type=scaling_config.accelerator_type,
            )
        super(JaxTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=jax_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    @classmethod
    def _validate_scaling_config(cls, scaling_config: ScalingConfig) -> ScalingConfig:
        """Return scaling config dataclass after validating updated keys."""
        ensure_only_allowed_dataclass_keys_updated(
            dataclass=scaling_config,
            allowed_keys=cls._scaling_config_allowed_keys,
        )

        use_tpu = (
            scaling_config.resources_per_worker
            and scaling_config.resources_per_worker.get("TPU", 0) > 0
        )

        if use_tpu and "PACK" in scaling_config.placement_strategy:
            scaling_config.placement_strategy = "STRICT_SPREAD"
            logger.warning(
                "In JaxTrainer with TPU, `placement_strategy` must be `STRICT_SPREAD`. "
                "Overriding `placement_strategy` to `STRICT_SPREAD`."
            )

        return scaling_config
