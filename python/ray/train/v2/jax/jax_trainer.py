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

    .. testcode::

        import os
        from absl import app
        import logging
        from typing import Sequence

        import ray
        from ray.train.v2.api.config import ScalingConfig, RunConfig
        from ray.train.v2.jax import JaxTrainer
        from MaxText.train import main as maxtext_main

        def train_loop_per_worker(config):
            argv = config["argv"]
            maxtext_main(argv)

        def main(argv: Sequence[str]):
            ray.init()

            trainer = JaxTrainer(
                train_loop_per_worker=train_loop_per_worker,
                train_loop_config={"argv": absolute_argv},
                scaling_config=ScalingConfig(
                    use_tpu=True,
                    num_workers=4,
                    topology="4x4",
                    accelerator_type="TPU-V6E",
                    resources_per_worker={"TPU": 4},
                    placement_strategy="SPREAD",
                ),
                run_config=RunConfig(
                    name="maxtext_jaxtrainer",
                    worker_runtime_env={
                        "env_vars": {
                            "JAX_PLATFORMS": "tpu",
                            "ENABLE_PJRT_COMPATIBILITY": "true",
                            "TPU_SLICE_BUILDER_DUMP_CHIP_FORCE": "true",
                            "TPU_SLICE_BUILDER_DUMP_ICI": "true",
                            "XLA_FLAGS": "--xla_dump_to=/tmp/xla_dump_file --xla_dump_hlo_as_proto",
                        }
                    },
                ),
            )

            result = trainer.fit()

    .. testoutput::
        :options: +ELLIPSIS
        :hide:

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
        train_loop_per_worker: The training function to execute on each worker.
            This function can either take in zero arguments or a single ``Dict``
            argument which is set by defining ``train_loop_config``.
            Within this function you can use any of the
            :ref:`Ray Train Loop utilities <train-loop-api>`.
        train_loop_config: A configuration ``Dict`` to pass in as an argument to
            ``train_loop_per_worker``.
            This is typically used for specifying hyperparameters. Passing large
            datasets via `train_loop_config` is not recommended and may introduce
            large overhead and unknown issues with serialization and deserialization.
        jax_config: The configuration for setting up the JAX backend.
            If set to None, a default configuration with TPUs will be used.
        scaling_config: Configuration for how to scale data parallel training
            with SPMD. ``num_workers`` should be set to the number of TPU hosts
            and ``topology`` should be set to the TPU topology.
            See :class:`~ray.train.ScalingConfig` for more info.
        dataset_config: The configuration for ingesting the input ``datasets``.
            By default, all the Ray Dataset are split equally across workers.
            See :class:`~ray.train.DataConfig` for more details.
        run_config: The configuration for the execution of the training run.
            See :class:`~ray.train.RunConfig` for more info.
        datasets: The Ray Datasets to ingest for training.
            Datasets are keyed by name (``{name: dataset}``).
            Each dataset can be accessed from within the ``train_loop_per_worker``
            by calling ``ray.train.get_dataset_shard(name)``.
            Sharding and additional configuration can be done by
            passing in a ``dataset_config``.
        resume_from_checkpoint: A checkpoint to resume training from.
            This checkpoint can be accessed from within ``train_loop_per_worker``
            by calling ``ray.train.get_checkpoint()``.
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

        return scaling_config
