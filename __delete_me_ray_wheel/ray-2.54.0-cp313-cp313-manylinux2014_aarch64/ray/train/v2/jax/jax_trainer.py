import logging
from typing import TYPE_CHECKING, Callable, Dict, Optional, Union

from ray.air._internal.config import ensure_only_allowed_dataclass_keys_updated
from ray.train import DataConfig
from ray.train.trainer import GenDataset
from ray.train.v2.api.config import RunConfig, ScalingConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.validation_config import ValidationConfig
from ray.train.v2.jax.config import JaxConfig
from ray.util import PublicAPI

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class JaxTrainer(DataParallelTrainer):
    """A Trainer for Single-Program Multi-Data (SPMD) JAX training.

    At a high level, this Trainer does the following:

    1. Launches multiple workers as defined by the ``scaling_config``.
    2. Sets up a distributed JAX environment for TPUs or GPUs
       on these workers as defined by the ``jax_config``.
    3. Ingests the input ``datasets`` based on the ``dataset_config``.
    4. Runs the input ``train_loop_per_worker(train_loop_config)``
       on all workers.

    For more details, see:

    * :ref:`Jax Guide <train-jax>`

    .. testcode::
        :skipif: True

        import os
        from absl import app
        import logging
        from typing import Sequence

        import ray
        from ray.train import ScalingConfig, RunConfig
        from ray.train.v2.jax import JaxTrainer
        from MaxText.train import main as maxtext_main

        def train_loop_per_worker(config):
            argv = config["argv"]
            maxtext_main(argv)

        def main(argv: Sequence[str]):
            ray.init()

            # If you want to use TPUs, specify the TPU topology and accelerator type.
            tpu_scaling_config = ScalingConfig(
                use_tpu=True,
                num_workers=4,
                topology="4x4",
                accelerator_type="TPU-V6E",
                placement_strategy="SPREAD",
                resources_per_worker={"TPU": 4},
            )

            # If you want to use GPUs, specify the GPU scaling config like below.
            # gpu_scaling_config = ScalingConfig(
            #     use_gpu=True,
            #     num_workers=4,
            #     resources_per_worker={"GPU": 1},
            # )


            trainer = JaxTrainer(
                train_loop_per_worker=train_loop_per_worker,
                train_loop_config={"argv": absolute_argv},
                scaling_config=tpu_scaling_config,
                run_config=RunConfig(
                    name="maxtext_jaxtrainer",
                    worker_runtime_env={
                        "env_vars": {
                            "JAX_PLATFORMS": "tpu",
                            # If you want to use GPUs, set the JAX_PLATFORMS to "cuda".
                            # "JAX_PLATFORMS": "cuda",
                        }
                    },
                ),
            )

            result = trainer.fit()

    If the ``datasets`` dict contains datasets (e.g. "train" and "val"), then it will be split into multiple dataset
    shards that can then be accessed by ``ray.train.get_dataset_shard("train")`` and ``ray.train.get_dataset_shard("val")``.

    Note:
        * If you are using TPUs, importing `jax` should occur within `train_loop_per_worker` to
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
            If set to None, a default configuration will be used based on the ``scaling_config`` and ``JAX_PLATFORMS`` environment variable.
        scaling_config: Configuration for how to scale data parallel training
            with SPMD. ``num_workers`` should be set to the number of TPU hosts or GPU workers.
            If using TPUs, ``topology`` should be set to the TPU topology.
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
        validation_config: [Alpha] Configuration for checkpoint validation.
            If provided and ``ray.train.report`` is called with the ``validation``
            argument, Ray Train will validate the reported checkpoint using
            the validation function specified in this config.
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
        validation_config: Optional[ValidationConfig] = None,
    ):
        if not jax_config:
            jax_config = JaxConfig(
                use_tpu=scaling_config.use_tpu,
                use_gpu=scaling_config.use_gpu,
            )
        super(JaxTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=jax_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            validation_config=validation_config,
        )

    @classmethod
    def _validate_scaling_config(cls, scaling_config: ScalingConfig) -> ScalingConfig:
        """Return scaling config dataclass after validating updated keys."""
        ensure_only_allowed_dataclass_keys_updated(
            dataclass=scaling_config,
            allowed_keys=cls._scaling_config_allowed_keys,
        )

        return scaling_config
