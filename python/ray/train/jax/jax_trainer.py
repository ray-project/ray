from typing import Callable, Optional, Dict, Union, TYPE_CHECKING
import logging

from ray.air._internal.config import ensure_only_allowed_dataclass_keys_updated
from ray.train.jax.config import JaxConfig
from ray.train.trainer import GenDataset
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.air.config import ScalingConfig, RunConfig, DatasetConfig
from ray.air.checkpoint import Checkpoint
from ray.util import PublicAPI

if TYPE_CHECKING:
    from ray.data.preprocessor import Preprocessor

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class JaxTrainer(DataParallelTrainer):
    """A Trainer for data parallel jax training.

    This Trainer runs the function ``train_loop_per_worker`` on multiple Ray
    Actors. These actors already have the necessary jax process group already
    configured for distributed jax training.

    The ``train_loop_per_worker`` function is expected to take in either 0 or 1
    arguments:

    .. code-block:: python

        def train_loop_per_worker():
            ...

    .. code-block:: python

        def train_loop_per_worker(config: Dict):
            ...

    If ``train_loop_per_worker`` accepts an argument, then
    ``train_loop_config`` will be passed in as the argument. This is useful if you
    want to tune the values in ``train_loop_config`` as hyperparameters.

    If the ``datasets`` dict contains a training dataset (denoted by
    the "train" key), then it will be split into multiple dataset
    shards that can then be accessed by ``session.get_dataset_shard("train")`` inside
    ``train_loop_per_worker``. All the other datasets will not be split and
    ``session.get_dataset_shard(...)`` will return the the entire Dataset.

    Note: Here are some details about how to use the ``JaxTrainer`` properly.

    * cluster scaling config:
        * For the GPU distributed cases, the inner-device communication is handled
            internally by Jax; the inter-device communication is set up by the
            ``JaxTrainer``. Therefore, the ``num_workers`` is set to be the number of
            nodes; and ``num_gpus_per_worker`` is set to be the number of gpus on each
            node. For the current experimental version, we only support the homogeneous
            distributed case, i.e. all the nodes have the same number of gpus. This is
            also the optimal / balanced case in terms of performance.
        * For the TPU distributed cases, the multi-device distributed training only
            supports the TPU pods for a) the TPU pods can host up to several thousands
            of TPU cores; b) the communication speed is also optimal for the TPU pods.
            Therefore, the ``num_workers`` is set to be the number of TPU-VMs
            (say ``num_workers=4`` for TPU-pod v2-32); and
            ``resources_per_worker={"TPU": 1}`` is used to set the TPU resource.
            Since the TPU resources are not the default resources on Ray,
            we need to set the ``resources`` when set up the cluster,
            e.g. ``ray start --resources='{"TPU":1}'``. For more details, please
            refer to `Ray Jax TPU Examples </train/examples/jax_tpu>`
            end to end example.
        * For the CPU distributed cases, the multi-device distributed training
            is not supported.
        * the placement group strategy: Since the JaxTrainer will spread the workers
            across different nodes, the default value of ``placement_strategy``
            is changed to ``SPREAD``.

    * ``train_loop_per_worker`` functions:
        **NOTE**:
        #. the flax nn module has to define inside the ``train_loop_per_worker``
            function. Otherwise, the error message ``ValueError: parent must be
            None, Module or Scope`` will be thrown. see more details:
            https://github.com/google/flax/discussions/1390.

            .. code-block:: python

            def train_func():
                from flax import linen as nn

                class NeuralNet(nn.Module):
                    pass

        #. ``import jax`` is encouraged to also put inside the trainer function
            because in the TPU case, `import jax` in the driver process
            will create the TPU lock file and block the training process.

    Inside the ``train_loop_per_worker`` function, you can use any of the
    :ref:`Ray AIR session methods <air-session-ref>`.

    .. code-block:: python

        def train_loop_per_worker():
            # Report intermediate results for callbacks or logging and
            # checkpoint data.
            session.report(...)

            # Returns dict of last saved checkpoint.
            session.get_checkpoint()

            # Returns the Ray Dataset shard for the given key.
            session.get_dataset_shard("my_dataset")

            # Returns the total number of workers executing training.
            session.get_world_size()

            # Returns the rank of this worker.
            session.get_world_rank()

            # Returns the rank of the worker on the current node.
            session.get_local_rank()

    Args:
        train_loop_per_worker: The training function to execute.
            This can either take in no arguments or a ``config`` dict.
        train_loop_config: Configurations to pass into
            ``train_loop_per_worker`` if it accepts an argument.
        jax_config: Configuration for setting up the jax backend. If set to
            None, use the default configuration. This replaces the ``backend_config``
            arg of ``DataParallelTrainer``.
        scaling_config: Configuration for how to scale data parallel training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use
            the key "train" to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
        preprocessor: A ``ray.data.Preprocessor`` to preprocess the
            provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.
    """

    def __init__(
        self,
        train_loop_per_worker: Union[Callable[[], None], Callable[[Dict], None]],
        *,
        train_loop_config: Optional[Dict] = None,
        jax_config: Optional[JaxConfig] = None,
        scaling_config: Optional[ScalingConfig] = None,
        dataset_config: Optional[Dict[str, DatasetConfig]] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional["Preprocessor"] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        if not jax_config:
            jax_config = JaxConfig()

        super(JaxTrainer, self).__init__(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config=train_loop_config,
            backend_config=jax_config,
            scaling_config=scaling_config,
            dataset_config=dataset_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    @classmethod
    def _validate_scaling_config(cls, scaling_config: ScalingConfig) -> ScalingConfig:
        """Return scaling config dataclass after validating updated keys."""
        ensure_only_allowed_dataclass_keys_updated(
            dataclass=scaling_config,
            allowed_keys=cls._scaling_config_allowed_keys,
        )

        use_gpu = scaling_config.use_gpu
        use_tpu = (
            scaling_config.resources_per_worker
            and scaling_config.resources_per_worker.get("TPU", 0) > 0
        )

        # cpu parallelism is not supported in jax
        if not (use_gpu or use_tpu):
            logger.warning(
                "CPU parallelism is not supported in jax. "
                "Please use distributed GPU or TPU training instead. "
                "Currently, the code is still running on CPU, "
                "but there is no distributed training happening."
            )

        # change the default value of `placment_strategy` to `SPREAD`.
        # using `SPREAD`: the jaxtrainer will spread the workers across different nodes.
        # between the nodes, the connections is to be established by NCCL;
        # within the nodes, jax can already enable communicates automatically.
        # so, no end to use jax backend to make the connections.
        # NOT `STRICT_SPREAD`: if the users use the trainer on one of GPU nodes instead
        # of on the extra cpu nodes, then `STRICT_SPREAD` will hang on for the resources
        if "PACK" in scaling_config.placement_strategy:
            scaling_config.placement_strategy = "SPREAD"
            logger.warning(
                "In JaxTrainer, the `placement_stategy` need to be `SPREAD`."
                " Placement strategy is now changed to `SPREAD`"
            )

        return scaling_config
