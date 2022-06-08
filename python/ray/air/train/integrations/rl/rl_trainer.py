import inspect
import os
from typing import Optional, Dict, Tuple, Type, Union, Callable, Any

import ray.cloudpickle as cpickle
from ray.air.checkpoint import Checkpoint
from ray.air.config import ScalingConfig, RunConfig
from ray.air.preprocessor import Preprocessor
from ray.air.trainer import Trainer, GenDataset
from ray.air._internal.checkpointing import (
    load_preprocessor_from_dir,
    save_preprocessor_to_dir,
)
from ray.rllib.agents.trainer import Trainer as RLlibTrainer
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import PartialTrainerConfigDict, EnvType
from ray.tune import Trainable, PlacementGroupFactory
from ray.tune.logger import Logger
from ray.tune.registry import get_trainable_cls
from ray.tune.resources import Resources
from ray.util.annotations import PublicAPI
from ray.util.ml_utils.dict import merge_dicts


RL_TRAINER_CLASS_FILE = "trainer_class.pkl"
RL_CONFIG_FILE = "config.pkl"


@PublicAPI(stability="alpha")
class RLTrainer(Trainer):
    """Reinforcement learning trainer.

    This trainer provides an interface to RLlib trainables.

    If datasets and preprocessors are used, they can be utilized for
    offline training, e.g. using behavior cloning. Otherwise, this trainer
    will use online training.

    Args:
        algorithm: Algorithm to train on. Can be a string reference,
            (e.g. ``"PPO"``) or a RLlib trainer class.
        scaling_config: Configuration for how to scale training.
        run_config: Configuration for the execution of the training run.
        datasets: Any Ray Datasets to use for training. Use the key "train"
            to denote which dataset is the training
            dataset. If a ``preprocessor`` is provided and has not already been fit,
            it will be fit on the training dataset. All datasets will be transformed
            by the ``preprocessor`` if one is provided.
            If specified, datasets will be used for offline training. Will be
            configured as an RLlib ``input`` config item.
        preprocessor: A preprocessor to preprocess the provided datasets.
        resume_from_checkpoint: A checkpoint to resume training from.

    Example:
        Online training:

        .. code-block:: python

            from ray.air.config import RunConfig
            from ray.air.train.integrations.rl import RLTrainer

            trainer = RLTrainer(
                run_config=RunConfig(stop={"training_iteration": 5}),
                scaling_config={
                    "num_workers": 2,
                    "use_gpu": False,
                },
                algorithm="PPO",
                config={
                    "env": "CartPole-v0",
                    "framework": "tf",
                    "evaluation_num_workers": 1,
                    "evaluation_interval": 1,
                    "evaluation_config": {"input": "sampler"},
                },
            )
            result = trainer.fit()


    Example:
        Offline training (assumes data is stored in ``/tmp/data-dir``):

        .. code-block:: python

            import ray
            from ray.air.config import RunConfig
            from ray.air.train.integrations.rl import RLTrainer
            from ray.rllib.agents.marwil.bc import BCTrainer

            dataset = ray.data.read_json(
                "/tmp/data-dir", parallelism=2, ray_remote_args={"num_cpus": 1}
            )

            trainer = RLTrainer(
                run_config=RunConfig(stop={"training_iteration": 5}),
                scaling_config={
                    "num_workers": 2,
                    "use_gpu": False,
                },
                datasets={"train": dataset},
                algorithm=BCTrainer,
                config={
                    "env": "CartPole-v0",
                    "framework": "tf",
                    "evaluation_num_workers": 1,
                    "evaluation_interval": 1,
                    "evaluation_config": {"input": "sampler"},
                },
            )
            result = trainer.fit()

    """

    def __init__(
        self,
        algorithm: Union[str, Type[RLlibTrainer]],
        config: Optional[Dict[str, Any]] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
    ):
        self._algorithm = algorithm
        self._config = config if config is not None else {}

        super(RLTrainer, self).__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def _validate_attributes(self):
        super(RLTrainer, self)._validate_attributes()

        if not isinstance(self._algorithm, str) and not (
            inspect.isclass(self._algorithm)
            and issubclass(self._algorithm, RLlibTrainer)
        ):
            raise ValueError(
                f"`algorithm` should be either a string or a RLlib trainer class, "
                f"found {type(self._algorithm)} with value `{self._algorithm}`."
            )

        if not isinstance(self._config, dict):
            raise ValueError(
                f"`config` should be either a dict, "
                f"found {type(self._config)} with value `{self._config}`."
            )

    def _get_rllib_config(self, process_datasets: bool = False) -> Dict:
        config = self._config.copy()
        num_workers = self.scaling_config.get("num_workers")
        if num_workers is not None:
            config["num_workers"] = num_workers

        worker_resources = self.scaling_config.get("resources_per_worker")
        if worker_resources:
            res = worker_resources.copy()
            config["num_cpus_per_worker"] = res.pop("CPU", 1)
            config["num_gpus_per_worker"] = res.pop("GPU", 0)
            config["custom_resources_per_worker"] = res

        use_gpu = self.scaling_config.get("use_gpu")
        if use_gpu:
            config["num_gpus"] = 1

        trainer_resources = self.scaling_config.get("trainer_resources")
        if trainer_resources:
            config["num_cpus_for_driver"] = trainer_resources.get("CPU", 1)

        if process_datasets:
            self.preprocess_datasets()
            # Up for discussion: If datasets is passed, should we always
            # set the input config? Is the sampler config required here, too?
            if self.datasets:
                config["input"] = "dataset"
                config["input_config"] = {
                    "loader_fn": lambda: self.datasets["train"],
                }

        return config

    def training_loop(self) -> None:
        pass

    def as_trainable(self) -> Type[Trainable]:
        param_dict = self._param_dict
        base_config = self._config or {}
        trainer_cls = self.__class__
        preprocessor = self.preprocessor

        if isinstance(self._algorithm, str):
            rllib_trainer = get_trainable_cls(self._algorithm)
        else:
            rllib_trainer = self._algorithm

        class AIRRLTrainer(rllib_trainer):
            def __init__(
                self,
                config: Optional[PartialTrainerConfigDict] = None,
                env: Optional[Union[str, EnvType]] = None,
                logger_creator: Optional[Callable[[], Logger]] = None,
                remote_checkpoint_dir: Optional[str] = None,
                sync_function_tpl: Optional[str] = None,
            ):
                resolved_config = merge_dicts(base_config, config or {})
                param_dict["config"] = resolved_config

                trainer = trainer_cls(**param_dict)
                rllib_config = trainer._get_rllib_config(process_datasets=True)

                super(AIRRLTrainer, self).__init__(
                    config=rllib_config,
                    env=env,
                    logger_creator=logger_creator,
                    remote_checkpoint_dir=remote_checkpoint_dir,
                    sync_function_tpl=sync_function_tpl,
                )

            def save_checkpoint(self, checkpoint_dir: str):
                checkpoint_path = super(AIRRLTrainer, self).save_checkpoint(
                    checkpoint_dir
                )

                trainer_class_path = os.path.join(checkpoint_dir, RL_TRAINER_CLASS_FILE)
                with open(trainer_class_path, "wb") as fp:
                    cpickle.dump(self.__class__, fp)

                config_path = os.path.join(checkpoint_dir, RL_CONFIG_FILE)
                with open(config_path, "wb") as fp:
                    cpickle.dump(self.config, fp)

                if preprocessor:
                    save_preprocessor_to_dir(preprocessor, checkpoint_dir)

                return checkpoint_path

            @classmethod
            def default_resource_request(
                cls, config: PartialTrainerConfigDict
            ) -> Union[Resources, PlacementGroupFactory]:
                resolved_config = merge_dicts(base_config, config)
                param_dict["config"] = resolved_config

                trainer = trainer_cls(**param_dict)
                rllib_config = trainer._get_rllib_config(process_datasets=False)

                return rllib_trainer.default_resource_request(rllib_config)

        AIRRLTrainer.__name__ = f"AIR{rllib_trainer.__name__}"
        return AIRRLTrainer


def load_checkpoint(
    checkpoint: Checkpoint,
    env: Optional[EnvType] = None,
) -> Tuple[Policy, Optional[Preprocessor]]:
    """Load a Checkpoint from ``RLTrainer``.

    Args:
        checkpoint: The checkpoint to load the policy and
            preprocessor from. It is expected to be from the result of a
            ``RLTrainer`` run.
        env: Optional environment to instantiate the trainer with. If not given,
            it is parsed from the saved trainer configuration instead.

    Returns:
        The policy and AIR preprocessor contained within.
    """
    with checkpoint.as_directory() as checkpoint_path:
        trainer_class_path = os.path.join(checkpoint_path, RL_TRAINER_CLASS_FILE)
        config_path = os.path.join(checkpoint_path, RL_CONFIG_FILE)

        if not os.path.exists(trainer_class_path):
            raise ValueError(
                f"RLPredictor only works with checkpoints created by "
                f"RLTrainer. The checkpoint you specified is missing the "
                f"`{RL_TRAINER_CLASS_FILE}` file."
            )

        if not os.path.exists(config_path):
            raise ValueError(
                f"RLPredictor only works with checkpoints created by "
                f"RLTrainer. The checkpoint you specified is missing the "
                f"`{RL_CONFIG_FILE}` file."
            )

        with open(trainer_class_path, "rb") as fp:
            trainer_cls = cpickle.load(fp)

        with open(config_path, "rb") as fp:
            config = cpickle.load(fp)

        checkpoint_data_path = None
        for file in os.listdir(checkpoint_path):
            if file.startswith("checkpoint") and not file.endswith(".tune_metadata"):
                checkpoint_data_path = os.path.join(checkpoint_path, file)

        if not checkpoint_data_path:
            raise ValueError(
                f"Could not find checkpoint data in RLlib checkpoint. "
                f"Found files: {list(os.listdir(checkpoint_path))}"
            )

        preprocessor = load_preprocessor_from_dir(checkpoint_path)

        config.get("evaluation_config", {}).pop("in_evaluation", None)
        trainer = trainer_cls(config=config, env=env)
        trainer.restore(checkpoint_data_path)

        policy = trainer.get_policy()
        return policy, preprocessor
