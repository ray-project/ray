from typing import Optional, Dict, Type, Union, Callable, Any

from ray.ml.checkpoint import Checkpoint
from ray.ml.config import ScalingConfig, RunConfig
from ray.ml.preprocessor import Preprocessor
from ray.ml.trainer import Trainer, GenDataset
from ray.rllib.agents.trainer import Trainer as RLLibTrainer
from ray.rllib.utils.typing import PartialTrainerConfigDict, EnvType
from ray.tune import Trainable, PlacementGroupFactory
from ray.tune.logger import Logger
from ray.tune.registry import get_trainable_cls
from ray.tune.resources import Resources
from ray.util.annotations import PublicAPI
from ray.util.ml_utils.dict import merge_dicts


@PublicAPI(stability="alpha")
class RLTrainer(Trainer):
    """Reinforcement learning trainer.

    This trainer provides an interface to RLLib trainables.

    If datasets and preprocessors are used, they can be utilized for
    offline training, e.g. using behavior cloning. Otherwise, this trainer
    will use online training.

    Args:
        algorithm: Algorithm to train on. Can be a string reference,
            (e.g. ``"PPO"``) or a RLLib trainer class.
        scaling_config: Configuration for distributed training, e.g. number
            of workers or resources per worker.
        run_config: Run config passed to ``Tuner()``
        datasets: If specified, datasets used for offline training. Will be
            configured as an RLLib ``input`` config item.
        preprocessor: If specified, preprocessors to be applied to the
            datasets before loading for input training.
        resume_from_checkpoint: Optional checkpoint to resume training from.
        **train_kwargs: Additional kwargs.

    Example:
        Online training:

        .. code-block:: python

            from ray.ml.config import RunConfig
            from ray.ml.train.integrations.rl import RLTrainer

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
            from ray.ml.config import RunConfig
            from ray.ml.train.integrations.rl import RLTrainer
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
        algorithm: Union[str, Type[RLLibTrainer]],
        config: Optional[Dict[str, Any]] = None,
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        **train_kwargs
    ):
        self._algorithm = algorithm
        self._train_kwargs = train_kwargs
        self._config = config if config is not None else {}

        Trainer.__init__(
            scaling_config=scaling_config,
            run_config=run_config,
            datasets=datasets,
            preprocessor=preprocessor,
            resume_from_checkpoint=resume_from_checkpoint,
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
        base_config = self._param_dict
        trainer_cls = self.__class__

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
                resolved_config = merge_dicts(base_config, config)

                trainer = trainer_cls(**resolved_config)
                rllib_config = trainer._get_rllib_config(process_datasets=True)

                super(AIRRLTrainer, self).__init__(
                    rllib_config,
                    env,
                    logger_creator,
                    remote_checkpoint_dir,
                    sync_function_tpl,
                )

            @classmethod
            def default_resource_request(
                cls, config: PartialTrainerConfigDict
            ) -> Union[Resources, PlacementGroupFactory]:
                resolved_config = merge_dicts(base_config, config)
                trainer = trainer_cls(**resolved_config)
                rllib_config = trainer._get_rllib_config(process_datasets=False)

                return rllib_trainer.default_resource_request(rllib_config)

        AIRRLTrainer.__name__ = rllib_trainer.__name__
        return AIRRLTrainer
