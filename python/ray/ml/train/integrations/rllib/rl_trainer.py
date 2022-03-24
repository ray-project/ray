from typing import Optional, Dict, Type, Union, Callable

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
    def __init__(
        self,
        algorithm: Union[str, RLLibTrainer],
        scaling_config: Optional[ScalingConfig] = None,
        run_config: Optional[RunConfig] = None,
        datasets: Optional[Dict[str, GenDataset]] = None,
        preprocessor: Optional[Preprocessor] = None,
        resume_from_checkpoint: Optional[Checkpoint] = None,
        **train_kwargs
    ):
        self._algorithm = algorithm
        self._train_kwargs = train_kwargs

        super().__init__(
            scaling_config, run_config, datasets, preprocessor, resume_from_checkpoint
        )

    def _get_rllib_config(self) -> Dict:
        config = self._train_kwargs.get("param_space", {}).copy()
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
                rllib_config = trainer._get_rllib_config()

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
                rllib_config = trainer._get_rllib_config()

                return rllib_trainer.default_resource_request(rllib_config)

        AIRRLTrainer.__name__ = rllib_trainer.__name__
        return AIRRLTrainer
