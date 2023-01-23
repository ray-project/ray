from typing import Type, Optional, TYPE_CHECKING, Union, Dict
from ray.rllib.utils.from_config import NotProvided
from ray.rllib.core.rl_trainer.trainer_runner import TrainerRunner

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.core.rl_module import RLModule
    from ray.rllib.core.rl_trainer import RLTrainer
    import gymnasium as gym


# TODO (Kourosh): We should make all configs come from a standard base class that
# defines the general interfaces for validation, from_dict, to_dict etc.
class TrainerRunnerConfig:
    """Configuration object for TrainerRunner."""

    def __init__(self, cls: Type[TrainerRunner] = None) -> None:

        # Define the default TrainerRunner class
        self.trainer_runner_class = cls or TrainerRunner

        # `self.module()`
        self.module_obj = None
        self.module_class = None
        self.observation_space = None
        self.action_space = None
        self.model_config = None

        # `self.trainer()`
        self.trainer_class = None
        self.eager_tracing = True
        self.optimizer_config = None

        # `self.resources()`
        self.num_gpus = 0
        self.fake_gpus = False

        # `self.algorithm()`
        self.algorithm_config = None

    def validate(self) -> None:

        if self.module_class is None and self.module_obj is None:
            raise ValueError(
                "Cannot initialize an RLTrainer without an RLModule. Please provide "
                "the RLModule class with .module(module_class=MyModuleClass) or "
                "an RLModule instance with .module(module=MyModuleInstance)."
            )

        if self.module_class is not None:
            if self.observation_space is None:
                raise ValueError(
                    "Must provide observation_space for RLModule when RLModule class "
                    "is provided. Use .module(observation_space=MySpace)."
                )
            if self.action_space is None:
                raise ValueError(
                    "Must provide action_space for RLModule when RLModule class "
                    "is provided. Use .module(action_space=MySpace)."
                )
            if self.model_config is None:
                raise ValueError(
                    "Must provide model_config for RLModule when RLModule class "
                    "is provided. Use .module(model_config=MyConfig)."
                )

        if self.trainer_class is None:
            raise ValueError(
                "Cannot initialize an RLTrainer without an RLTrainer. Please provide "
                "the RLTrainer class with .trainer(trainer_class=MyTrainerClass)."
            )

        if self.algorithm_config is None:
            raise ValueError(
                "Must provide algorithm_config for RLTrainer. Use "
                ".algorithm(algorithm_config=MyConfig)."
            )

        if self.optimizer_config is None:
            # get the default optimizer config if it's not provided
            # TODO (Kourosh): Change the optimizer config to a dataclass object.
            self.optimizer_config = {"lr": 1e-3}

        if self.fake_gpus and self.num_gpus <= 0:
            raise ValueError("If fake_gpus is True, num_gpus must be greater than 0.")

    def build(self) -> TrainerRunner:
        self.validate()
        # TODO (Kourosh): What should be scaling_config? it's not clear what
        # should be passed in as trainer_config and what will be inferred
        return self.trainer_runner_class(
            trainer_class=self.trainer_class,
            trainer_config={
                "module_class": self.module_class,
                "module_kwargs": {
                    "observation_space": self.observation_space,
                    "action_space": self.action_space,
                    "model_config": self.model_config,
                },
                # TODO (Kourosh): should this be inferred inside the constructor?
                "distributed": self.num_gpus > 1,
                # TODO (Avnish): add this
                # "enable_tf_function": self.eager_tracing,
                "optimizer_config": self.optimizer_config,
                "algorithm_config": self.algorithm_config,
            },
            compute_config={
                "num_gpus": self.num_gpus,
                # TODO (Avnish): add this
                # "fake_gpus": self.fake_gpus,
            },
        )

    def algorithm(
        self, algorithm_config: Optional["AlgorithmConfig"] = NotProvided
    ) -> "TrainerRunnerConfig":
        if algorithm_config is not NotProvided:
            self.algorithm_config = algorithm_config
        return self

    def module(
        self,
        *,
        module_class: Optional[Type["RLModule"]] = NotProvided,
        observation_space: Optional["gym.Space"] = NotProvided,
        action_space: Optional["gym.Space"] = NotProvided,
        model_config: Optional[dict] = NotProvided,
        module: Optional["RLModule"] = NotProvided,
    ) -> "TrainerRunnerConfig":

        if module is NotProvided and module_class is NotProvided:
            raise ValueError(
                "Must provide either module or module_class. Please provide "
                "the RLModule class with .module(module=MyModule) or "
                ".module(module_class=MyModuleClass)."
            )

        if module_class is not NotProvided:
            self.module_class = module_class
        if observation_space is not NotProvided:
            self.observation_space = observation_space
        if action_space is not NotProvided:
            self.action_space = action_space
        if model_config is not NotProvided:
            self.model_config = model_config
        if module is not NotProvided:
            self.module_obj = module

        return self

    def trainer(
        self,
        *,
        trainer_class: Optional[Type["RLTrainer"]] = NotProvided,
        eager_tracing: Optional[bool] = NotProvided,
        optimizer_config: Optional[Dict] = NotProvided,
    ) -> "TrainerRunnerConfig":

        if trainer_class is not NotProvided:
            self.trainer_class = trainer_class
        if eager_tracing is not NotProvided:
            self.eager_tracing = eager_tracing
        if optimizer_config is not NotProvided:
            self.optimizer_config = optimizer_config

        return self

    def resources(
        self,
        num_gpus: Optional[Union[float, int]] = NotProvided,
        fake_gpus: Optional[bool] = NotProvided,
    ) -> "TrainerRunnerConfig":

        if num_gpus is not NotProvided:
            self.num_gpus = num_gpus
        if fake_gpus is not NotProvided:
            self.fake_gpus = fake_gpus

        return self
