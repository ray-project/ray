from typing import Type, Optional, TYPE_CHECKING, Union, Dict
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.utils.from_config import NotProvided
from ray.rllib.core.rl_trainer.trainer_runner import TrainerRunner

if TYPE_CHECKING:
    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
    from ray.rllib.core.rl_trainer import RLTrainer

ModuleSpec = Union[SingleAgentRLModuleSpec, MultiAgentRLModuleSpec]


# TODO (Kourosh): We should make all configs come from a standard base class that
# defines the general interfaces for validation, from_dict, to_dict etc.
class TrainerRunnerConfig:
    """Configuration object for TrainerRunner."""

    def __init__(self, cls: Type[TrainerRunner] = None) -> None:

        # Define the default TrainerRunner class
        self.trainer_runner_class = cls or TrainerRunner

        # `self.module()`
        self.module_spec = None

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

        if self.module_spec is None:
            raise ValueError(
                "Cannot initialize an RLTrainer without the module specs. "
                "Please provide the specs via .module(module_spec)."
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

        # If the module class is a multi agent class it will override the default
        # MultiAgentRLModule class. otherwise, it will be a single agent wrapped with
        # mutliagent
        # TODO (Kourosh): What should be scaling_config? it's not clear what
        # should be passed in as trainer_config and what will be inferred
        return self.trainer_runner_class(
            trainer_class=self.trainer_class,
            trainer_config={
                "module_spec": self.module_spec,
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
        module_spec: Optional[ModuleSpec] = NotProvided,
    ) -> "TrainerRunnerConfig":

        if module_spec is not NotProvided:
            self.module_spec = module_spec

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
