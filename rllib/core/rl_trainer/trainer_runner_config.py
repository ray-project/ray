
from typing import Type, Optional, TYPE_CHECKING, Union
from ray.rllib.utils.from_config import NotProvided
from ray.rllib.core.rl_trainer.trainer_runner import TrainerRunner

if TYPE_CHECKING:
    from ray.rllib.core.rl_module import RLModule
    from ray.rllib.core.rl_trainer import RLTrainer
    import gymnasium as gym


# TODO: We should make all configs come from a standard base class that defines the 
# general interfaces for validation, from_dict, to_dict etc. 
class TrainerRunnerConfig:
    """Configuration object for TrainerRunner."""

    def __init__(self, cls: Type[TrainerRunner]=None) -> None:

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

        # `self.resources()`
        self.num_gpus = 0
        self.fake_gpu = False


    def validate(self) -> None:
        pass

    def build(self) -> TrainerRunner:
        self.validate()
        # TODO: change the constructor of TrainerRunner to also accept passing in a 
        # TODO: What should be for example scaling_config? it's not clear what
        # should be passed in as trainer_config and what will be inferred
        return self.trainer_runner_class(
            tariner_class=self.trainer_class,
            trainer_config={
                "module_class": self.module_class,
                "module_config": {
                    "observation_space": self.observation_space,
                    "action_space": self.action_space,
                    "model_config": self.model_config,
                },
                # TODO: should this be inferred inside the constructor?
                "distributed": self.config.num_gpus > 1,
                # TODO: add this
                # "enable_tf_function": self.eager_tracing,
            },
            compute_config={
                "num_gpus": self.num_gpus,
                # TODO: add this
                # "fake_gpus": self.fake_gpus,
            },
        )
    
    def module(
        self,
        *,
        module_class: Optional[Type["RLModule"]] = NotProvided,
        observation_space: Optional["gym.Space"] = NotProvided,
        action_space: Optional["gym.Space"] = NotProvided,
        model_config: Optional[dict] = NotProvided,
        module: Optional["RLModule"] = NotProvided
    ) -> "TrainerRunnerConfig":

        if module is NotProvided and module_class is NotProvided:
            raise ValueError("Must provide either module or module_class")
        
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
    ) -> "TrainerRunnerConfig":

        if trainer_class is not NotProvided:
            self.trainer_class = trainer_class
        if eager_tracing is not NotProvided:
            self.eager_tracing = eager_tracing
        
        return self
    
    def resources(
        self,
        num_gpus: Optional[Union[float, int]] = NotProvided,
        fake_gpu: Optional[bool] = NotProvided,
    ) -> "TrainerRunnerConfig":
        
        if num_gpus is not NotProvided:
            self.num_gpus = num_gpus
        if fake_gpu is not NotProvided:
            self.fake_gpu = fake_gpu
        
        return self
    

        