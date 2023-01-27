import abc
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type, Union, TYPE_CHECKING

from ray.rllib.utils.params import Hyperparams

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.rl_module import RLModule, SingleAgentRLModuleSpec
    from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
    from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
    from ray.rllib.algorithms.algorithm import AlgorithmConfig


HyperparamType = Union["AlgorithmConfig", Hyperparams]


@dataclass
class RLTrainerScalingConfig:
    """Base class for scaling config relevant to RLTrainer."""

    def __post_init__(self):
        self._distributed: bool = False

    @property
    def distributed(self) -> bool:
        return self._distributed

    def set_distributed(self, distributed: bool) -> "RLTrainerScalingConfig":
        """Set the distributed flag.

        _distibuted attribute should not be set directly at the time of constuction,
        the caller should explicitly decide whether the rl_trainer should be
        instiantiated in distributed mode or not.

        Args:
            distributed: If True, the rl trainer will be instantiated in distributed
                mode.
        """
        self._distributed = distributed
        return self


@dataclass
class TorchRLTrainerScalingConfig(RLTrainerScalingConfig):
    """Torch-specific scaling config relevant to TorchRLTrainer."""

    def __post_init__(self):
        super().__post_init__()
        self._use_gpu: bool = False

    @property
    def use_gpu(self) -> bool:
        return self._use_gpu

    def set_use_gpu(self, use_gpu: bool) -> "TorchRLTrainerScalingConfig":
        """Set the use_gpu flag.

        _use_gpu attribute should not be set directly at the time of constuction,
        the caller should explicitly decide whether the torch rl_trainer should be using gpu or not

        Args:
            use_gpu: If True, the rl trainer will be setup to use the gpu.
        """
        self._use_gpu = use_gpu
        return self


@dataclass
class TFRLTrainerScalingConfig(RLTrainerScalingConfig):
    """Place holder for TF-specific scaling config relevant to TFRLTrainer."""

    enable_tf_function: bool = True


@dataclass
class TrainerRunnerScalingConfig:
    """Configuratiom for scaling training actors.

    Attributes:
        local: If True, create a trainer in the current process. This is useful for
            debugging to be able to use breakpoints. If False, the trainers are created
            as Ray actors.
        num_workers: The number of workers to use for training.
        num_cpus_per_worker: The number of CPUs to allocate per worker.
        num_gpus_per_worker: The number of GPUs to allocate per worker.
    """

    local: bool = True
    num_workers: int = 1
    num_cpus_per_worker: int = 1
    num_gpus_per_worker: int = 0


@dataclass
class RLTrainerSpec:
    # The RLTrainer class to use.
    rl_trainer_class: Type["RLTrainer"] = None
    # The underlying (MA)RLModule spec to completely define the module
    module_spec: Union["SingleAgentRLModuleSpec", "MultiAgentRLModuleSpec"] = None
    # Alternatively the RLModule instance can be passed in directly (won't work if
    # RLTrainer is an actor)
    module: Optional["RLModule"] = (None,)
    # The scaling config for properly distributing the RLModule
    scaling_config: "RLTrainerScalingConfig" = None
    # The optimizer setting to apply during training
    optimizer_config: Dict[str, Any] = field(default_factory=dict)
    # The extra config for the loss/additional update specific hyper-parameters
    # for now we assume we can get both algorithm config or a dict that contains the
    # hyper-parameters
    trainer_hyperparameters: HyperparamType = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.trainer_hyperparameters, abc.Mapping):
            self.trainer_hyperparameters = Hyperparams(self.trainer_hyperparameters)

    def get_params_dict(self) -> Dict[str, Any]:
        return {
            "module": self.module,
            "module_spec": self.module_spec,
            "scaling_config": self.scaling_config,
            "optimizer_config": self.optimizer_config,
            "trainer_hyperparameters": self.trainer_hyperparameters,
        }

    def build(self):
        return self.rl_trainer_class(**self.get_params_dict())
