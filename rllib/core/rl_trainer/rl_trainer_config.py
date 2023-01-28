from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type, Union, TYPE_CHECKING

from ray.rllib.utils.params import Hyperparams
from ray.rllib.core.rl_module.torch import TorchRLModule

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.rl_module import RLModule, SingleAgentRLModuleSpec
    from ray.rllib.core.rl_module.marl_module import MultiAgentRLModuleSpec
    from ray.rllib.core.rl_trainer.rl_trainer import RLTrainer
    from ray.rllib.algorithms.algorithm import AlgorithmConfig


HyperparamType = Union["AlgorithmConfig", Hyperparams]


@dataclass
class RLModuleBackendConfig:
    """Base class for scaling config relevant to RLTrainer.

    Attributes:
        distributed: If True, the rl_trainer will be instantiated in distributed mode.

    Methods:
        set_distributed: Set the distributed flag. _distibuted attribute should not be
            set to True at the time of constructing the config. The caller should
            explicitly decide whether the rl_trainer should be instiantiated in
            distributed mode or not.
    """

    def __post_init__(self):
        super().__post_init__()
        self._distributed: bool = False

    @property
    def distributed(self) -> bool:
        return self._distributed

    def set_distributed(self, distributed: bool) -> "RLModuleBackendConfig":
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
class TorchRLModuleBackendConfig(RLModuleBackendConfig):
    """Torch-specific scaling config relevant to TorchRLTrainer.

    Attributes:
        use_gpu: If True, the torch rl_trainer will be setup to use the gpu.

    Methods:
        set_use_gpu: Set the use_gpu flag. _use_gpu attribute should not be set to True
            at the time of constructing the config. The caller should explicitly decide
            whether the torch rl_trainer should be using gpu or not.
    """

    def __post_init__(self):
        super().__post_init__()
        self._use_gpu: bool = False

    @property
    def use_gpu(self) -> bool:
        return self._use_gpu

    def set_use_gpu(self, use_gpu: bool) -> "TorchRLModuleBackendConfig":
        """Set the use_gpu flag.

        _use_gpu attribute should not be set directly at the time of constuction,
        the caller should explicitly decide whether the torch rl_trainer should be
        using gpu or not

        Args:
            use_gpu: If True, the rl trainer will be setup to use the gpu.
        """
        self._use_gpu = use_gpu
        return self


@dataclass
class TfRLModuleBackendConfig(RLModuleBackendConfig):
    """Tf-specific scaling config relevant to TFRLTrainer.

    Args:
        enable_tf_function: If True, the tf.function decorator will be used to
            decorate the train_step function. This is recommended to boost performance
            via tracing the graph.
    """

    enable_tf_function: bool = True


@dataclass
class TrainerScalingConfig:
    """Configuratiom for scaling training actors.

    Attributes:
        num_workers: The number of workers to use for training. num_workers=0 means you
            have only one local worker (either on 1 CPU or 1 GPU)
        num_cpus_per_worker: The number of CPUs to allocate per worker. If
            num_workers=0 and num_gpus_per_worker=0, regardless of this value, the
            training will run on a single CPU.
        num_gpus_per_worker: The number of GPUs to allocate per worker. If
            num_workers=0, any number greater than 0 will run the training on a single
            GPU. A value of zero will run the training on a single CPU.
    """

    num_workers: int = 0
    num_cpus_per_worker: int = 1
    num_gpus_per_worker: int = 0


@dataclass
class RLTrainerSpec:
    """The spec for construcitng RLTrainer actors.

    Args:
        rl_trainer_class: The RLTrainer class to use.
        module_spec: The underlying (MA)RLModule spec to completely define the module.
        module: Alternatively the RLModule instance can be passed in directly. This
            only works if the RLTrainer is not an actor.
        backend_config: The backend config for properly distributing the RLModule.
        optimizer_config: The optimizer setting to apply during training.
        trainer_hyperparameters: The extra config for the loss/additional update. The
            items within this object should be accessible via a dot notation. For
            example, if the trainer_hyperparameters contains {"coeff": 0.001}, then the
            learning rate can be accessed via trainer_hyperparameters.coeff. This is
            useful for passing in algorithm config or a HyperParams that contains the
            hyper-parameters.
    """

    rl_trainer_class: Type["RLTrainer"]
    module_spec: Union["SingleAgentRLModuleSpec", "MultiAgentRLModuleSpec"] = None
    module: Optional["RLModule"] = None
    module_backend_config: "RLModuleBackendConfig" = None
    optimizer_config: Dict[str, Any] = field(default_factory=dict)
    trainer_hyperparameters: HyperparamType = field(default_factory=dict)

    def __post_init__(self):
        # convert to hyper params object if needed
        if isinstance(self.trainer_hyperparameters, dict):
            self.trainer_hyperparameters = Hyperparams(self.trainer_hyperparameters)

        # if module_backend_config is not set, we will create a dafault.
        if self.module_backend_config is None:
            if self.module is not None:
                if isinstance(self.module, TorchRLModule):
                    self.module_backend_config = TorchRLModuleBackendConfig()
                else:
                    self.module_backend_config = TfRLModuleBackendConfig()

            if self.module_spec is not None:
                if issubclass(self.module_spec.module_class, TorchRLModule):
                    self.module_backend_config = TorchRLModuleBackendConfig()
                else:
                    self.module_backend_config = TfRLModuleBackendConfig()

    def get_params_dict(self) -> Dict[str, Any]:
        """Returns the parameters than be passed to the RLTrainer constructor."""
        return {
            "module": self.module,
            "module_spec": self.module_spec,
            "scaling_config": self.module_backend_config,
            "optimizer_config": self.optimizer_config,
            "trainer_hyperparameters": self.trainer_hyperparameters,
        }

    def build(self) -> "RLTrainer":
        """Builds the RLTrainer instance."""
        return self.rl_trainer_class(**self.get_params_dict())
