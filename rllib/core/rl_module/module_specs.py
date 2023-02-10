
import abc
from dataclasses import dataclass
import gymnasium as gym
from typing import Mapping, Any, TYPE_CHECKING, Union, Optional, Type, Dict

if TYPE_CHECKING:
    from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
    from ray.rllib.core.rl_module.rl_module import RLModule, ModuleID

from ray.rllib.utils.annotations import (
    ExperimentalAPI,
    OverrideToImplementCustomLogic_CallToSuperRecommended,
)
from ray.rllib.utils.serialization import check_if_args_kwargs_serializable

from ray.rllib.models.specs.typing import SpecType
from ray.rllib.models.specs.checker import check_input_specs, check_output_specs
from ray.rllib.models.distributions import Distribution
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.nested_dict import NestedDict

from ray.rllib.utils.typing import SampleBatchType


@dataclass
class RLModuleSpec(abc.ABC):
    module_class: Optional[Type["RLModule"]] = None

    @abc.abstractmethod
    def build(self) -> "RLModule":
        """Builds the (single or multi-agent) RLModule instance.
        
        Returns:
            The RLModule instance.
        """

@ExperimentalAPI
@dataclass
class SingleAgentRLModuleSpec(RLModuleSpec):
    """A utility spec class to make it constructing RLModules (in single-agent case) easier.

    Args:
        module_class: ...
        observation_space: ...
        action_space: ...
        model_config: ...
    """

    module_class: Optional[Type["RLModule"]] = None
    observation_space: Optional["gym.Space"] = None
    action_space: Optional["gym.Space"] = None
    model_config: Optional[Dict[str, Any]] = None

    def build(self) -> "RLModule":
        return self.module_class.from_model_config(
            observation_space=self.observation_space,
            action_space=self.action_space,
            model_config=self.model_config,
        )


@ExperimentalAPI
@dataclass
class MultiAgentRLModuleSpec(RLModuleSpec):
    """A utility spec class to make it constructing RLModules (in multi-agent case) easier.

    Args:
        module_class: ...
        module_specs: ...
    """

    module_class: Optional[Type["MultiAgentRLModule"]] = None
    module_specs: Optional[Dict["ModuleID", RLModuleSpec]] = None

    def build(self) -> "MultiAgentRLModule":
        return self.module_class.from_multi_agent_config({"modules": self.module_specs})
