from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import sys
import numpy as np
import gym

from ray.rllib.utils.annotations import ExperimentalAPI

try:
    from typing import TypeAlias
except ImportError:
    from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from ray.rllib.env.env_context import EnvContext
    from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
    from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
    from ray.rllib.policy.policy import PolicySpec
    from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
    from ray.rllib.policy.view_requirement import ViewRequirement
    from ray.rllib.utils import try_import_tf, try_import_torch

    _, tf, _ = try_import_tf()
    torch, _ = try_import_torch()

# Represents a generic tensor type.
# This could be an np.ndarray, tf.Tensor, or a torch.Tensor.
TensorType: TypeAlias = Union[np.array, "tf.Tensor", "torch.Tensor"]

# Either a plain tensor, or a dict or tuple of tensors (or StructTensors).
TensorStructType: TypeAlias = Union[TensorType, dict, tuple]

# A shape of a tensor.
TensorShape: TypeAlias = Union[Tuple[int], List[int]]

# Represents a fully filled out config of a Algorithm class.
# Note: Policy config dicts are usually the same as AlgorithmConfigDict, but
# parts of it may sometimes be altered in e.g. a multi-agent setup,
# where we have >1 Policies in the same Algorithm.
AlgorithmConfigDict: TypeAlias = dict
TrainerConfigDict: TypeAlias = dict

# An algorithm config dict that only has overrides. It needs to be combined with
# the default algorithm config to be used.
PartialAlgorithmConfigDict: TypeAlias = dict
PartialTrainerConfigDict: TypeAlias = dict

# Represents the model config sub-dict of the algo config that is passed to
# the model catalog.
ModelConfigDict: TypeAlias = dict

# Objects that can be created through the `from_config()` util method
# need a config dict with a "type" key, a class path (str), or a type directly.
FromConfigSpec: TypeAlias = Union[Dict[str, Any], type, str]

# Represents the env_config sub-dict of the algo config that is passed to
# the env constructor.
EnvConfigDict: TypeAlias = dict

# Represents an environment id. These could be:
# - An int index for a sub-env within a vectorized env.
# - An external env ID (str), which changes(!) each episode.
EnvID: TypeAlias = Union[int, str]

# Represents a BaseEnv, MultiAgentEnv, ExternalEnv, ExternalMultiAgentEnv,
# VectorEnv, gym.Env, or ActorHandle.
EnvType: TypeAlias = Any

# A callable, taking a EnvContext object
# (config dict + properties: `worker_index`, `vector_index`, `num_workers`,
# and `remote`) and returning an env object (or None if no env is used).
EnvCreator: TypeAlias = Callable[["EnvContext"], Optional[EnvType]]

# Represents a generic identifier for an agent (e.g., "agent1").
AgentID: TypeAlias = Any

# Represents a generic identifier for a policy (e.g., "pol1").
PolicyID: TypeAlias = str

# Type of the config["multiagent"]["policies"] dict for multi-agent training.
MultiAgentPolicyConfigDict: TypeAlias = Dict[PolicyID, "PolicySpec"]

# State dict of a Policy, mapping strings (e.g. "weights") to some state
# data (TensorStructType).
PolicyState: TypeAlias = Dict[str, TensorStructType]

# Any tf Policy type (static-graph or eager Policy).
TFPolicyV2Type: TypeAlias = Type[Union["DynamicTFPolicyV2", "EagerTFPolicyV2"]]

# Represents an episode id.
EpisodeID: TypeAlias = int

# Represents an "unroll" (maybe across different sub-envs in a vector env).
UnrollID: TypeAlias = int

# A dict keyed by agent ids, e.g. {"agent-1": value}.
MultiAgentDict: TypeAlias = Dict[AgentID, Any]

# A dict keyed by env ids that contain further nested dictionaries keyed by
# agent ids. e.g., {"env-1": {"agent-1": value}}.
MultiEnvDict: TypeAlias = Dict[EnvID, MultiAgentDict]

# Represents an observation returned from the env.
EnvObsType: TypeAlias = Any

# Represents an action passed to the env.
EnvActionType: TypeAlias = Any

# Info dictionary returned by calling step() on gym envs. Commonly empty dict.
EnvInfoDict: TypeAlias = dict

# Represents a File object
FileType: TypeAlias = Any

# Represents a ViewRequirements dict mapping column names (str) to
# ViewRequirement objects.
ViewRequirementsDict: TypeAlias = Dict[str, "ViewRequirement"]

# Represents the result dict returned by Algorithm.train().
ResultDict: TypeAlias = dict

# A tf or torch local optimizer object.
LocalOptimizer: TypeAlias = Union[
    "tf.keras.optimizers.Optimizer", "torch.optim.Optimizer"
]

# Dict of tensors returned by compute gradients on the policy, e.g.,
# {"td_error": [...], "learner_stats": {"vf_loss": ..., ...}}, for multi-agent,
# {"policy1": {"learner_stats": ..., }, "policy2": ...}.
GradInfoDict: TypeAlias = dict

# Dict of learner stats returned by compute gradients on the policy, e.g.,
# {"vf_loss": ..., ...}. This will always be nested under the "learner_stats"
# key(s) of a GradInfoDict. In the multi-agent case, this will be keyed by
# policy id.
LearnerStatsDict: TypeAlias = dict

# List of grads+var tuples (tf) or list of gradient tensors (torch)
# representing model gradients and returned by compute_gradients().
ModelGradients: TypeAlias = Union[List[Tuple[TensorType, TensorType]], List[TensorType]]

# Type of dict returned by get_weights() representing model weights.
ModelWeights: TypeAlias = dict

# An input dict used for direct ModelV2 calls.
ModelInputDict: TypeAlias = Dict[str, TensorType]

# Some kind of sample batch.
SampleBatchType: TypeAlias = Union["SampleBatch", "MultiAgentBatch"]

# A (possibly nested) space struct: Either a gym.spaces.Space or a
# (possibly nested) dict|tuple of gym.space.Spaces.
SpaceStruct: TypeAlias = Union[gym.spaces.Space, dict, tuple]

# A list of batches of RNN states.
# Each item in this list has dimension [B, S] (S=state vector size)
StateBatches: TypeAlias = List[List[Any]]

# Format of data output from policy forward pass.
# __sphinx_doc_begin_policy_output_type__
PolicyOutputType: TypeAlias = Tuple[TensorStructType, StateBatches, Dict]
# __sphinx_doc_end_policy_output_type__


# __sphinx_doc_begin_agent_connector_data_type__
@ExperimentalAPI
class AgentConnectorDataType:
    """Data type that is fed into and yielded from agent connectors.

    Args:
        env_id: ID of the environment.
        agent_id: ID to help identify the agent from which the data is received.
        data: A payload (``data``). With RLlib's default sampler, the payload
            is a dictionary of arbitrary data columns (obs, rewards, dones, etc).
    """

    def __init__(self, env_id: str, agent_id: str, data: Any):
        self.env_id = env_id
        self.agent_id = agent_id
        self.data = data


# __sphinx_doc_end_agent_connector_data_type__


# __sphinx_doc_begin_action_connector_output__
@ExperimentalAPI
class ActionConnectorDataType:
    """Data type that is fed into and yielded from agent connectors.

    Args:
        env_id: ID of the environment.
        agent_id: ID to help identify the agent from which the data is received.
        output: An object of PolicyOutputType. It is is composed of the
            action output, the internal state output, and additional data fetches.

    """

    def __init__(self, env_id: str, agent_id: str, output: PolicyOutputType):
        self.env_id = env_id
        self.agent_id = agent_id
        self.output = output


# __sphinx_doc_end_action_connector_output__


# __sphinx_doc_begin_agent_connector_output__
@ExperimentalAPI
class AgentConnectorsOutput:
    """Final output data type of agent connectors.

    Args are populated depending on the AgentConnector settings.
    The branching happens in ViewRequirementAgentConnector.

    Args:
        for_training: The raw input dictionary that sampler can use to
            build episodes and training batches,
        for_action: The SampleBatch that can be immediately used for
            querying the policy for next action.
    """

    def __init__(
        self, for_training: Dict[str, TensorStructType], for_action: "SampleBatch"
    ):
        self.for_training = for_training
        self.for_action = for_action


# __sphinx_doc_end_agent_connector_output__


# Generic type var.
T = TypeVar("T")
