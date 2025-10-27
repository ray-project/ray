from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Hashable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import gymnasium as gym

from ray.rllib.utils.annotations import OldAPIStack

if TYPE_CHECKING:
    # Modules might be missing but supply users with type hints if they are installed.
    import jax.numpy as jnp
    import keras
    import tensorflow as tf
    import torch
    from numpy.typing import NDArray

    from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
    from ray.rllib.core.rl_module.rl_module import RLModuleSpec
    from ray.rllib.env.env_context import EnvContext
    from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
    from ray.rllib.env.single_agent_episode import SingleAgentEpisode
    from ray.rllib.policy.dynamic_tf_policy_v2 import DynamicTFPolicyV2
    from ray.rllib.policy.eager_tf_policy_v2 import EagerTFPolicyV2
    from ray.rllib.policy.policy import PolicySpec
    from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
    from ray.rllib.policy.view_requirement import ViewRequirement


TensorType = Union["NDArray[Any]", "jnp.ndarray", "tf.Tensor", "torch.Tensor"]
"""
Represents a generic tensor type.
This could be an np.ndarray, jnp.ndarray, tf.Tensor, or a torch.Tensor.
"""

TensorStructType = Union[TensorType, dict, tuple]
"""Either a plain tensor, or a dict or tuple of tensors (or StructTensors)."""

# A shape of a tensor.
TensorShape = Union[Tuple[int, ...], List[int]]

NetworkType = Union["torch.nn.Module", "keras.Model"]
"""A neural network."""

DeviceType = Union[str, "torch.device", "int"]
"""
A device identifier, which can be a string (e.g. 'cpu', 'cuda:0'),
a torch.device object, or other types supported by torch.
"""

RLModuleSpecType = Union["RLModuleSpec", "MultiRLModuleSpec"]
"""An RLModule spec (single-agent or multi-agent)."""

StateDict = Dict[str, Any]
"""A state dict of an RLlib component (e.g. EnvRunner, Learner, RLModule)."""

AlgorithmConfigDict = dict  # @OldAPIStack
"""
Represents a fully filled out config of a Algorithm class.

Note:
    Policy config dicts are usually the same as AlgorithmConfigDict, but
    parts of it may sometimes be altered in e.g. a multi-agent setup,
    where we have >1 Policies in the same Algorithm.
"""

PartialAlgorithmConfigDict = dict  # @OldAPIStack
"""
An algorithm config dict that only has overrides. It needs to be combined with
the default algorithm config to be used.
"""

ModelConfigDict = dict  # @OldAPIStack
"""
Represents the model config sub-dict of the algo config that is passed to the
model catalog.
"""

ConvFilterSpec = List[
    Tuple[int, Union[int, Tuple[int, int]], Union[int, Tuple[int, int]]]
]
"""
Conv2D configuration format. Each entry in the outer list represents one Conv2D
layer. Each inner list has the format: [num_output_filters, kernel, stride], where
kernel and stride may be single ints (width and height are the same) or 2-tuples
(int, int) for width and height (different values).
"""

FromConfigSpec = Union[Dict[str, Union[Any, type, str]], type, str]
"""
Objects that can be created through the `from_config()` util method
need a config dict with a "type" key, a class path (str), or a type directly.
"""

EnvConfigDict = dict
"""
Represents the env_config sub-dict of the algo config that is passed to
the env constructor.
"""

EnvID = Union[int, str]
"""
Represents an environment id. These could be:
- An int index for a sub-env within a vectorized env.
- An external env ID (str), which changes(!) each episode.
"""

# TODO (sven): Specify this type more strictly (it should just be gym.Env).
EnvType = Union[Any, gym.Env]
"""
Represents a BaseEnv, MultiAgentEnv, ExternalEnv, ExternalMultiAgentEnv,
VectorEnv, gym.Env, or ActorHandle.
"""

EnvCreator = Callable[["EnvContext"], Optional[EnvType]]
"""
A callable, taking a EnvContext object
(config dict + properties: `worker_index`, `vector_index`, `num_workers`,
and `remote`) and returning an env object (or None if no env is used).
"""

AgentID = Hashable
"""Represents a generic identifier for an agent (e.g., "agent1")."""

PolicyID = str  # @OldAPIStack
"""Represents a generic identifier for a policy (e.g., "pol1")."""

ModuleID = str
"""Represents a generic identifier for a (single-agent) RLModule."""

MultiAgentPolicyConfigDict = Dict[PolicyID, "PolicySpec"]  # @OldAPIStack
"""Type of the config.policies dict for multi-agent training."""

EpisodeType = Union["SingleAgentEpisode", "MultiAgentEpisode"]
"""A new stack Episode type: Either single-agent or multi-agent."""

# @ OldAPIStack
IsPolicyToTrain = Callable[[PolicyID, Optional["MultiAgentBatch"]], bool]
"""Is Policy to train callable."""

AgentToModuleMappingFn = Callable[[AgentID, EpisodeType], ModuleID]
"""Function describing an agent to module mapping."""

ShouldModuleBeUpdatedFn = Union[
    Sequence[ModuleID],
    Callable[[ModuleID, Optional["MultiAgentBatch"]], bool],
]
"""
ModuleIDs that should be updated
or a callable to return whether a module should be updated.
"""

PolicyState = Dict[str, TensorStructType]  # @OldAPIStack
"""
State dict of a Policy, mapping strings (e.g. "weights")
to some state data (TensorStructType).
"""

TFPolicyV2Type = Type[Union["DynamicTFPolicyV2", "EagerTFPolicyV2"]]  # @OldAPIStack
"""Any tf Policy type (static-graph or eager Policy)."""

EpisodeID = Union[int, str]
"""Represents an episode id (old and new API stack)."""

UnrollID = int  # @OldAPIStack
"""Represents an "unroll" (maybe across different sub-envs in a vector env)."""

MultiAgentDict = Dict[AgentID, Any]
"""A dict keyed by agent ids, e.g. {"agent-1": value}."""

MultiEnvDict = Dict[EnvID, MultiAgentDict]
"""
A dict keyed by env ids that contain further nested dictionaries keyed by agent
ids. e.g., {"env-1": {"agent-1": value}}.
"""

EnvObsType = Any
"""Represents an observation returned from the env. (Any alias)"""

EnvActionType = Any
"""Represents an action passed to the env. (Any alias)"""

EnvInfoDict = dict
"""
Info dictionary returned by calling `reset()` or `step()` on `gymnasium.Env`
instances. Might be an empty dict.
"""

FileType = Any
"""Represents a File object. (Any alias)"""

ViewRequirementsDict = Dict[str, "ViewRequirement"]  # @OldAPIStack
"""
Represents a ViewRequirements dict mapping column names (str) to ViewRequirement
objects.
"""

ResultDict = Dict
"""
Represents the result dict returned by Algorithm.train() and algorithm components,
such as EnvRunners, LearnerGroup, etc.. Also, the MetricsLogger used by all these
components returns this upon its `reduce()` method call, so a ResultDict can further
be accumulated (and reduced again) by downstream components.
"""

LocalOptimizer = Union["torch.optim.Optimizer", "keras.optimizers.Optimizer"]
"""A tf or torch local optimizer object."""

Optimizer = LocalOptimizer
"""A tf or torch optimizer object."""

Param = Union["torch.Tensor", "tf.Variable"]
"""A parameter, either a torch.Tensor or tf.Variable."""

ParamRef = Hashable
"""A reference to a parameter. (Hashable alias)"""

ParamDict = Dict[ParamRef, Param]
"""A dictionary mapping parameter references to parameters."""

ParamList = List[Param]
"""A list of parameters."""

NamedParamDict = Dict[str, Param]
"""A dictionary mapping parameter names to parameters."""

LearningRateOrSchedule = Union[
    float,
    List[List[Union[int, float]]],
    List[Tuple[int, Union[int, float]]],
]
"""
A single learning rate or a learning rate schedule (list of sub-lists, each of
the format: [ts (int), lr_to_reach_by_ts (float)]).
"""

GradInfoDict = dict
"""
Dict of tensors returned by compute gradients on the policy, e.g.,
{"td_error": [...], "learner_stats": {"vf_loss": ..., ...}},
for multi-agent, {"policy1": {"learner_stats": ..., }, "policy2": ...}.
"""

LearnerStatsDict = dict
"""
Dict of learner stats returned by compute gradients on the policy, e.g.,
{"vf_loss": ..., ...}. This will always be nested under the "learner_stats" key(s)
of a GradInfoDict. In the multi-agent case, this will be keyed by policy id.
"""

ModelGradients = Union[List[Tuple[TensorType, TensorType]], List[TensorType]]
"""
List of grads+var tuples (tf) or list of gradient tensors (torch) representing
model gradients and returned by compute_gradients().
"""

ModelWeights = dict
"""Type of dict returned by get_weights() representing model weights."""

ModelInputDict = Dict[str, TensorType]
"""An input dict used for direct ModelV2 calls."""

SampleBatchType = Union["SampleBatch", "MultiAgentBatch", Dict[str, Any]]
"""Some kind of sample batch."""

SpaceStruct = Union[
    gym.spaces.Space, Dict[str, gym.spaces.Space], Tuple[gym.spaces.Space, ...]
]
"""
A (possibly nested) space struct: Either a gym.spaces.Space or a (possibly
nested) dict|tuple of gym.space.Spaces.
"""

StateBatches = List[List[Any]]  # @OldAPIStack
"""
A list of batches of RNN states.
Each item in this list has dimension [B, S] (S=state vector size)
"""

# __sphinx_doc_begin_policy_output_type__
PolicyOutputType = Tuple[TensorStructType, StateBatches, Dict]  # @OldAPIStack
"""Format of data output from policy forward pass."""
# __sphinx_doc_end_policy_output_type__


# __sphinx_doc_begin_agent_connector_data_type__
@OldAPIStack
class AgentConnectorDataType:
    """Data type that is fed into and yielded from agent connectors.

    Args:
        env_id: ID of the environment.
        agent_id: ID to help identify the agent from which the data is received.
        data: A payload (``data``). With RLlib's default sampler, the payload
            is a dictionary of arbitrary data columns (obs, rewards, terminateds,
            truncateds, etc).
    """

    def __init__(self, env_id: str, agent_id: str, data: Any):
        self.env_id = env_id
        self.agent_id = agent_id
        self.data = data


# __sphinx_doc_end_agent_connector_data_type__


# __sphinx_doc_begin_action_connector_output__
@OldAPIStack
class ActionConnectorDataType:
    """Data type that is fed into and yielded from agent connectors.

    Args:
        env_id: ID of the environment.
        agent_id: ID to help identify the agent from which the data is received.
        input_dict: Input data that was passed into the policy.
            Sometimes output must be adapted based on the input, for example
            action masking. So the entire input data structure is provided here.
        output: An object of PolicyOutputType. It is is composed of the
            action output, the internal state output, and additional data fetches.

    """

    def __init__(
        self,
        env_id: str,
        agent_id: str,
        input_dict: TensorStructType,
        output: PolicyOutputType,
    ):
        self.env_id = env_id
        self.agent_id = agent_id
        self.input_dict = input_dict
        self.output = output


# __sphinx_doc_end_action_connector_output__


# __sphinx_doc_begin_agent_connector_output__
@OldAPIStack
class AgentConnectorsOutput:
    """Final output data type of agent connectors.

    Args are populated depending on the AgentConnector settings.
    The branching happens in ViewRequirementAgentConnector.

    Args:
        raw_dict: The raw input dictionary that sampler can use to
            build episodes and training batches.
            This raw dict also gets passed into ActionConnectors in case
            it contains data useful for action adaptation (e.g. action masks).
        sample_batch: The SampleBatch that can be immediately used for
            querying the policy for next action.
    """

    def __init__(
        self, raw_dict: Dict[str, TensorStructType], sample_batch: "SampleBatch"
    ):
        self.raw_dict = raw_dict
        self.sample_batch = sample_batch


# __sphinx_doc_end_agent_connector_output__


# Generic type var.
T = TypeVar("T")
