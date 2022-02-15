import gym
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    TypeVar,
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from ray.rllib.env.env_context import EnvContext
    from ray.rllib.policy.policy import PolicySpec
    from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
    from ray.rllib.policy.view_requirement import ViewRequirement
    from ray.rllib.utils import try_import_tf, try_import_torch

    _, tf, _ = try_import_tf()
    torch, _ = try_import_torch()

# Represents a generic tensor type.
# This could be an np.ndarray, tf.Tensor, or a torch.Tensor.
TensorType = Any

# Either a plain tensor, or a dict or tuple of tensors (or StructTensors).
TensorStructType = Union[TensorType, dict, tuple]

# A shape of a tensor.
TensorShape = Union[Tuple[int], List[int]]

# Represents a fully filled out config of a Trainer class.
# Note: Policy config dicts are usually the same as TrainerConfigDict, but
# parts of it may sometimes be altered in e.g. a multi-agent setup,
# where we have >1 Policies in the same Trainer.
TrainerConfigDict = dict

# A trainer config dict that only has overrides. It needs to be combined with
# the default trainer config to be used.
PartialTrainerConfigDict = dict

# Represents the model config sub-dict of the trainer config that is passed to
# the model catalog.
ModelConfigDict = dict

# Objects that can be created through the `from_config()` util method
# need a config dict with a "type" key, a class path (str), or a type directly.
FromConfigSpec = Union[Dict[str, Any], type, str]

# Represents the env_config sub-dict of the trainer config that is passed to
# the env constructor.
EnvConfigDict = dict

# Represents an environment id. These could be:
# - An int index for a sub-env within a vectorized env.
# - An external env ID (str), which changes(!) each episode.
EnvID = Union[int, str]

# Represents a BaseEnv, MultiAgentEnv, ExternalEnv, ExternalMultiAgentEnv,
# VectorEnv, gym.Env, or ActorHandle.
EnvType = Any

# A callable, taking a EnvContext object
# (config dict + properties: `worker_index`, `vector_index`, `num_workers`,
# and `remote`) and returning an env object (or None if no env is used).
EnvCreator = Callable[["EnvContext"], Optional[EnvType]]

# Represents a generic identifier for an agent (e.g., "agent1").
AgentID = Any

# Represents a generic identifier for a policy (e.g., "pol1").
PolicyID = str

# Type of the config["multiagent"]["policies"] dict for multi-agent training.
MultiAgentPolicyConfigDict = Dict[PolicyID, "PolicySpec"]

# State dict of a Policy, mapping strings (e.g. "weights") to some state
# data (TensorStructType).
PolicyState = Dict[str, TensorStructType]

# Represents an episode id.
EpisodeID = int

# Represents an "unroll" (maybe across different sub-envs in a vector env).
UnrollID = int

# A dict keyed by agent ids, e.g. {"agent-1": value}.
MultiAgentDict = Dict[AgentID, Any]

# A dict keyed by env ids that contain further nested dictionaries keyed by
# agent ids. e.g., {"env-1": {"agent-1": value}}.
MultiEnvDict = Dict[EnvID, MultiAgentDict]

# Represents an observation returned from the env.
EnvObsType = Any

# Represents an action passed to the env.
EnvActionType = Any

# Info dictionary returned by calling step() on gym envs. Commonly empty dict.
EnvInfoDict = dict

# Represents a File object
FileType = Any

# Represents a ViewRequirements dict mapping column names (str) to
# ViewRequirement objects.
ViewRequirementsDict = Dict[str, "ViewRequirement"]

# Represents the result dict returned by Trainer.train().
ResultDict = dict

# A tf or torch local optimizer object.
LocalOptimizer = Union["tf.keras.optimizers.Optimizer", "torch.optim.Optimizer"]

# Dict of tensors returned by compute gradients on the policy, e.g.,
# {"td_error": [...], "learner_stats": {"vf_loss": ..., ...}}, for multi-agent,
# {"policy1": {"learner_stats": ..., }, "policy2": ...}.
GradInfoDict = dict

# Dict of learner stats returned by compute gradients on the policy, e.g.,
# {"vf_loss": ..., ...}. This will always be nested under the "learner_stats"
# key(s) of a GradInfoDict. In the multi-agent case, this will be keyed by
# policy id.
LearnerStatsDict = dict

# List of grads+var tuples (tf) or list of gradient tensors (torch)
# representing model gradients and returned by compute_gradients().
ModelGradients = Union[List[Tuple[TensorType, TensorType]], List[TensorType]]

# Type of dict returned by get_weights() representing model weights.
ModelWeights = dict

# An input dict used for direct ModelV2 calls.
ModelInputDict = Dict[str, TensorType]

# Some kind of sample batch.
SampleBatchType = Union["SampleBatch", "MultiAgentBatch"]

# A (possibly nested) space struct: Either a gym.spaces.Space or a
# (possibly nested) dict|tuple of gym.space.Spaces.
SpaceStruct = Union[gym.spaces.Space, dict, tuple]

# Generic type var.
T = TypeVar("T")
