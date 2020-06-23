from typing import Any, Dict, Union, Tuple
import gym

# Represents a fully filled out config of a Trainer class.
TrainerConfigDict = dict

# A trainer config dict that only has overrides. It needs to be combined with
# the default trainer config to be used.
PartialTrainerConfigDict = dict

# Represents the env_config sub-dict of the trainer config that is passed to
# the env constructor.
EnvConfigDict = dict

# Represents the model config sub-dict of the trainer config that is passed to
# the model catalog.
ModelConfigDict = dict

# Represents a BaseEnv, MultiAgentEnv, ExternalEnv, ExternalMultiAgentEnv,
# VectorEnv, or gym.Env.
EnvType = Any

# Represents a generic identifier for an agent (e.g., "agent1").
AgentID = Any

# Represents a generic identifier for a policy (e.g., "pol1").
PolicyID = str

# Type of the config["multiagent"]["policies"] dict for multi-agent training.
MultiAgentPolicyConfigDict = Dict[PolicyID, Tuple[type, gym.Space, gym.Space,
                                                  PartialTrainerConfigDict]]

# Represents an environment id.
EnvID = int

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

# Represents the result dict returned by Trainer.train().
ResultDict = dict

# Dict of tensors returned by compute gradients on the policy, e.g.,
# {"td_error": [...], "learner_stats": {"vf_loss": ..., ...}}, for multi-agent,
# {"policy1": {"learner_stats": ..., }, "policy2": ...}.
GradInfoDict = dict

# Dict of learner stats returned by compute gradients on the policy, e.g.,
# {"vf_loss": ..., ...}. This will always be nested under the "learner_stats"
# key(s) of a GradInfoDict. In the multi-agent case, this will be keyed by
# policy id.
LearnerStatsDict = dict

# Type of dict returned by compute_gradients() representing model gradients.
ModelGradients = dict

# Type of dict returned by get_weights() representing model weights.
ModelWeights = dict

# Some kind of sample batch.
SampleBatchType = Union["SampleBatch", "MultiAgentBatch"]

# Represents a generic tensor type.
TensorType = Any

# Either a plain tensor, or a dict or tuple of tensors (or StructTensors).
TensorStructType = Union[TensorType, dict, tuple]
