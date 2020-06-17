from typing import Any, Dict

# Represents a fully filled out config of a Trainer class.
TrainerConfigDict = dict

# A trainer config dict that only has overrides. It needs to be combined with
# the default trainer config to be used.
PartialTrainerConfigDict = dict

# Represents the env_config sub-dict of the trainer config that is passed to
# the env constructor.
EnvConfigDict = dict

# Represents a BaseEnv, MultiAgentEnv, ExternalEnv, ExternalMultiAgentEnv,
# VectorEnv, or gym.Env.
EnvType = Any

# Represents a generic identifier for an agent (e.g., "agent1").
AgentID = Any

# Represents a generic identifier for a policy (e.g., "pol1").
PolicyID = str

# Represents an environment id.
EnvID = int

# A dict keyed by agent ids, e.g. {"agent-1": value}.
MultiAgentDict = Dict[AgentID, Any]

# A dict keyed by env ids that contain further nested dictionaries keyed by
# agent ids. e.g., {"env-1": {"agent-1": value}}.
MultiEnvDict = Dict[EnvID, MultiAgentDict]

# Represents an observation returned from the env.
EnvObsType = Any

# Info dictionary returned by calling step() on gym envs. Commonly empty dict.
EnvInfoDict = dict

# Represents the result dict returned by Trainer.train().
ResultDict = dict
