from typing import Any

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

# Info dictionary returned by calling step() on gym envs.
EnvInfoDict = dict

# Represents the result dict returned by Trainer.train()
ResultDict = dict
