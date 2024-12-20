# flake8: noqa


# __pass-custom-multirlmodule-shared-enc-begin__
import gymnasium as gym
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec

spec = MultiRLModuleSpec(
    multi_rl_module_class=BCTorchMultiAgentModuleWithSharedEncoder,
    rl_module_specs={
        "local_2d": RLModuleSpec(
            observation_space=gym.spaces.Dict(
                {
                    "global": gym.spaces.Box(low=-1, high=1, shape=(2,)),
                    "local": gym.spaces.Box(low=-1, high=1, shape=(2,)),
                }
            ),
            action_space=gym.spaces.Discrete(2),
            model_config={"fcnet_hiddens": [64]},
        ),
        "local_5d": RLModuleSpec(
            observation_space=gym.spaces.Dict(
                {
                    "global": gym.spaces.Box(low=-1, high=1, shape=(2,)),
                    "local": gym.spaces.Box(low=-1, high=1, shape=(5,)),
                }
            ),
            action_space=gym.spaces.Discrete(5),
            model_config={"fcnet_hiddens": [64]},
        ),
    },
)

module = spec.build()
# __pass-custom-multirlmodule-shared-enc-end__
