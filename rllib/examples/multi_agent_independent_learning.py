from ray import tune
from ray.tune.registry import register_env
from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
from pettingzoo.sisl import waterworld_v3

# Based on code from github.com/parametersharingmadrl/parametersharingmadrl

if __name__ == "__main__":
    # RDQN - Rainbow DQN
    # ADQN - Apex DQN
    def env_creator(args):
        return PettingZooEnv(waterworld_v3.env())

    env = env_creator({})
    register_env("waterworld", env_creator)

    tune.run(
        "APEX_DDPG",
        stop={"episodes_total": 60000},
        checkpoint_freq=10,
        config={
            # Enviroment specific
            "env": "waterworld",
            # General
            "num_gpus": 1,
            "num_workers": 2,
            # Method specific
            "multiagent": {
                "policies": set(env.agents),
                "policy_mapping_fn": (lambda agent_id, episode, **kwargs: agent_id),
            },
        },
    )
