from ray import air, tune
from ray.tune.registry import register_env
from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
from pettingzoo.sisl import waterworld_v3

# TODO (Kourosh): Noticed that the env is broken and throws an error in this test.
# The error is ValueError: Input vector should be 1-D. (Could be pettingzoo version
# issue)
# Based on code from github.com/parametersharingmadrl/parametersharingmadrl

if __name__ == "__main__":
    # RDQN - Rainbow DQN
    # ADQN - Apex DQN
    def env_creator(args):
        return PettingZooEnv(waterworld_v3.env())

    env = env_creator({})
    register_env("waterworld", env_creator)

    tune.Tuner(
        "APEX_DDPG",
        run_config=air.RunConfig(
            stop={"episodes_total": 60000},
            checkpoint_config=air.CheckpointConfig(
                checkpoint_frequency=10,
            ),
        ),
        param_space={
            # Enviroment specific
            "env": "waterworld",
            # General
            "num_gpus": 1,
            "num_workers": 2,
            # Method specific
            "multiagent": {
                "policies": env.get_agent_ids(),
                "policy_mapping_fn": (lambda agent_id, episode, **kwargs: agent_id),
            },
        },
    ).fit()
