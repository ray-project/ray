import gymnasium as gym

from ray import air, tune
from ray.rllib.algorithms.apex_ddpg import ApexDDPGConfig
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
    gym.register("waterworld", env_creator)

    config = (
        ApexDDPGConfig()
        .environment("waterworld")
        .resources(num_gpus=1)
        .rollouts(num_rollout_workers=2)
        .multi_agent(
            policies=env.get_agent_ids(),
            policy_mapping_fn=(lambda agent_id, *args, **kwargs: agent_id),
        )
    )

    tune.Tuner(
        "APEX_DDPG",
        run_config=air.RunConfig(
            stop={"episodes_total": 60000},
            checkpoint_config=air.CheckpointConfig(
                checkpoint_frequency=10,
            ),
        ),
        param_space=config,
    ).fit()
