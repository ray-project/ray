from ray import tune
from ray.tune.registry import register_env
from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
from pettingzoo.sisl import waterworld_v0

# Based on code from github.com/parametersharingmadrl/parametersharingmadrl

if __name__ == "__main__":
    # RDQN - Rainbow DQN
    # ADQN - Apex DQN
    def env_creator(args):
        return PettingZooEnv(waterworld_v0.env())

    env = env_creator({})
    register_env("waterworld", env_creator)

    obs_space = env.observation_space
    act_space = env.action_space

    policies = {"shared_policy": (None, obs_space, act_space, {})}

    # for all methods
    policy_ids = list(policies.keys())

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
            "num_envs_per_worker": 8,
            "learning_starts": 1000,
            "buffer_size": int(1e5),
            "compress_observations": True,
            "rollout_fragment_length": 20,
            "train_batch_size": 512,
            "gamma": .99,
            "n_step": 3,
            "lr": .0001,
            "prioritized_replay_alpha": 0.5,
            "final_prioritized_replay_beta": 1.0,
            "target_network_update_freq": 50000,
            "timesteps_per_iteration": 25000,

            # Method specific
            "multiagent": {
                "policies": policies,
                "policy_mapping_fn": (lambda agent_id: "shared_policy"),
            },
        },
    )
