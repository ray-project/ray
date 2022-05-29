from ray import tune
from ray.tune.registry import register_env
from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
from pettingzoo.sisl import waterworld_v3

# Based on code from github.com/parametersharingmadrl/parametersharingmadrl

if __name__ == "__main__":
    # RDQN - Rainbow DQN
    # ADQN - Apex DQN

    register_env("waterworld", lambda _: PettingZooEnv(waterworld_v3.env()))

    tune.run(
        "APEX_DDPG",
        stop={"episodes_total": 60000},
        checkpoint_freq=10,
        config={
            # Enviroment specific.
            "env": "waterworld",
            # General
            "num_gpus": 1,
            "num_workers": 2,
            "num_envs_per_worker": 8,
            "replay_buffer_config": {
                "learning_starts": 1000,
                "capacity": int(1e5),
                "prioritized_replay_alpha": 0.5,
            },
            "compress_observations": True,
            "rollout_fragment_length": 20,
            "train_batch_size": 512,
            "gamma": 0.99,
            "n_step": 3,
            "lr": 0.0001,
            "target_network_update_freq": 50000,
            "min_sample_timesteps_per_reporting": 25000,
            # Method specific.
            "multiagent": {
                # We only have one policy (calling it "shared").
                # Class, obs/act-spaces, and config will be derived
                # automatically.
                "policies": {"shared_policy"},
                # Always use "shared" policy.
                "policy_mapping_fn": (
                    lambda agent_id, episode, **kwargs: "shared_policy"
                ),
            },
        },
    )
