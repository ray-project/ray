from ray import air, tune
from ray.tune.registry import register_env
from ray.rllib.env.wrappers.pettingzoo_env import PettingZooEnv
from pettingzoo.sisl import waterworld_v4

if __name__ == "__main__":
    register_env("waterworld", lambda _: PettingZooEnv(waterworld_v4.env()))

    tune.Tuner(
        "APEX_DDPG",
        run_config=air.RunConfig(
            stop={"episodes_total": 10},
            checkpoint_config=air.CheckpointConfig(
                checkpoint_frequency=10,
            ),
        ),
        param_space={
            # Enviroment specific.
            "env": "waterworld",
            # General
            "num_gpus": 0,
            "num_workers": 1,
            "num_envs_per_worker": 8,
            "replay_buffer_config": {
                "capacity": int(1e5),
                "prioritized_replay_alpha": 0.5,
            },
            "num_steps_sampled_before_learning_starts": 1000,
            "compress_observations": True,
            "rollout_fragment_length": 20,
            "train_batch_size": 512,
            "gamma": 0.99,
            "n_step": 3,
            "lr": 0.0001,
            "target_network_update_freq": 50000,
            "min_sample_timesteps_per_iteration": 25000,
            # Method specific.
            "multiagent": {
                # We only have one policy (calling it "shared").
                # Class, obs/act-spaces, and config will be derived
                # automatically.
                "policies": {"shared_policy"},
                # Always use "shared" policy.
                "policy_mapping_fn": (
                    lambda agent_id, episode, worker, **kwargs: "shared_policy"
                ),
            },
        },
    ).fit()