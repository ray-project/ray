import json
import os
from pathlib import Path

import ray
from ray import air, tune
from ray.tune.registry import get_trainable_cls

from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole


param_space = {
    "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
    "framework": "torch",
    "num_workers": 4,
    "num_envs_per_worker": 1,
    "num_cpus_per_worker": 1,
    "log_level": "INFO",
    "env": StatelessCartPole,
    "gamma": 0.95,
    "batch_mode": "complete_episodes",
    "replay_buffer_config": {
        "type": "MultiAgentReplayBuffer",
        "storage_unit": "sequences",
        "capacity": 100000,
        "replay_burn_in": 4,
    },
    "num_steps_sampled_before_learning_starts": 1000,
    "train_batch_size": 480,
    "target_network_update_freq": 480,
    "tau": 0.3,
    "zero_init_states": False,
    "optimization": {
        "actor_learning_rate": 0.005,
        "critic_learning_rate": 0.005,
        "entropy_learning_rate": 0.0001,
    },
    "model": {
        "max_seq_len": 20,
    },
    "policy_model_config": {
        "use_lstm": True,
        "lstm_cell_size": 64,
        "fcnet_hiddens": [64, 64],
        "lstm_use_prev_action": True,
        "lstm_use_prev_reward": True,
    },
    "q_model_config": {
        "use_lstm": True,
        "lstm_cell_size": 64,
        "fcnet_hiddens": [64, 64],
        "lstm_use_prev_action": True,
        "lstm_use_prev_reward": True,
    },
}

if __name__ == "__main__":
    # INIT
    ray.init(num_cpus=5)

    # TRAIN
    results = tune.Tuner(
        "RNNSAC",
        run_config=air.RunConfig(
            name="RNNSAC_example",
            local_dir=str(Path(__file__).parent / "example_out"),
            verbose=2,
            checkpoint_config=air.CheckpointConfig(
                checkpoint_at_end=True,
                num_to_keep=1,
                checkpoint_score_attribute="episode_reward_mean",
            ),
            stop={
                "episode_reward_mean": 65.0,
                "timesteps_total": 50000,
            },
        ),
        tune_config=tune.TuneConfig(
            metric="episode_reward_mean",
            mode="max",
        ),
        param_space=param_space,
    ).fit()

    # TEST

    checkpoint_config_path = os.path.join(results.get_best_result().path, "params.json")
    with open(checkpoint_config_path, "rb") as f:
        checkpoint_config = json.load(f)

    checkpoint_config["explore"] = False

    best_checkpoint = results.get_best_result().best_checkpoints[0][0]
    print("Loading checkpoint: {}".format(best_checkpoint))

    algo = get_trainable_cls("RNNSAC")(env=StatelessCartPole, config=checkpoint_config)
    algo.restore(best_checkpoint)

    env = algo.env_creator({})
    state = algo.get_policy().get_initial_state()
    prev_action = 0
    prev_reward = 0
    obs, info = env.reset()

    eps = 0
    ep_reward = 0
    while eps < 10:
        action, state, info_algo = algo.compute_single_action(
            obs,
            state=state,
            prev_action=prev_action,
            prev_reward=prev_reward,
            full_fetch=True,
        )
        obs, reward, terminated, truncated, info = env.step(action)
        prev_action = action
        prev_reward = reward
        ep_reward += reward
        try:
            env.render()
        except Exception:
            pass
        if terminated or truncated:
            eps += 1
            print("Episode {}: {}".format(eps, ep_reward))
            ep_reward = 0
            state = algo.get_policy().get_initial_state()
            prev_action = 0
            prev_reward = 0
            obs, info = env.reset()
    ray.shutdown()
