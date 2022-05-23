import json
import os
from pathlib import Path

import ray
from ray import tune
from ray.rllib.agents.registry import get_trainer_class

# from ray.rllib.examples.env.repeat_after_me_env import RepeatAfterMeEnv
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole


config = {
    "name": "RNNSAC_example",
    "local_dir": str(Path(__file__).parent / "example_out"),
    "checkpoint_at_end": True,
    "keep_checkpoints_num": 1,
    "checkpoint_score_attr": "episode_reward_mean",
    "stop": {
        "episode_reward_mean": 65.0,
        "timesteps_total": 50000,
    },
    "metric": "episode_reward_mean",
    "mode": "max",
    "verbose": 2,
    "config": {
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": "torch",
        "num_workers": 4,
        "num_envs_per_worker": 1,
        "num_cpus_per_worker": 1,
        "log_level": "INFO",
        "env": StatelessCartPole,
        "horizon": 1000,
        "gamma": 0.95,
        "batch_mode": "complete_episodes",
        "replay_buffer_config": {
            "type": "MultiAgentReplayBuffer",
            "storage_unit": "sequences",
            "capacity": 100000,
            "learning_starts": 1000,
            "replay_burn_in": 4,
        },
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
    },
}

if __name__ == "__main__":
    # INIT
    ray.init(num_cpus=5)

    # TRAIN
    results = tune.run("RNNSAC", **config)

    # TEST
    best_checkpoint = results.best_checkpoint
    print("Loading checkpoint: {}".format(best_checkpoint))
    checkpoint_config_path = str(Path(best_checkpoint).parent.parent / "params.json")
    with open(checkpoint_config_path, "rb") as f:
        checkpoint_config = json.load(f)

    checkpoint_config["explore"] = False

    agent = get_trainer_class("RNNSAC")(
        env=config["config"]["env"], config=checkpoint_config
    )
    agent.restore(best_checkpoint)

    env = agent.env_creator({})
    state = agent.get_policy().get_initial_state()
    prev_action = 0
    prev_reward = 0
    obs = env.reset()

    eps = 0
    ep_reward = 0
    while eps < 10:
        action, state, info_trainer = agent.compute_single_action(
            obs,
            state=state,
            prev_action=prev_action,
            prev_reward=prev_reward,
            full_fetch=True,
        )
        obs, reward, done, info = env.step(action)
        prev_action = action
        prev_reward = reward
        ep_reward += reward
        try:
            env.render()
        except Exception:
            pass
        if done:
            eps += 1
            print("Episode {}: {}".format(eps, ep_reward))
            ep_reward = 0
            state = agent.get_policy().get_initial_state()
            prev_action = 0
            prev_reward = 0
            obs = env.reset()
    ray.shutdown()
