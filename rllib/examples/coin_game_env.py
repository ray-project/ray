##########
# Contribution by the Center on Long-Term Risk:
# https://github.com/longtermrisk/marltoolbox
##########
import argparse
import os

import ray
from ray import tune
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.examples.env.coin_game_non_vectorized_env import CoinGame, AsymCoinGame

parser = argparse.ArgumentParser()
parser.add_argument("--tf", action="store_true")
parser.add_argument("--stop-iters", type=int, default=2000)


def main(debug, stop_iters=2000, tf=False, asymmetric_env=False):
    train_n_replicates = 1 if debug else 1
    seeds = list(range(train_n_replicates))

    ray.init()

    stop = {
        "training_iteration": 2 if debug else stop_iters,
    }

    env_config = {
        "players_ids": ["player_red", "player_blue"],
        "max_steps": 20,
        "grid_size": 3,
        "get_additional_info": True,
    }

    rllib_config = {
        "env": AsymCoinGame if asymmetric_env else CoinGame,
        "env_config": env_config,
        "multiagent": {
            "policies": {
                env_config["players_ids"][0]: (
                    None,
                    AsymCoinGame(env_config).OBSERVATION_SPACE,
                    AsymCoinGame.ACTION_SPACE,
                    {},
                ),
                env_config["players_ids"][1]: (
                    None,
                    AsymCoinGame(env_config).OBSERVATION_SPACE,
                    AsymCoinGame.ACTION_SPACE,
                    {},
                ),
            },
            "policy_mapping_fn": lambda agent_id, **kwargs: agent_id,
        },
        # Size of batches collected from each worker.
        "rollout_fragment_length": 20,
        # Number of timesteps collected for each SGD round.
        # This defines the size of each SGD epoch.
        "train_batch_size": 512,
        "model": {
            "dim": env_config["grid_size"],
            "conv_filters": [
                [16, [3, 3], 1],
                [32, [3, 3], 1],
            ],  # [Channel, [Kernel, Kernel], Stride]]
        },
        "lr": 5e-3,
        "seed": tune.grid_search(seeds),
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": "tf" if tf else "torch",
    }

    tune_analysis = tune.run(
        PPOTrainer,
        config=rllib_config,
        stop=stop,
        checkpoint_freq=0,
        checkpoint_at_end=True,
        name="PPO_AsymCG",
    )
    ray.shutdown()
    return tune_analysis


if __name__ == "__main__":
    args = parser.parse_args()
    debug_mode = True
    use_asymmetric_env = False
    main(debug_mode, args.stop_iters, args.tf, use_asymmetric_env)
