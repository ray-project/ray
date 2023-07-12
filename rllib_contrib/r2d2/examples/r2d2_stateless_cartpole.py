import argparse

from rllib_r2d2.r2d2.r2d2 import R2D2, R2D2Config

import ray
from ray import air, tune
from ray.rllib.examples.env.stateless_cartpole import StatelessCartPole
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import register_env


def get_cli_args():
    """Create CLI parser and return parsed arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-as-test", action="store_true", default=False)
    args = parser.parse_args()
    print(f"Running with following CLI args: {args}")
    return args


if __name__ == "__main__":
    args = get_cli_args()
    ray.init()

    register_env("stateless_cartpole", lambda env_cfg: StatelessCartPole(env_cfg))

    config = (
        R2D2Config()
        .environment(env="stateless_cartpole")
        .rollouts(num_rollout_workers=0)
        .training(
            model={
                # Wrap with an LSTM and use a very simple base-model.
                "use_lstm": True,
                "max_seq_len": 20,
                "fcnet_hiddens": [64],
                "lstm_cell_size": 64,
                "fcnet_activation": "linear",
            },
            dueling=False,
            lr=5e-4,
            zero_init_states=True,
            replay_buffer_config={
                "type": "MultiAgentReplayBuffer",
                "storage_unit": "sequences",
                "replay_burn_in": 20,
                "zero_init_states": True,
            },
            num_steps_sampled_before_learning_starts=0,
        )
        .exploration(exploration_config={"epsilon_timesteps": 50000})
    )

    stop_reward = 150

    stop = {
        "sampler_results/episode_reward_mean": stop_reward,
        "timesteps_total": 1000000,
    }

    tuner = tune.Tuner(
        R2D2,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop,
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()
    if args.run_as_test:
        check_learning_achieved(results, stop_reward)

    ray.shutdown()
