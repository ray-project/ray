import argparse

from rllib_alpha_zero.alpha_zero import AlphaZero, AlphaZeroConfig
from rllib_alpha_zero.alpha_zero.custom_torch_models import DenseModel

import ray
from ray import air, tune
from ray.rllib.examples.env.cartpole_sparse_rewards import CartPoleSparseRewards
from ray.rllib.utils.test_utils import check_learning_achieved


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

    config = (
        AlphaZeroConfig()
        .rollouts(
            num_rollout_workers=6,
            rollout_fragment_length=50,
        )
        .framework("torch")
        .environment(CartPoleSparseRewards)
        .training(
            train_batch_size=500,
            sgd_minibatch_size=64,
            lr=1e-4,
            num_sgd_iter=1,
            mcts_config={
                "puct_coefficient": 1.5,
                "num_simulations": 100,
                "temperature": 1.0,
                "dirichlet_epsilon": 0.20,
                "dirichlet_noise": 0.03,
                "argmax_tree_policy": False,
                "add_dirichlet_noise": True,
            },
            ranked_rewards={
                "enable": True,
            },
            model={
                "custom_model": DenseModel,
            },
        )
    )

    stop_reward = 30.0

    tuner = tune.Tuner(
        AlphaZero,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={
                "sampler_results/episode_reward_mean": stop_reward,
                "timesteps_total": 100000,
            },
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()

    if args.run_as_test:
        check_learning_achieved(results, stop_reward)
