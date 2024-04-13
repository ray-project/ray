"""Example of a custom Ray Tune experiment wrapping an RLlib Algorithm.

You should only use such a customized workflow if the following conditions apply:
- You know exactly what you are doing :)
- Simply configuring an existing RLlib Algorithm (e.g. PPO) via its AlgorithmConfig
is not enough and doesn't allow you to shape the Algorithm into behaving the way you'd
like.
-- Note that for complex and custom evaluation procedures there is a RLlib Algorithm
config option (see examples/evaluation/custom_evaluation.py for more details).
- Subclassing

"""

TODO: Continue docstring above

import argparse

import ray
from ray import train, tune
import ray.rllib.algorithms.ppo as ppo

parser = argparse.ArgumentParser()
parser.add_argument("--train-iterations", type=int, default=10)


def experiment(config):
    iterations = config.pop("train-iterations")

    algo = ppo.PPO(config=config)
    checkpoint = None
    train_results = {}

    # Train
    for i in range(iterations):
        train_results = algo.train()
        if i % 2 == 0 or i == iterations - 1:
            checkpoint = algo.save(train.get_context().get_trial_dir())
        train.report(train_results)
    algo.stop()

    # Manual Eval
    config["num_workers"] = 0
    eval_algo = ppo.PPO(config=config)
    eval_algo.restore(checkpoint)
    env = eval_algo.workers.local_worker().env

    obs, info = env.reset()
    done = False
    eval_results = {"eval_reward": 0, "eval_eps_length": 0}
    while not done:
        action = eval_algo.compute_single_action(obs)
        next_obs, reward, done, truncated, info = env.step(action)
        eval_results["eval_reward"] += reward
        eval_results["eval_eps_length"] += 1
    results = {**train_results, **eval_results}
    train.report(results)


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=3)
    base_config = (
    )
    base_config = base_config.to_dict()
    base_config["train-iterations"] = args.train_iterations

    tune.Tuner(
        tune.with_resources(experiment, ppo.PPO.default_resource_request(base_config)),
        param_space=config,
    ).fit()
