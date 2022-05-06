"""Example of a custom experiment wrapped around an RLlib trainer."""
import argparse

import ray
from ray import tune
from ray.rllib.agents import ppo

parser = argparse.ArgumentParser()
parser.add_argument("--train-iterations", type=int, default=10)


def experiment(config):
    iterations = config.pop("train-iterations")
    train_agent = ppo.PPOTrainer(config=config, env="CartPole-v0")
    checkpoint = None
    train_results = {}

    # Train
    for i in range(iterations):
        train_results = train_agent.train()
        if i % 2 == 0 or i == iterations - 1:
            checkpoint = train_agent.save(tune.get_trial_dir())
        tune.report(**train_results)
    train_agent.stop()

    # Manual Eval
    config["num_workers"] = 0
    eval_agent = ppo.PPOTrainer(config=config, env="CartPole-v0")
    eval_agent.restore(checkpoint)
    env = eval_agent.workers.local_worker().env

    obs = env.reset()
    done = False
    eval_results = {"eval_reward": 0, "eval_eps_length": 0}
    while not done:
        action = eval_agent.compute_single_action(obs)
        next_obs, reward, done, info = env.step(action)
        eval_results["eval_reward"] += reward
        eval_results["eval_eps_length"] += 1
    results = {**train_results, **eval_results}
    tune.report(results)


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=3)
    config = ppo.DEFAULT_CONFIG.copy()
    config["train-iterations"] = args.train_iterations

    config["env"] = "CartPole-v0"

    tune.run(
        experiment,
        config=config,
        resources_per_trial=ppo.PPOTrainer.default_resource_request(config),
    )
