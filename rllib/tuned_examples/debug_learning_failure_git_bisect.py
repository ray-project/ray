"""Example of testing, whether RLlib can still learn with a certain config.

Can be used with git bisect to find the faulty commit responsible for a
learning failure. Produces an error if the given reward is not reached within
the stopping criteria (training iters or timesteps) allowing git bisect to
properly analyze and find the faulty commit.

Run as follows using a simple command line config:
$ python debug_learning_failure_git_bisect.py --config '{...}'
    --env CartPole-v0 --run PPO --stop-reward=180 --stop-iters=100

With a yaml file:
$ python debug_learning_failure_git_bisect.py -f [yaml file] --stop-reward=180
    --stop-iters=100

Within git bisect:
$ git bisect start
$ git bisect bad
$ git bisect good [some previous commit we know was good]
$ git bisect run python debug_learning_failure_git_bisect.py [... options]
"""
import argparse
import json
import yaml

import ray
from ray import tune
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default=None)
parser.add_argument("--torch", action="store_true")
parser.add_argument("--stop-iters", type=int, default=None)
parser.add_argument("--stop-timesteps", type=int, default=None)
parser.add_argument("--stop-reward", type=float, default=None)
parser.add_argument("-f", type=str, default=None)
parser.add_argument("--config", type=str, default=None)
parser.add_argument("--env", type=str, default=None)

if __name__ == "__main__":
    run = None

    args = parser.parse_args()

    # Explicit yaml config file.
    if args.f:
        with open(args.f, "r") as fp:
            experiment_config = yaml.load(fp)
            experiment_config = experiment_config[next(
                iter(experiment_config))]
            config = experiment_config.get("config", {})
            config["env"] = experiment_config.get("env")
            run = experiment_config.pop("run")
    # JSON string on command line.
    else:
        config = json.loads(args.config)
        assert args.env
        config["env"] = args.env

    # Explicit run.
    if args.run:
        run = args.run

    # Explicit --torch framework.
    if args.torch:
        config["framework"] = "torch"
    # Framework not specified in config, try to infer it.
    if "framework" not in config:
        config["framework"] = "torch" if args.torch else "tf"

    ray.init()

    stop = {}
    if args.stop_iters:
        stop["training_iteration"] = args.stop_iters
    if args.stop_timesteps:
        stop["timesteps_total"] = args.stop_timesteps
    if args.stop_reward:
        stop["episode_reward_mean"] = args.stop_reward

    results = tune.run(run, stop=stop, config=config)

    check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
