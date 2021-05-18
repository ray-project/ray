good commit (from Oct16th):
6233cef22cc3c62034b4a40923d1eaebdfef883f


"""
This script should be used to find learning or performance regressions in RLlib.

If you think something broke after(!) some good commit C, do the following
while checked out in the current bad commit D (where D is newer than C):

$ cd ray
$ git bisect start
$ git bisect bad
$ git bisect good [the hash code of C]
$ git bisect run python debug_learning_failure_git_bisect.py [... options]

Can be used with git bisect to find the faulty commit responsible for a
learning failure. Produces an error if the given reward is not reached within
the stopping criteria (training iters or timesteps) allowing git bisect to
properly analyze and find the faulty commit.

Run as follows using a simple command line config
(must run 1M timesteps in 10min):
$ python debug_learning_failure_git_bisect.py --config '{...}'
    --env CartPole-v0 --run PPO --stop-time=600 --stop-timesteps=1000000

With a yaml file (must reach 180 reward in 100 training iterations):
$ python debug_learning_failure_git_bisect.py -f [yaml file] --stop-reward=180
    --stop-iters=100
"""
import argparse
import json
import os
import subprocess
import yaml

import ray
from ray import tune
from ray.rllib.utils.test_utils import check_learning_achieved

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default=None)
parser.add_argument("--torch", action="store_true")
parser.add_argument("--skip-install-ray", action="store_true")
parser.add_argument("--stop-iters", type=int, default=None)
parser.add_argument("--stop-timesteps", type=int, default=None)
parser.add_argument("--stop-time", type=int, default=None)
parser.add_argument("--stop-reward", type=float, default=None)
parser.add_argument("-f", type=str, default=None)
parser.add_argument("--config", type=str, default=None)
parser.add_argument("--env", type=str, default=None)

if __name__ == "__main__":

    args = parser.parse_args()

    try:
        subprocess.run(["ray", "stop"])
    except Exception:
        pass

    # Install ray from the checked out repo.
    if not args.skip_install_ray:
        # Assume we are in the ray (git clone) directory.
        subprocess.run(["ci/travis/install-bazel.sh"])
        os.chdir("python")
        subprocess.run(["pip", "install", "-e", "."])

    subprocess.run(["ray", "start", "--head", "--include-dashboard", "false"])

    run = None

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

    # Criterium is to have reached some min reward.
    if args.stop_reward:
        check_learning_achieved(results, args.stop_reward)
    # Criterium is to have run through n env timesteps in some wall time m.
    elif args.stop_timesteps and args.stop_time:
        last_result = results.trials[0].last_result
        total_timesteps = last_result["timesteps_total"]
        # We stopped because we reached the time limit ->
        # Means throughput is too slow (time steps not reached).
        if args.stop_timesteps < total_timesteps - 100:
            raise ValueError("`stop-timesteps` of {} not reached in {}sec!".format(args.stop_timesteps, args.stop_time))
        print("ok")
    else:
        raise ValueError("Invalid pass criterium!")

    ray.shutdown()
