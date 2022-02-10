"""
This script can be used to find learning- or performance regressions in RLlib.

If you think something broke after(!) some good commit C, do the following
while checked out in the current bad commit D (where D is newer than C):

$ cd ray
$ git bisect start
$ git bisect bad
$ git bisect good [the hash code of C]
$ git bisect run python debug_learning_failure_git_bisect.py [... options]

Produces an error if the given reward is not reached within
the stopping criteria (training iters or timesteps) OR if some number
of env timesteps are not reached within some wall time or iterations,
and thus allowing git bisect to properly analyze and find the faulty commit.

Run as follows using a simple command line config
(must run 1M timesteps in 2min):
$ python debug_learning_failure_git_bisect.py --config '{...}'
    --env CartPole-v0 --run PPO --stop-time=120 --stop-timesteps=1000000

With a yaml file (must reach 180.0 reward in 100 training iterations):
$ python debug_learning_failure_git_bisect.py -f [yaml file] --stop-reward=180
    --stop-iters=100
"""
import argparse
import importlib
import json
import numpy as np
import os
import subprocess
import yaml

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    type=str,
    default=None,
    help="The RLlib-registered algorithm to use, even if -f (yaml file) given "
    "(will override yaml run setting).")
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default=None,
    help="The DL framework specifier.")
parser.add_argument(
    "--skip-install-ray",
    action="store_true",
    help="If set, do not attempt to re-build ray from source.")
parser.add_argument(
    "--num-samples",
    type=int,
    default=1,
    help="The number of samples to run for the given experiment.")
parser.add_argument(
    "--stop-iters",
    type=int,
    default=None,
    help="Number of iterations to train. Skip if this criterium is not "
    "important.")
parser.add_argument(
    "--stop-timesteps",
    type=int,
    default=None,
    help="Number of env timesteps to train. Can be used in combination with "
    "--stop-time to assertain we reach a certain (env) "
    "timesteps per (wall) time interval. Skip if this "
    "criterium is not important.")
parser.add_argument(
    "--stop-time",
    type=int,
    default=None,
    help="Time in seconds, when to stop the run. Can be used in combination "
    "with --stop-timesteps to assertain we reach a certain (env) "
    "timesteps per (wall) time interval. Skip if this criterium is "
    "not important.")
parser.add_argument(
    "--stop-reward",
    type=float,
    default=None,
    help="The minimum reward that must be reached within the given "
    "time/timesteps/iters. Skip if this criterium is not important.")
parser.add_argument(
    "-f",
    type=str,
    default=None,
    help="The yaml file to use as config. Alternatively, use --run, "
    "--config, and --env.")
parser.add_argument(
    "--config",
    type=str,
    default=None,
    help="If no -f (yaml file) given, use this config instead.")
parser.add_argument(
    "--env",
    type=str,
    default=None,
    help="Sets the env to use, even if -f (yaml file) given "
    "(will override yaml env setting).")

if __name__ == "__main__":

    args = parser.parse_args()

    run = None

    # Explicit yaml config file.
    if args.f:
        with open(args.f, "r") as fp:
            experiment_config = yaml.safe_load(fp)
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

    # Set --framework, if provided.
    if args.framework:
        config["framework"] = args.framework

    # Define stopping criteria. From the yaml file ..
    stop = experiment_config.get("stop", {})
    # .. but override with command line provided ones.
    if args.stop_iters:
        stop["training_iteration"] = args.stop_iters
    if args.stop_timesteps:
        stop["timesteps_total"] = args.stop_timesteps
    if args.stop_reward:
        stop["episode_reward_mean"] = args.stop_reward
    if args.stop_time:
        stop["time_total_s"] = args.stop_time

    # Invalid pass criteria.
    if stop.get("episode_reward_mean") is None and \
            (stop.get("timesteps_total") is None or
             stop.get("time_total_s") is None):
        raise ValueError("Invalid pass criterium! Must use either "
                         "(--stop-reward + optionally any other) OR "
                         "(--stop-timesteps + --stop-time).")

    # - Stop ray.
    # Do this twice to make sure all processes are stopped (older versions of
    # ray used to not kill everything the first time around).
    try:
        subprocess.run("ray stop".split(" "))
        subprocess.run("ray stop".split(" "))
    except Exception:
        pass

    # - Uninstall and re-install ray (from source) if required.
    # Install ray from the checked out repo.
    if not args.skip_install_ray:
        subprocess.run("sudo apt-get update".split(" "))
        subprocess.run("sudo apt-get install -y build-essential curl unzip "
                       "psmisc".split(" "))
        subprocess.run("pip install cython==0.29.26 pytest".split(" "))
        # Assume we are in the ray (git clone) directory.
        try:
            subprocess.run("pip uninstall -y ray".split(" "))
        except Exception:
            pass
        subprocess.run("ci/travis/install-bazel.sh".split(" "))
        os.chdir("python")
        subprocess.run("pip install -e . --verbose".split(" "))
        os.chdir("../")

    # - Start ray.
    try:
        subprocess.run("ray start --head".split(" "))
    except Exception:
        try:
            subprocess.run("ray stop".split(" "))
            subprocess.run("ray stop".split(" "))
        except Exception:
            pass
        try:
            subprocess.run("ray start --head".split(" "))
        except Exception as e:
            print(f"ERROR: {e.args[0]}")

    # Run the training experiment.
    importlib.invalidate_caches()
    import ray
    from ray import tune

    ray.init()

    results = tune.run(run, stop=stop, config=config)
    last_results = [t.last_result for t in results.trials]

    # Criterion is to have reached some min reward within given
    # wall time, iters, or timesteps.
    if stop.get("episode_reward_mean") is not None:
        max_avg_reward = np.max(
            [r["episode_reward_mean"] for r in last_results])
        if max_avg_reward < stop["episode_reward_mean"]:
            raise ValueError("`stop-reward` of {} not reached!".format(
                stop["episode_reward_mean"]))
    # Criterion is to have run through n env timesteps in some wall time m
    # (minimum throughput).
    else:
        total_timesteps = np.sum([r["timesteps_total"] for r in last_results])
        total_time = np.sum([r["time_total_s"] for r in last_results])
        desired_speed = stop["timesteps_total"] / stop["time_total_s"]
        actual_speed = total_timesteps / total_time
        # We stopped because we reached the time limit ->
        # Means throughput is too slow (time steps not reached).
        if actual_speed < desired_speed:
            raise ValueError(
                "`stop-timesteps` of {} not reached in {}sec!".format(
                    stop["timesteps_total"], stop["time_total_s"]))

    print("ok")
    ray.shutdown()
