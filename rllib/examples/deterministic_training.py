"""
Example of a fully deterministic, repeatable RLlib train run using
the "seed" config key.
"""
import argparse

import ray
from ray import tune
from ray.rllib.examples.env.env_using_remote_actor import (
    CartPoleWithRemoteParamServer,
    ParameterStorage,
)
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import check

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--framework", choices=["tf2", "tf", "tfe", "torch"], default="tf")
parser.add_argument("--seed", type=int, default=42)
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=2)
parser.add_argument("--num-gpus", type=float, default=0)
parser.add_argument("--num-gpus-per-worker", type=float, default=0)

if __name__ == "__main__":
    args = parser.parse_args()

    param_storage = ParameterStorage.options(name="param-server").remote()

    config = {
        "env": CartPoleWithRemoteParamServer,
        "env_config": {
            "param_server": "param-server",
        },
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": args.num_gpus,
        "num_workers": 1,  # parallelism
        "num_gpus_per_worker": args.num_gpus_per_worker,
        "num_envs_per_worker": 2,
        "framework": args.framework,
        # Make sure every environment gets a fixed seed.
        "seed": args.seed,
        # Simplify to run this example script faster.
        "train_batch_size": 100,
        "sgd_minibatch_size": 10,
        "num_sgd_iter": 5,
        "rollout_fragment_length": 50,
    }

    stop = {
        "training_iteration": args.stop_iters,
    }

    results1 = tune.run(args.run, config=config, stop=stop, verbose=1)
    results2 = tune.run(args.run, config=config, stop=stop, verbose=1)

    if args.as_test:
        results1 = list(results1.results.values())[0]
        results2 = list(results2.results.values())[0]
        # Test rollout behavior.
        check(results1["hist_stats"], results2["hist_stats"])
        # As well as training behavior (minibatch sequence during SGD
        # iterations).
        check(
            results1["info"][LEARNER_INFO][DEFAULT_POLICY_ID]["learner_stats"],
            results2["info"][LEARNER_INFO][DEFAULT_POLICY_ID]["learner_stats"],
        )
    ray.shutdown()
