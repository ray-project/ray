"""
Example of a fully deterministic, repeatable RLlib train run using
the "seed" config key.
"""
import argparse

import ray
from ray import air, tune
from ray.rllib.examples.env.env_using_remote_actor import (
    CartPoleWithRemoteParamServer,
    ParameterStorage,
)
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import check
from ray.tune.registry import get_trainable_cls

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--framework", choices=["tf2", "tf", "torch"], default="torch")
parser.add_argument("--seed", type=int, default=42)
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=2)
parser.add_argument("--num-gpus", type=float, default=0)
parser.add_argument("--num-gpus-per-worker", type=float, default=0)

if __name__ == "__main__":
    args = parser.parse_args()

    param_storage = ParameterStorage.options(name="param-server").remote()

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .environment(
            CartPoleWithRemoteParamServer,
            env_config={"param_server": "param-server"},
        )
        .framework(args.framework)
        .rollouts(
            num_rollout_workers=1,
            num_envs_per_worker=2,
            rollout_fragment_length=50,
        )
        .resources(
            num_gpus_per_worker=args.num_gpus_per_worker,
            # Old gpu-training API
            num_gpus=args.num_gpus,
            # The new Learner API
            num_learner_workers=int(args.num_gpus),
            num_gpus_per_learner_worker=int(args.num_gpus > 0),
        )
        # Make sure every environment gets a fixed seed.
        .debugging(seed=args.seed)
        .training(
            train_batch_size=100,
        )
    )

    if args.run == "PPO":
        # Simplify to run this example script faster.
        config.training(sgd_minibatch_size=10, num_sgd_iter=5)

    stop = {
        "training_iteration": args.stop_iters,
    }

    results1 = tune.Tuner(
        args.run,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop, verbose=1, failure_config=air.FailureConfig(fail_fast="raise")
        ),
    ).fit()
    results2 = tune.Tuner(
        args.run,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop=stop, verbose=1, failure_config=air.FailureConfig(fail_fast="raise")
        ),
    ).fit()

    if args.as_test:
        results1 = results1.get_best_result().metrics
        results2 = results2.get_best_result().metrics
        # Test rollout behavior.
        check(results1["hist_stats"], results2["hist_stats"])
        # As well as training behavior (minibatch sequence during SGD
        # iterations).
        if config._enable_learner_api:
            check(
                results1["info"][LEARNER_INFO][DEFAULT_POLICY_ID],
                results2["info"][LEARNER_INFO][DEFAULT_POLICY_ID],
            )
        else:
            check(
                results1["info"][LEARNER_INFO][DEFAULT_POLICY_ID]["learner_stats"],
                results2["info"][LEARNER_INFO][DEFAULT_POLICY_ID]["learner_stats"],
            )
    ray.shutdown()
