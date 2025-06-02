# @OldAPIStack

"""
Example of a fully deterministic, repeatable RLlib train run using
the "seed" config key.
"""
import argparse

import ray
from ray import tune
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.examples.envs.classes.env_using_remote_actor import (
    CartPoleWithRemoteParamServer,
    ParameterStorage,
)
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import check
from ray.tune.registry import get_trainable_cls
from ray.tune.result import TRAINING_ITERATION

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--framework", choices=["tf2", "tf", "torch"], default="torch")
parser.add_argument("--seed", type=int, default=42)
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=2)
parser.add_argument("--num-gpus", type=float, default=0)
parser.add_argument("--num-gpus-per-env-runner", type=float, default=0)

if __name__ == "__main__":
    args = parser.parse_args()

    param_storage = ParameterStorage.options(name="param-server").remote()

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .api_stack(
            enable_rl_module_and_learner=False,
            enable_env_runner_and_connector_v2=False,
        )
        .environment(
            CartPoleWithRemoteParamServer,
            env_config={"param_server": "param-server"},
        )
        .framework(args.framework)
        .env_runners(
            num_env_runners=1,
            num_envs_per_env_runner=2,
            rollout_fragment_length=50,
            num_gpus_per_env_runner=args.num_gpus_per_env_runner,
        )
        # The new Learner API.
        .learners(
            num_learners=int(args.num_gpus),
            num_gpus_per_learner=int(args.num_gpus > 0),
        )
        # Old gpu-training API.
        .resources(
            num_gpus=args.num_gpus,
        )
        # Make sure every environment gets a fixed seed.
        .debugging(seed=args.seed)
        .training(
            train_batch_size=100,
        )
    )

    if args.run == "PPO":
        # Simplify to run this example script faster.
        config.training(minibatch_size=10, num_epochs=5)

    stop = {TRAINING_ITERATION: args.stop_iters}

    results1 = tune.Tuner(
        args.run,
        param_space=config.to_dict(),
        run_config=tune.RunConfig(
            stop=stop, verbose=1, failure_config=tune.FailureConfig(fail_fast="raise")
        ),
    ).fit()
    results2 = tune.Tuner(
        args.run,
        param_space=config.to_dict(),
        run_config=tune.RunConfig(
            stop=stop, verbose=1, failure_config=tune.FailureConfig(fail_fast="raise")
        ),
    ).fit()

    if args.as_test:
        results1 = results1.get_best_result().metrics
        results2 = results2.get_best_result().metrics
        # Test rollout behavior.
        check(
            results1[ENV_RUNNER_RESULTS]["hist_stats"],
            results2[ENV_RUNNER_RESULTS]["hist_stats"],
        )
        # As well as training behavior (minibatch sequence during SGD
        # iterations).
        if config.enable_rl_module_and_learner:
            check(
                results1["info"][LEARNER_INFO][DEFAULT_MODULE_ID],
                results2["info"][LEARNER_INFO][DEFAULT_MODULE_ID],
            )
        else:
            check(
                results1["info"][LEARNER_INFO][DEFAULT_MODULE_ID]["learner_stats"],
                results2["info"][LEARNER_INFO][DEFAULT_MODULE_ID]["learner_stats"],
            )
    ray.shutdown()
