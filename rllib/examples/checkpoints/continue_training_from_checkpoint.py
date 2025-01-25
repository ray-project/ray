"""Example showing how to restore an Algorithm from a checkpoint and resume training.

Use the setup shown in this script if your experiments tend to crash after some time,
and you would therefore like to make your setup more robust and fault-tolerant.

This example:
    - runs a single- or multi-agent CartPole experiment (for multi-agent, we use
    different learning rates) thereby checkpointing the state of the Algorithm every n
    iterations.
    - stops the experiment due to an expected crash in the algorithm's main process
    after a certain number of iterations.
    - just for testing purposes, restores the entire algorithm from the latest
    checkpoint and checks, whether the state of the restored algo exactly match the
    state of the crashed one.
    - then continues training with the restored algorithm until the desired final
    episode return is reached.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-agents=[0 or 2]
--stop-reward-crash=[the episode return after which the algo should crash]
--stop-reward=[the final episode return to achieve after(!) restoration from the
checkpoint]
`

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
First, you should see the initial tune.Tuner do it's thing:

Trial status: 1 RUNNING
Current time: 2024-06-03 12:03:39. Total running time: 30s
Logical resource usage: 3.0/12 CPUs, 0/0 GPUs
╭────────────────────────────────────────────────────────────────────────
│ Trial name                    status       iter     total time (s)
├────────────────────────────────────────────────────────────────────────
│ PPO_CartPole-v1_7b1eb_00000   RUNNING         6             15.362
╰────────────────────────────────────────────────────────────────────────
───────────────────────────────────────────────────────────────────────╮
..._sampled_lifetime     ..._trained_lifetime     ...episodes_lifetime │
───────────────────────────────────────────────────────────────────────┤
               24000                    24000                      340 │
───────────────────────────────────────────────────────────────────────╯
...

then, you should see the experiment crashing as soon as the `--stop-reward-crash`
has been reached:

```RuntimeError: Intended crash after reaching trigger return.```

At some point, the experiment should resume exactly where it left off (using
the checkpoint and restored Tuner):

Trial status: 1 RUNNING
Current time: 2024-06-03 12:05:00. Total running time: 1min 0s
Logical resource usage: 3.0/12 CPUs, 0/0 GPUs
╭────────────────────────────────────────────────────────────────────────
│ Trial name                    status       iter     total time (s)
├────────────────────────────────────────────────────────────────────────
│ PPO_CartPole-v1_7b1eb_00000   RUNNING        27            66.1451
╰────────────────────────────────────────────────────────────────────────
───────────────────────────────────────────────────────────────────────╮
..._sampled_lifetime     ..._trained_lifetime     ...episodes_lifetime │
───────────────────────────────────────────────────────────────────────┤
              108000                   108000                      531 │
───────────────────────────────────────────────────────────────────────╯

And if you are using the `--as-test` option, you should see a finel message:

```
`env_runners/episode_return_mean` of 500.0 reached! ok
```
"""
import re
import time

from ray import tune
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    check_learning_achieved,
)
from ray.tune.registry import get_trainable_cls, register_env
from ray.air.integrations.wandb import WandbLoggerCallback


parser = add_rllib_example_script_args(
    default_reward=500.0, default_timesteps=10000000, default_iters=2000
)
parser.add_argument(
    "--stop-reward-crash",
    type=float,
    default=200.0,
    help="Mean episode return after which the Algorithm should crash.",
)
# By default, set `args.checkpoint_freq` to 1 and `args.checkpoint_at_end` to True.
parser.set_defaults(checkpoint_freq=1, checkpoint_at_end=True)


class CrashAfterNIters(RLlibCallback):
    """Callback that makes the algo crash after a certain avg. return is reached."""

    def __init__(self):
        super().__init__()
        # We have to delay crashing by one iteration just so the checkpoint still
        # gets created by Tune after(!) we have reached the trigger avg. return.
        self._should_crash = False

    def on_train_result(self, *, algorithm, metrics_logger, result, **kwargs):
        # We had already reached the mean-return to crash, the last checkpoint written
        # (the one from the previous iteration) should yield that exact avg. return.
        if self._should_crash:
            raise RuntimeError("Intended crash after reaching trigger return.")
        # Reached crashing criterion, crash on next iteration.
        elif result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN] >= args.stop_reward_crash:
            print(
                "Reached trigger return of "
                f"{result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]}"
            )
            self._should_crash = True


if __name__ == "__main__":
    args = parser.parse_args()

    register_env(
        "ma_cart", lambda cfg: MultiAgentCartPole({"num_agents": args.num_agents})
    )

    # Simple generic config.
    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .api_stack(
            enable_rl_module_and_learner=args.enable_new_api_stack,
            enable_env_runner_and_connector_v2=args.enable_new_api_stack,
        )
        .environment("CartPole-v1" if args.num_agents == 0 else "ma_cart")
        .env_runners(create_env_on_local_worker=True)
        .training(lr=0.0001)
        .callbacks(CrashAfterNIters)
    )

    # Tune config.
    # Need a WandB callback?
    tune_callbacks = []
    if args.wandb_key:
        project = args.wandb_project or (
            args.algo.lower() + "-" + re.sub("\\W+", "-", str(config.env).lower())
        )
        tune_callbacks.append(
            WandbLoggerCallback(
                api_key=args.wandb_key,
                project=args.wandb_project,
                upload_checkpoints=False,
                **({"name": args.wandb_run_name} if args.wandb_run_name else {}),
            )
        )

    # Setup multi-agent, if required.
    if args.num_agents > 0:
        config.multi_agent(
            policies={
                f"p{aid}": PolicySpec(
                    config=AlgorithmConfig.overrides(
                        lr=5e-5
                        * (aid + 1),  # agent 1 has double the learning rate as 0.
                    )
                )
                for aid in range(args.num_agents)
            },
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # Define some stopping criterion. Note that this criterion is an avg episode return
    # to be reached. The stop criterion does not consider the built-in crash we are
    # triggering through our callback.
    stop = {
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
    }

    # Run tune for some iterations and generate checkpoints.
    tuner = tune.Tuner(
        trainable=config.algo_class,
        param_space=config,
        run_config=tune.RunConfig(
            callbacks=tune_callbacks,
            checkpoint_config=tune.CheckpointConfig(
                checkpoint_frequency=args.checkpoint_freq,
                checkpoint_at_end=args.checkpoint_at_end,
            ),
            stop=stop,
        ),
    )
    tuner_results = tuner.fit()

    # Perform a very quick test to make sure our algo (upon restoration) did not lose
    # its ability to perform well in the env.
    # - Extract the best checkpoint.
    metric = f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    best_result = tuner_results.get_best_result(metric=metric, mode="max")
    assert (
        best_result.metrics[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        >= args.stop_reward_crash
    )
    # - Change our config, such that the restored algo will have an env on the local
    # EnvRunner (to perform evaluation) and won't crash anymore (remove the crashing
    # callback).
    config.callbacks(None)
    # Rebuild the algorithm (just for testing purposes).
    test_algo = config.build()
    # Load algo's state from best checkpoint.
    test_algo.restore(best_result.checkpoint)
    # Perform some checks on the restored state.
    assert test_algo.training_iteration > 0
    # Evaluate on the restored algorithm.
    test_eval_results = test_algo.evaluate()
    assert (
        test_eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        >= args.stop_reward_crash
    ), test_eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
    # Train one iteration to make sure, the performance does not collapse (e.g. due
    # to the optimizer weights not having been restored properly).
    test_results = test_algo.train()
    assert (
        test_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN] >= args.stop_reward_crash
    ), test_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
    # Stop the test algorithm again.
    test_algo.stop()

    # Create a new Tuner from the existing experiment path (which contains the tuner's
    # own checkpoint file). Note that even the WandB logging will be continued without
    # creating a new WandB run name.
    restored_tuner = tune.Tuner.restore(
        path=tuner_results.experiment_path,
        trainable=config.algo_class,
        param_space=config,
        # Important to set this to True b/c the previous trial had failed (due to our
        # `CrashAfterNIters` callback).
        resume_errored=True,
    )
    # Continue the experiment exactly where we left off.
    tuner_results = restored_tuner.fit()

    # Not sure, whether this is really necessary, but we have observed the WandB
    # logger sometimes not logging some of the last iterations. This sleep here might
    # give it enough time to do so.
    time.sleep(20)

    if args.as_test:
        check_learning_achieved(tuner_results, args.stop_reward, metric=metric)
