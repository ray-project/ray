"""Example showing how to restore an Algorithm from a checkpoint and resume training.

Use the setup shown in this script if your experiments tend to crash after some time
and you would therefore like to make your setup more robust and fault-tolerant.

This example:
- runs a single- or multi-agent CartPole experiment (for multi-agent, we use different
learning rates) thereby checkpointing the state of the Algorithm every n iterations.
- stops the experiment due to an expected crash in the environment after a certain
number of iterations.
- restores the entire algorithm from the latest checkpoint and checks, whether the
state of the restored algo exactly match the state of the crashed one.
- continues training with the restored algorithm until the desired episode return
is reached.


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
"""

from ray import air, tune
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    LEARNER_RESULTS,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    check,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls, register_env


parser = add_rllib_example_script_args(
    default_reward=500.0, default_timesteps=10000000, default_iters=2000
)
parser.add_argument(
    "--stop-reward-crash",
    type=float,
    default=200.0,
    help="Mean episode return after which the Algorithm should crash.",
)


class CrashAfterNIters(DefaultCallbacks):
    """Callback that makes the algo crash after a certain mean return is reached."""
    def __init__(self):
        super().__init__()
        # We have to delay crashing by one iteration just so the checkpoint still
        # gets created by Tune after(!) we have reached the trigger mean-return.
        self._should_crash = False

    def on_train_result(self, *, algorithm, metrics_logger, result, **kwargs):
        # We had already reached the mean-return to crash, the last checkpoint written
        # (the one from the previous iteration) should yield that exact mean return.
        if self._should_crash:
            raise RuntimeError("Intended crash after reaching trigger return.")
        # Reached crashing criterion, crash on next iteration.
        elif (
            result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
            >= args.stop_reward_crash
        ):
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

    # Force-set `args.checkpoint_freq` to 1 and `args.checkpoint_at_end` to True.
    args.checkpoint_freq = 1
    args.checkpoint_at_end = True

    # Simple generic config.
    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("CartPole-v1" if args.num_agents == 0 else "ma_cart")
        .callbacks(CrashAfterNIters)
    )

    # Setup multi-agent, if required.
    if args.num_agents > 0:
        config.multi_agent(
            policies={
                f"p{aid}": PolicySpec(config=AlgorithmConfig.overrides(
                    lr=5e-5 * (aid + 1),  # agent 1 has double the learning rate as 0.
                ))
                for aid in range(args.num_agents)
            },
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    # Run tune for some iterations and generate checkpoints.
    tuner = tune.Tuner(
        trainable=config.algo_class,
        param_space=config,
        run_config=air.RunConfig(
            checkpoint_config=air.CheckpointConfig(
                checkpoint_frequency=args.checkpoint_freq,
                checkpoint_at_end=args.checkpoint_at_end,
            ),
        ),
    )
    # Run the Tuner (until our Algo crashes once it reaches --stop-reward-crash).
    results = tuner.fit()

    # Extract the latest checkpoint from the results and confirm it's the right one.
    metric = f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    best_result = results.get_best_result(metric=metric, mode="max", scope="all")
    assert (
        best_result.metrics[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        >= args.stop_reward_crash
    )

    # Perform a very quick test to make sure our algo (upon restoration) did not lose
    # its ability to perform well in the env.
    # Change our config, such that the restored algo will have an env on the local
    # EnvRunner (to perform evaluation) and won't crash anymore (remove the crashing
    # callback).
    config.env_runners(create_env_on_local_worker=True)
    config.callbacks(None)

    test_algo = config.build()
    # We evaluate once on the randomly initialized algo and expect a bad performance.
    eval_results = test_algo.evaluate()
    assert eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN] <= 30.0
    # Load state from checkpoint.
    test_algo.restore(best_result.checkpoint)
    # Evaluate once more on the restored algorithm.
    eval_results = test_algo.evaluate()
    assert (
        eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        >= args.stop_reward_crash - 10.0  # 10 = allow some buffer
    )
    test_algo.stop()

    # Note that you might want to change a lot of other settings at this point here.
    # Not all settings are changeable between iterations (before a restore), such as
    # NN architecture settings, the algorithm's type, (probably) the environment,
    # your connector types, etc...

    # Here is a non-complete list of setting that should be able to change between
    # experiments (before restoring your algo from a checkpoint):
    config.env_runners(num_env_runners=2)
    config.learners(num_learners=2)
    config.training(
        lr=0.0001,
        train_batch_size=5000,
    )
    #TODO continue


    # Get the best of the 3 trials by using some metric.
    # NOTE: Choosing the min `episodes_this_iter` automatically picks the trial
    # with the best performance (over the entire run (scope="all")):
    # The fewer episodes, the longer each episode lasted, the more reward we
    # got each episode.
    # Setting scope to "last", "last-5-avg", or "last-10-avg" will only compare
    # (using `mode=min|max`) the average values of the last 1, 5, or 10
    # iterations with each other, respectively.
    # Setting scope to "avg" will compare (using `mode`=min|max) the average
    # values over the entire run.
    metric = f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    # notice here `scope` is `all`, meaning for each trial,
    # all results (not just the last one) will be examined.
    best_result = results.get_best_result(metric=metric, mode="max", scope="all")
    value_best_metric = best_result.metrics_dataframe[metric].min()
    best_return_best = best_result.metrics_dataframe[
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    ].max()
    print(
        f"Best trial was the one with lr={best_result.metrics['config']['lr']}. "
        f"Reached lowest episode count ({value_best_metric}) in a single iteration and "
        f"an average return of {best_return_best}."
    )

    # Confirm, we picked the right trial.

    assert (
        value_best_metric
        == results.get_dataframe(filter_metric=metric, filter_mode="min")[metric].min()
    )

    # Get the best checkpoints from the trial, based on different metrics.
    # Checkpoint with the lowest policy loss value:
    if args.enable_new_api_stack:
        policy_loss_key = f"{LEARNER_RESULTS}/{DEFAULT_MODULE_ID}/policy_loss"
    else:
        policy_loss_key = "info/learner/default_policy/learner_stats/policy_loss"
    best_result = results.get_best_result(metric=policy_loss_key, mode="min")
    ckpt = best_result.checkpoint
    lowest_policy_loss = best_result.metrics_dataframe[policy_loss_key].min()
    print(f"Checkpoint w/ lowest policy loss ({lowest_policy_loss}): {ckpt}")

    # Checkpoint with the highest value-function loss:
    if args.enable_new_api_stack:
        vf_loss_key = f"{LEARNER_RESULTS}/{DEFAULT_MODULE_ID}/vf_loss"
    else:
        vf_loss_key = "info/learner/default_policy/learner_stats/vf_loss"
    best_result = results.get_best_result(metric=vf_loss_key, mode="max")
    ckpt = best_result.checkpoint
    highest_value_fn_loss = best_result.metrics_dataframe[vf_loss_key].max()
    print(f"Checkpoint w/ highest value function loss: {ckpt}")
    print(f"Highest value function loss: {highest_value_fn_loss}")
