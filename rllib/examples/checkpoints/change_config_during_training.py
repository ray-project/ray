"""Example showing how to continue training an Algorithm with a changed config.

Use the setup shown in this script if you want to continue a prior experiment, but
would also like to change some of the config values you originally used.

This example:
    - runs a single- or multi-agent CartPole experiment (for multi-agent, we use
    different learning rates) thereby checkpointing the state of the Algorithm every n
    iterations. The config used is hereafter called "1st config".
    - stops the experiment due to some episode return being achieved.
    - just for testing purposes, restores the entire algorithm from the latest
    checkpoint and checks, whether the state of the restored algo exactly match the
    state of the previously saved one.
    - then changes the original config used (learning rate and other settings) and
    continues training with the restored algorithm and the changed config until a
    final episode return is reached. The new config is hereafter called "2nd config".


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack --num-agents=[0 or 2]
--stop-reward-first-config=[return at which the algo on 1st config should stop training]
--stop-reward=[the final return to achieve after restoration from the checkpoint with
the 2nd config]
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
│ PPO_CartPole-v1_7b1eb_00000   RUNNING         6             16.265
╰────────────────────────────────────────────────────────────────────────
───────────────────────────────────────────────────────────────────────╮
..._sampled_lifetime     ..._trained_lifetime     ...episodes_lifetime │
───────────────────────────────────────────────────────────────────────┤
               24000                    24000                      340 │
───────────────────────────────────────────────────────────────────────╯
...

The experiment stops at an average episode return of `--stop-reward-first-config`.

After the validation of the last checkpoint, a new experiment is started from
scratch, but with the RLlib callback restoring the Algorithm right after
initialization using the previous checkpoint. This new experiment then runs
until `--stop-reward` is reached.

Trial status: 1 RUNNING
Current time: 2024-06-03 12:05:00. Total running time: 1min 0s
Logical resource usage: 3.0/12 CPUs, 0/0 GPUs
╭────────────────────────────────────────────────────────────────────────
│ Trial name                    status       iter     total time (s)
├────────────────────────────────────────────────────────────────────────
│ PPO_CartPole-v1_7b1eb_00000   RUNNING        23            14.8372
╰────────────────────────────────────────────────────────────────────────
───────────────────────────────────────────────────────────────────────╮
..._sampled_lifetime     ..._trained_lifetime     ...episodes_lifetime │
───────────────────────────────────────────────────────────────────────┤
              109078                   109078                      531 │
───────────────────────────────────────────────────────────────────────╯

And if you are using the `--as-test` option, you should see a finel message:

```
`env_runners/episode_return_mean` of 450.0 reached! ok
```
"""
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    LEARNER_RESULTS,
)
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    check,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import register_env


parser = add_rllib_example_script_args(
    default_reward=450.0, default_timesteps=10000000, default_iters=2000
)
parser.add_argument(
    "--stop-reward-first-config",
    type=float,
    default=150.0,
    help="Mean episode return after which the Algorithm on the first config should "
    "stop training.",
)
# By default, set `args.checkpoint_freq` to 1 and `args.checkpoint_at_end` to True.
parser.set_defaults(
    enable_new_api_stack=True,
    checkpoint_freq=1,
    checkpoint_at_end=True,
)


if __name__ == "__main__":
    args = parser.parse_args()

    register_env(
        "ma_cart", lambda cfg: MultiAgentCartPole({"num_agents": args.num_agents})
    )

    # Simple generic config.
    base_config = (
        PPOConfig()
        .environment("CartPole-v1" if args.num_agents == 0 else "ma_cart")
        .training(lr=0.0001)
        # TODO (sven): Tune throws a weird error inside the "log json" callback
        #  when running with this option. The `perf` key in the result dict contains
        #  binary data (instead of just 2 float values for mem and cpu usage).
        # .experimental(_use_msgpack_checkpoints=True)
    )

    # Setup multi-agent, if required.
    if args.num_agents > 0:
        base_config.multi_agent(
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
    # to be reached.
    metric = f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}"
    stop = {metric: args.stop_reward_first_config}

    tuner_results = run_rllib_example_script_experiment(
        base_config,
        args,
        stop=stop,
        keep_ray_up=True,
    )

    # Perform a very quick test to make sure our algo (upon restoration) did not lose
    # its ability to perform well in the env.
    # - Extract the best checkpoint.
    best_result = tuner_results.get_best_result(metric=metric, mode="max")
    assert (
        best_result.metrics[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        >= args.stop_reward_first_config
    )
    best_checkpoint_path = best_result.checkpoint.path

    # Rebuild the algorithm (just for testing purposes).
    test_algo = base_config.build()
    # Load algo's state from the best checkpoint.
    test_algo.restore_from_path(best_checkpoint_path)
    # Perform some checks on the restored state.
    assert test_algo.training_iteration > 0
    # Evaluate on the restored algorithm.
    test_eval_results = test_algo.evaluate()
    assert (
        test_eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        >= args.stop_reward_first_config
    ), test_eval_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
    # Train one iteration to make sure, the performance does not collapse (e.g. due
    # to the optimizer weights not having been restored properly).
    test_results = test_algo.train()
    assert (
        test_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        >= args.stop_reward_first_config
    ), test_results[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
    # Stop the test algorithm again.
    test_algo.stop()

    # Make sure the algorithm gets restored from a checkpoint right after
    # initialization. Note that this includes all subcomponents of the algorithm,
    # including the optimizer states in the LearnerGroup/Learner actors.
    def on_algorithm_init(algorithm, **kwargs):
        module_p0 = algorithm.get_module("p0")
        weight_before = convert_to_numpy(next(iter(module_p0.parameters())))

        algorithm.restore_from_path(best_checkpoint_path)

        # Make sure weights were restored (changed).
        weight_after = convert_to_numpy(next(iter(module_p0.parameters())))
        check(weight_before, weight_after, false=True)

    # Change the config.
    (
        base_config
        # Make sure the algorithm gets restored upon initialization.
        .callbacks(on_algorithm_init=on_algorithm_init)
        # Change training parameters considerably.
        .training(
            lr=0.0003,
            train_batch_size=5000,
            grad_clip=100.0,
            gamma=0.996,
            num_epochs=6,
            vf_loss_coeff=0.01,
        )
        # Make multi-CPU/GPU.
        .learners(num_learners=2)
        # Use more env runners and more envs per env runner.
        .env_runners(num_env_runners=3, num_envs_per_env_runner=5)
    )

    # Update the stopping criterium to the final target return per episode.
    stop = {metric: args.stop_reward}

    # Run a new experiment with the (RLlib) callback `on_algorithm_init` restoring
    # from the best checkpoint.
    # Note that the new experiment starts again from iteration=0 (unlike when you
    # use `tune.Tuner.restore()` after a crash or interrupted trial).
    tuner_results = run_rllib_example_script_experiment(base_config, args, stop=stop)

    # Assert that we have continued training with a different learning rate.
    assert (
        tuner_results[0].metrics[LEARNER_RESULTS][DEFAULT_MODULE_ID][
            "default_optimizer_learning_rate"
        ]
        == base_config.lr
        == 0.0003
    )
