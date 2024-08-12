"""Example showing how one can set up evaluation running in parallel to training.

Such a setup saves a considerable amount of time during RL Algorithm training, b/c
the next training step does NOT have to wait for the previous evaluation procedure to
finish, but can already start running (in parallel).

See RLlib's documentation for more details on the effect of the different supported
evaluation configuration options:
https://docs.ray.io/en/latest/rllib/rllib-advanced-api.html#customized-evaluation-during-training  # noqa

For an example of how to write a fully customized evaluation function (which normally
is not necessary as the config options are sufficient and offer maximum flexibility),
see this example script here:

https://github.com/ray-project/ray/blob/master/rllib/examples/evaluation/custom_evaluation.py  # noqa


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack`

Use the `--evaluation-num-workers` option to scale up the evaluation workers. Note
that the requested evaluation duration (`--evaluation-duration` measured in
`--evaluation-duration-unit`, which is either "timesteps" (default) or "episodes") is
shared between all configured evaluation workers. For example, if the evaluation
duration is 10 and the unit is "episodes" and you configured 5 workers, then each of the
evaluation workers will run exactly 2 episodes.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
You should see the following output (at the end of the experiment) in your console when
running with a fixed number of 100k training timesteps
(`--enable-new-api-stack --evaluation-duration=auto --stop-timesteps=100000
--stop-reward=100000`):
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_1377a_00000 | TERMINATED | 127.0.0.1:73330 |     25 |
+-----------------------------+------------+-----------------+--------+
+------------------+--------+----------+--------------------+
|   total time (s) |     ts |   reward |   episode_len_mean |
|------------------+--------+----------+--------------------|
|          71.7485 | 100000 |   476.51 |             476.51 |
+------------------+--------+----------+--------------------+

When running without parallel evaluation (`--evaluation-not-parallel-to-training` flag),
the experiment takes considerably longer (~70sec vs ~80sec):
+-----------------------------+------------+-----------------+--------+
| Trial name                  | status     | loc             |   iter |
|-----------------------------+------------+-----------------+--------+
| PPO_CartPole-v1_f1788_00000 | TERMINATED | 127.0.0.1:75135 |     25 |
+-----------------------------+------------+-----------------+--------+
+------------------+--------+----------+--------------------+
|   total time (s) |     ts |   reward |   episode_len_mean |
|------------------+--------+----------+--------------------|
|          81.7371 | 100000 |   494.68 |             494.68 |
+------------------+--------+----------+--------------------+
"""
from typing import Optional

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EVALUATION_RESULTS,
    NUM_EPISODES,
    NUM_ENV_STEPS_SAMPLED,
)
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.rllib.utils.typing import ResultDict
from ray.tune.registry import get_trainable_cls, register_env

parser = add_rllib_example_script_args(default_reward=500.0)
parser.add_argument(
    "--evaluation-duration",
    type=lambda v: v if v == "auto" else int(v),
    default="auto",
    help="Number of evaluation episodes/timesteps to run each iteration. "
    "If 'auto', will run as many as possible during train pass.",
)
parser.add_argument(
    "--evaluation-duration-unit",
    type=str,
    default="timesteps",
    choices=["episodes", "timesteps"],
    help="The unit in which to measure the duration (`episodes` or `timesteps`).",
)
parser.add_argument(
    "--evaluation-not-parallel-to-training",
    action="store_true",
    help="Whether to  NOT run evaluation parallel to training, but in sequence.",
)
parser.add_argument(
    "--evaluation-num-env-runners",
    type=int,
    default=2,
    help="The number of evaluation EnvRunners to setup. "
    "0 for a single local evaluation EnvRunner.",
)
parser.add_argument(
    "--evaluation-interval",
    type=int,
    default=1,
    help="Every how many train iterations should we run an evaluation loop?",
)
parser.add_argument(
    "--evaluation-parallel-to-training-wo-thread",
    action="store_true",
    help="A debugging setting that disables using a threadpool when evaluating in "
    "parallel to training. Use for testing purposes only!",
)


class AssertEvalCallback(DefaultCallbacks):
    def on_train_result(
        self,
        *,
        algorithm: Algorithm,
        metrics_logger: Optional[MetricsLogger] = None,
        result: ResultDict,
        **kwargs,
    ):
        # The eval results can be found inside the main `result` dict
        # (old API stack: "evaluation").
        eval_results = result.get(EVALUATION_RESULTS, result.get("evaluation", {}))
        # In there, there is a sub-key: ENV_RUNNER_RESULTS
        # (old API stack: "sampler_results")
        eval_env_runner_results = eval_results.get(
            ENV_RUNNER_RESULTS, eval_results.get("sampler_results")
        )
        # Make sure we always run exactly the given evaluation duration,
        # no matter what the other settings are (such as
        # `evaluation_num_env_runners` or `evaluation_parallel_to_training`).
        if eval_env_runner_results and NUM_EPISODES in eval_env_runner_results:
            num_episodes_done = eval_env_runner_results[NUM_EPISODES]
            if algorithm.config.enable_env_runner_and_connector_v2:
                num_timesteps_reported = eval_env_runner_results[NUM_ENV_STEPS_SAMPLED]
            else:
                num_timesteps_reported = eval_results["timesteps_this_iter"]

            # We run for automatic duration (as long as training takes).
            if algorithm.config.evaluation_duration == "auto":
                # If duration=auto: Expect at least as many timesteps as workers
                # (each worker's `sample()` is at least called once).
                # UNLESS: All eval workers were completely busy during the auto-time
                # with older (async) requests and did NOT return anything from the async
                # fetch.
                assert (
                    num_timesteps_reported == 0
                    or num_timesteps_reported
                    >= algorithm.config.evaluation_num_env_runners
                )
            # We count in episodes.
            elif algorithm.config.evaluation_duration_unit == "episodes":
                # Compare number of entries in episode_lengths (this is the
                # number of episodes actually run) with desired number of
                # episodes from the config.
                assert num_episodes_done == algorithm.config.evaluation_duration, (
                    num_episodes_done,
                    algorithm.config.evaluation_duration,
                )
                print(
                    "Number of run evaluation episodes: " f"{num_episodes_done} (ok)!"
                )
            # We count in timesteps.
            else:
                num_timesteps_wanted = algorithm.config.evaluation_duration
                delta = num_timesteps_wanted - num_timesteps_reported
                # Expect roughly the same (desired // num-eval-workers).
                assert abs(delta) < 20, (
                    delta,
                    num_timesteps_wanted,
                    num_timesteps_reported,
                )
                print(
                    "Number of run evaluation timesteps: "
                    f"{num_timesteps_reported} (ok)!"
                )
        # Expect at least evaluation_results/env_runner_results to be always available.
        elif algorithm.config.always_attach_evaluation_results and (
            not eval_env_runner_results
        ):
            raise KeyError(
                "`evaluation_results->env_runner_results` not found in result dict!"
            )


if __name__ == "__main__":
    args = parser.parse_args()

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentCartPole(config={"num_agents": args.num_agents}),
        )

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .environment("env" if args.num_agents > 0 else "CartPole-v1")
        # Use a custom callback that asserts that we are running the
        # configured exact number of episodes per evaluation OR - in auto
        # mode - run at least as many episodes as we have eval workers.
        .callbacks(AssertEvalCallback)
        .evaluation(
            # Parallel evaluation+training config.
            # Switch on evaluation in parallel with training.
            evaluation_parallel_to_training=(
                not args.evaluation_not_parallel_to_training
            ),
            # Use two evaluation workers. Must be >0, otherwise,
            # evaluation will run on a local worker and block (no parallelism).
            evaluation_num_env_runners=args.evaluation_num_env_runners,
            # Evaluate every other training iteration (together
            # with every other call to Algorithm.train()).
            evaluation_interval=args.evaluation_interval,
            # Run for n episodes/timesteps (properly distribute load amongst
            # all eval workers). The longer it takes to evaluate, the more sense
            # it makes to use `evaluation_parallel_to_training=True`.
            # Use "auto" to run evaluation for roughly as long as the training
            # step takes.
            evaluation_duration=args.evaluation_duration,
            # "episodes" or "timesteps".
            evaluation_duration_unit=args.evaluation_duration_unit,
            # Switch off exploratory behavior for better (greedy) results.
            evaluation_config={
                "explore": False,
                # TODO (sven): Add support for window=float(inf) and reduce=mean for
                #  evaluation episode_return_mean reductions (identical to old stack
                #  behavior, which does NOT use a window (100 by default) to reduce
                #  eval episode returns.
                "metrics_num_episodes_for_smoothing": 5,
            },
        )
        .debugging(
            _evaluation_parallel_to_training_wo_thread=(
                args.evaluation_parallel_to_training_wo_thread
            ),
        )
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        base_config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    stop = {
        "training_iteration": args.stop_iters,
        "evaluation_results/env_runner_results/episode_return_mean": args.stop_reward,
        "num_env_steps_sampled_lifetime": args.stop_timesteps,
    }

    run_rllib_example_script_experiment(
        base_config,
        args,
        stop=stop,
        success_metric={
            "evaluation_results/env_runner_results/episode_return_mean": (
                args.stop_reward
            ),
        },
    )
