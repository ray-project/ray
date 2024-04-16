from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
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
    "--evaluation-num-workers",
    type=int,
    default=2,
    help="The number of evaluation workers to setup. "
    "0 for a single local evaluation worker. Note that for values >0, no"
    "local evaluation worker will be created (b/c not needed).",
)
parser.add_argument(
    "--evaluation-interval",
    type=int,
    default=1,
    help="Every how many train iterations should we run an evaluation loop?",
)


class AssertEvalCallback(DefaultCallbacks):
    def on_train_result(self, *, algorithm, result, **kwargs):
        # Make sure we always run exactly the given evaluation duration,
        # no matter what the other settings are (such as
        # `evaluation_num_workers` or `evaluation_parallel_to_training`).
        if (
            "evaluation" in result
            and "hist_stats" in result["evaluation"]["sampler_results"]
        ):
            eval_sampler_res = result["evaluation"]["sampler_results"]
            hist_stats = eval_sampler_res["hist_stats"]
            num_episodes_done = len(hist_stats["episode_lengths"])
            num_timesteps_reported = result["evaluation"]["timesteps_this_iter"]

            # We run for automatic duration (as long as training takes).
            if algorithm.config.evaluation_duration == "auto":
                # If duration=auto: Expect at least as many timesteps as workers
                # (each worker's `sample()` is at least called once).
                # UNLESS: All eval workers were completely busy during the auto-time
                # with older (async) requests and did NOT return anything from the async
                # fetch.
                assert (
                    num_timesteps_reported == 0
                    or num_timesteps_reported >= algorithm.config.evaluation_num_workers
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
        # Expect at least evaluation/sampler_results to be always available.
        elif algorithm.config.always_attach_evaluation_results and (
            "evaluation" not in result or "sampler_results" not in result["evaluation"]
        ):
            raise KeyError(
                "`evaluation->sampler_results->hist_stats` not found in result dict!"
            )


if __name__ == "__main__":
    args = parser.parse_args()

    # Register our environment with tune.
    if args.num_agents > 0:
        register_env(
            "env",
            lambda _: MultiAgentCartPole(config={"num_agents": args.num_agents}),
        )

    config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        .experimental(_enable_new_api_stack=args.enable_new_api_stack)
        .environment("env" if args.num_agents > 0 else "CartPole-v1")
        # Run with tracing enabled for tf2.
        .framework(args.framework)
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
            evaluation_num_workers=args.evaluation_num_workers,
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
            evaluation_config={"explore": False},
        )
        .rollouts(
            num_rollout_workers=args.num_env_runners,
            # Set up the correct env-runner to use depending on
            # old-stack/new-stack and multi-agent settings.
            env_runner_cls=(
                None
                if not args.enable_new_api_stack
                else SingleAgentEnvRunner
                if args.num_agents == 0
                else MultiAgentEnvRunner
            ),
        )
        .resources(
            num_learner_workers=args.num_gpus,
            num_gpus_per_learner_worker=int(args.num_gpus != 0),
            num_cpus_for_local_worker=1,
        )
    )

    # Add a simple multi-agent setup.
    if args.num_agents > 0:
        config.multi_agent(
            policies={f"p{i}" for i in range(args.num_agents)},
            policy_mapping_fn=lambda aid, *a, **kw: f"p{aid}",
        )

    stop = {
        "training_iteration": args.stop_iters,
        "evaluation/sampler_results/episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
    }

    run_rllib_example_script_experiment(config, args, stop=stop)
