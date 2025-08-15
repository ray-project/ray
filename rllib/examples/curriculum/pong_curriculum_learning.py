"""Example of using curriculum learning for Atari Pong by implementing a custom callback.

This example:
    - demonstrates how to define a curriculum for an agent playing gymnasium's Atari
    Pong.
    - defines a custom callback that gets called once per iteration and - if the agent
    performs well enough - increases the task difficulty, i.e. the `frameskip` for all
    environments on all EnvRunners (the agent must act now faster).
    - also demonstrates how to provide the callback with varying curriculum parameters
    (like threshold maps, returns at which the curriculum ends, etc.).
    - uses Ray Tune and RLlib to curriculum-learn Atari Pong with a high frameskip.

We use Atari Pong with a framestack of 4 images (i.e. observation dimensions of 64x64x4)
and start with a frameskip of 1. At a return of 15.0 we increase the frameskip to 2, at
a return of 17.0 to 3, at 19.0 to 4, and the task is solved at a frameskip of 21.0.

How to run this script
----------------------
`python [script file name].py`

Use the `--solved-return` flag to define the threshold at which curriculum learning ends.
Note that a PPO agent on Atari Pong will need a long time to learn.

To ensure the agent has not collapsed, but rather made had a bad seed, we only decrease
the frameskip when the agent performed worse than the next lower threshold. The margin by
which the agent has to be worse is defined by the `--demotion-margin` argument and defaults
to 2.0.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


"""

import functools
import gymnasium as gym
from typing import Callable

from ray import tune
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.connectors.env_to_module.frame_stacking import FrameStackingEnvToModule
from ray.rllib.connectors.learner.frame_stacking import FrameStackingLearner
from ray.rllib.core.rl_module.default_model_config import DefaultModelConfig
from ray.rllib.env.wrappers.atari_wrappers import wrap_atari_for_new_api_stack
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, EPISODE_RETURN_MEAN
from ray.rllib.utils.test_utils import add_rllib_example_script_args


parser = add_rllib_example_script_args(
    default_reward=float("inf"),
    default_timesteps=3000000,
    default_iters=100000000000,
)
parser.set_defaults(
    env="ale_py:ALE/Pong-v5",
)
parser.add_argument(
    "--solved-return",
    type=float,
    default=21.0,
    help=("The mean episode return at which we consider the task to be fully solved."),
)
parser.add_argument(
    "--demotion-margin",
    type=float,
    default=2.0,
    help=(
        "The margin below the next lower task threshold, beneath which the agent "
        " is considered to have collapsed, prompting a downgrade of the task."
    ),
)
# Use `parser` to add your own custom command line options to this script
# and (if needed) use their values to set up `config` below.
args = parser.parse_args()

NUM_LEARNERS = args.num_learners or 1
ENV = args.env


class PongEnvTaskCallback(RLlibCallback):
    """Custom callback changing the frameskip in Atari Pong dependent on return."""

    def __init__(
        self,
        task_threshold_map: dict,
        remote_fn: Callable,
        demotion_margin: float = 0.0,
        solved_return: float = float("inf"),
    ):
        self.task_threshold_map = task_threshold_map
        self.remote_fn = remote_fn
        self.demotion_margin = demotion_margin
        self.solved_return = solved_return

    def on_algorithm_init(
        self,
        *,
        algorithm: "Algorithm",
        **kwargs,
    ) -> None:
        # Set the initial task to 1, which corresponds to a frameskip of 1.
        algorithm.metrics.log_value("current_env_task", 1, reduce="sum")

    def on_train_result(
        self,
        *,
        algorithm: Algorithm,
        metrics_logger=None,
        result: dict,
        **kwargs,
    ) -> None:
        # Store the current task inside the metrics logger in our Algorithm.
        current_task = metrics_logger.peek("current_env_task")

        # If episode return is consistently above `task_threshold_map[current_task]`,
        # we switch to a more difficult task (i.e. higher `frameskip`` if possible).
        # If we already mastered the most difficult task, we publish our victory in
        # the result dict.
        result["task_solved"] = 0.0

        # Note, in the first callback executions there may be no completed episode
        # (and therefore no episode return) reported. In this case we will skip the
        # the logic to manage task difficulty.
        if EPISODE_RETURN_MEAN in result[ENV_RUNNER_RESULTS]:
            current_return = result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MEAN]
        else:
            return

        # Get the threshold of the current task from the threshold map.
        threshold = self.task_threshold_map.get(current_task, float("inf"))

        # Check, if curriculum is solved.
        final_task = max(self.task_threshold_map.keys())
        if current_task == final_task and current_return >= self.solved_return:
            # Hardest task was solved -> report this in the results dict.
            result["task_solved"] = 1.0

        # Check promotion (increasing task). Note, we could use here also a promotion_patience
        # that ensures that the return is collected in a stable manner instead of a lucky shot.
        if (
            current_return >= threshold
        ):  # & result[ENV_RUNNER_RESULTS][NUM_EPISODES] > promotion_patience.
            next_task = current_task + 1
            if next_task in self.task_threshold_map:
                print(
                    f"Switching task on all EnvRunners up to #{next_task} (1=easiest, "
                    f"4=hardest), b/c R={current_return} on current task."
                )
                # Increase task.
                algorithm.env_runner_group.foreach_env_runner(
                    func=functools.partial(self.remote_fn, new_task=next_task)
                )
                metrics_logger.log_value("current_env_task", next_task, window=1)

        # Check demotion (decreasing task). The demotion is used to avoid decreasing the task
        # in case of an unlucky episode run. Only if the return is singificantly lower we
        # decrease the task.
        previous_task = current_task - 1
        if previous_task in self.task_threshold_map:
            previous_threshold = self.task_threshold_map[previous_task]
            if current_return < previous_threshold - self.demotion_margin:
                print(
                    f"Switching task on all EnvRunners back to #{previous_task} (1=easiest, "
                    f"4=hardest), b/c R={current_return} on current task."
                )
                # Decrease to previous level.
                algorithm.env_runner_group.foreach_env_runner(
                    func=functools.partial(self.remote_fn, new_task=previous_task)
                )
                metrics_logger.log_value("current_env_task", previous_task, window=1)


# These tags allow extracting portions of this script on Anyscale.
# ws-template-code-start
def _make_env_to_module_connector(env, spaces, device):
    return FrameStackingEnvToModule(num_frames=4)


def _make_learner_connector(input_observation_space, input_action_space):
    return FrameStackingLearner(num_frames=4)


# Create a custom Atari setup (w/o the usual RLlib-hard-coded framestacking in it).
# We would like our frame stacking connector to do this job.
def _env_creator(cfg):
    return wrap_atari_for_new_api_stack(
        gym.make(ENV, **cfg, render_mode="rgb_array"),
        # Perform frame-stacking through ConnectorV2 API.
        framestack=None,
    )


# Simple function sent to an EnvRunner to change the map of all its gym. Envs from
# the current one to a new (tougher) one, in which the frameskip is higher
# and the agent must therefore act faster.
def _remote_fn(env_runner, new_task: int):
    # Override the env_config with the new setting.
    env_runner.config.env_config.update(
        {
            "frameskip": new_task,
        }
    )
    # We recreate the entire env object by changing the env_config on the worker,
    # then calling its `make_env()` method.
    env_runner.make_env()


# Task threshold map keeps track of thresholds for each task. If the threshold has
# been surpassed the task difficulty is increased.
task_threshold_map = {
    # Frameskip: Return.
    1: 15.0,
    2: 17.0,
    3: 19.0,
    4: float("inf"),
}

tune.register_env("env", _env_creator)

config = (
    PPOConfig()
    .environment(
        "env",
        env_config={
            # Make analogous to old v4 + NoFrameskip.
            "frameskip": 1,
            "full_action_space": False,
            "repeat_action_probability": 0.0,
        },
        clip_rewards=True,
    )
    .env_runners(
        env_to_module_connector=_make_env_to_module_connector,
    )
    .training(
        learner_connector=_make_learner_connector,
        train_batch_size_per_learner=4000,
        minibatch_size=128,
        lambda_=0.95,
        kl_coeff=0.5,
        clip_param=0.1,
        vf_clip_param=10.0,
        entropy_coeff=0.01,
        num_epochs=10,
        lr=0.00015 * NUM_LEARNERS,
        grad_clip=100.0,
        grad_clip_by="global_norm",
    )
    .rl_module(
        model_config=DefaultModelConfig(
            conv_filters=[[16, 4, 2], [32, 4, 2], [64, 4, 2], [128, 4, 2]],
            conv_activation="relu",
            head_fcnet_hiddens=[256],
            vf_share_layers=True,
        ),
    )
    .callbacks(
        functools.partial(
            PongEnvTaskCallback,
            task_threshold_map=task_threshold_map,
            remote_fn=_remote_fn,
            # Avoids downgrading the task to early when the agent had an unlucky run.
            demotion_margin=args.demotion_margin,
            # The return at which the task is learned.
            solved_return=args.solved_return,
        )
    )
)

if __name__ == "__main__":
    from ray.rllib.utils.test_utils import run_rllib_example_script_experiment

    run_rllib_example_script_experiment(config, args=args)
