"""Example of using an env-task curriculum via implementing a custom callback.

This example:
    - demonstrates how to define your own curriculum-capable environments using
    gymnasium's FrozenLake env.
    - defines a custom callback that gets called once per iteration and - if necessary -
    changes the maps used by FrozenLake on all EnvRunners to a new task (by moving the
    goal position further and further away from the starting position).
    - also demonstrates an alternative approach via reloading/recreating an entirely new
    env inside all EnvRunners.
    - uses Tune and RLlib to curriculum-learn the env described above and compares 2
    algorithms, one that does use curriculum learning vs one that does not.

We use a FrozenLake (sparse reward) environment with a map size of 8x8 and a time step
limit of 16 to make it almost impossible for a non-curriculum policy to learn.


How to run this script
----------------------
`python [script file name].py --enable-new-api-stack`

Use the `--no-curriculum` flag to disable curriculum learning and force your policy
to be trained on the hardest task right away. With this option, the algorithm should NOT
succeed.

For debugging, use the following additional command line options
`--no-tune --num-env-runners=0`
which should allow you to set breakpoints anywhere in the RLlib code and
have the execution stop there for inspection and debugging.

For logging to your WandB account, use:
`--wandb-key=[your WandB API key] --wandb-project=[some project name]
--wandb-run-name=[optional: WandB run name (within the defined project)]`


Results to expect
-----------------
In the console output, you can see that only PPO policy that uses a curriculum can
actually learn, whereas the one that is thrown into the toughest task right from the
start never learns anything.

Policy using the curriculum:
+-------------------------------+------------+-----------------+--------+
| Trial name                    | status     | loc             |   iter |
|-------------------------------+------------+-----------------+--------+
| PPO_FrozenLake-v1_93ca4_00000 | TERMINATED | 127.0.0.1:73318 |     41 |
+-------------------------------+------------+-----------------+--------+
+------------------+--------+----------+--------------------+
|   total time (s) |     ts |   reward |   episode_len_mean |
|------------------+--------+----------+--------------------|
|           97.652 | 164000 |        1 |            14.0348 |
+------------------+--------+----------+--------------------+

Policy NOT using the curriculum (trying to solve the hardest task right away):


"""
from functools import partial

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.connectors.env_to_module import (
    AddObservationsFromEpisodesToBatch,
    FlattenObservations,
    WriteObservationsToEpisodes,
)
from ray.rllib.utils.test_utils import (
    add_rllib_example_script_args,
    run_rllib_example_script_experiment,
)
from ray.tune.registry import get_trainable_cls

parser = add_rllib_example_script_args(default_iters=100, default_timesteps=600000)
parser.add_argument(
    "--recreate-entire-gym-env",
    action="store_true",
    help="Whether to recreate the entire gym.vector.Env on each EnvRunner whenever a "
    "new task/map should be loaded.",
)
parser.add_argument(
    "--upgrade-task-threshold",
    type=float,
    default=0.99,
    help="The mean episode return, upon reaching of which we increase the task by one.",
)
parser.add_argument(
    "--no-curriculum",
    action="store_true",
    help="Whether to NOT use curriculum learning (and instead trying to solve the "
    "hardest task right away).",
)


ENV_OPTIONS = {
    "is_slippery": False,
    # Limit the number of steps the agent is allowed to make in the env to
    # make it almost impossible to learn without the curriculum.
    "max_episode_steps": 16,
}

# Our 3 tasks: 0=easiest, 1=medium, 2=hard
ENV_MAPS = [
    # 0
    [
        "SFFHFFFH",
        "FFFHFFFF",
        "FFGFFFFF",
        "FFFFFFFF",
        "HFFFFFFF",
        "HHFFFFHF",
        "FFFFFHHF",
        "FHFFFFFF",
    ],
    # 1
    [
        "SFFHFFFH",
        "FFFHFFFF",
        "FFFFFFFF",
        "FFFFFFFF",
        "HFFFFFFF",
        "HHFFGFHF",
        "FFFFFHHF",
        "FHFFFFFF",
    ],
    # 2
    [
        "SFFHFFFH",
        "FFFHFFFF",
        "FFFFFFFF",
        "FFFFFFFF",
        "HFFFFFFF",
        "HHFFFFHF",
        "FFFFFHHF",
        "FHFFFFFG",
    ],
]


# Simple function sent to an EnvRunner to change the map of all its gym.Envs from
# the current one to a new (tougher) one, in which the goal position is further away
# from the starting position. Note that a map is a list of strings, each one
# representing one row in the map. Each character in the strings represent a single
# field (S=starting position, H=hole (bad), F=frozen/free field (ok), G=goal (great!)).
def _remote_fn(env_runner, new_task: int):
    # We recreate the entire env object and re-assign it to the EnvRunner.
    if True:  # args.recreate_entire_gym_env:
        # A SingleAgentEnvRunner holds a gym.vector.Env under `self.env`.
        env_runner.config.environment(env_config={"desc": ENV_MAPS[new_task]})
        env_runner._make_env()
        # Set the EnvRunners `_needs_initial_reset` property (to make sure we reset the
        # next time we call `sample()` on the EnvRunner.
        env_runner._needs_initial_reset = True
    # We change each vectorized Env's map to the provided one.
    else:
        raise NotImplementedError


class CallbackThatSetsNewEnvTask(DefaultCallbacks):
    """Custom callback implementing `on_train_result()` for changing the envs' maps."""

    def on_train_result(
        self,
        *,
        algorithm: Algorithm,
        result: dict,
        **kwargs,
    ) -> None:
        # Hack: Store the current task inside a counter in our Algorithm.
        # W/o a curriculum, the task is always 2 (hardest).
        if args.no_curriculum:
            algorithm._counters["current_env_task"] = 2
        current_task = algorithm._counters["current_env_task"]

        # If episode return is consistently 1.0, we switch to a more difficult task
        # (if possible). If we already mastered the most difficult task, we publish
        # our victory in the result dict.
        result["task_solved"] = 0.0
        current_return = result["sampler_results"]["episode_reward_mean"]
        if current_return > args.upgrade_task_threshold:
            if current_task < 2:
                new_task = current_task + 1
                print(
                    f"Switching task/map on all EnvRunners to #{new_task} (0=easiest, "
                    f"2=hardest), b/c R={current_return} on current task."
                )
                algorithm.workers.foreach_worker(
                    func=partial(_remote_fn, new_task=new_task)
                )
                algorithm._counters["current_env_task"] = new_task

            # Hardest task was solved (1.0) -> report this in the results dict.
            elif current_return == 1.0:
                result["task_solved"] = 1.0


if __name__ == "__main__":
    args = parser.parse_args()

    base_config = (
        get_trainable_cls(args.algo)
        .get_default_config()
        # Plug in our curriculum callbacks that controls when we should upgrade the env
        # task based on the received return for the current task.
        .callbacks(CallbackThatSetsNewEnvTask)
        .environment(
            "FrozenLake-v1",
            env_config={
                # w/ curriculum: start with task=0 (easiest)
                # w/o curriculum: start directly with hardest task 2.
                "desc": ENV_MAPS[2 if args.no_curriculum else 0],
                **ENV_OPTIONS,
            },
        )
        .rollouts(
            num_envs_per_worker=5,
            env_to_module_connector=lambda env: [
                AddObservationsFromEpisodesToBatch(),
                FlattenObservations(),
                WriteObservationsToEpisodes(),
            ],
        )
    )

    stop = {
        "training_iteration": args.stop_iters,
        # Reward directly does not matter to us as we would like to continue
        # after the 0-task (easiest) reaches a return of 1.0. But we DO want to
        # stop, once the entire task is done (reaches return of 1.0 in the most
        # difficult task=2).
        "task_solved": 1.0,
        "timesteps_total": args.stop_timesteps,
    }

    run_rllib_example_script_experiment(
        base_config, args, stop=stop, success_metric={"task_solved": 1.0}
    )
