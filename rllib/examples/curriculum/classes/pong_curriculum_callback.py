from functools import partial
from typing import Callable

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.callbacks.callbacks import RLlibCallback
from ray.rllib.utils.metrics import ENV_RUNNER_RESULTS, EPISODE_RETURN_MEAN


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

    def on_train_result(
        self,
        *,
        algorithm: Algorithm,
        metrics_logger=None,
        result: dict,
        **kwargs,
    ) -> None:
        # Hack: Store the current task inside a counter in our Algorithm.
        # W/o a curriculum, the task is always 2 (hardest).
        current_task = algorithm._counters["current_env_task"]

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
                    func=partial(self.remote_fn, new_task=next_task)
                )
                algorithm._counters["current_env_task"] = next_task

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
                    func=partial(self.remote_fn, new_task=previous_task)
                )
                algorithm._counters["current_env_task"] = previous_task
