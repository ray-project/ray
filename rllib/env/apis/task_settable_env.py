import gym
from typing import List, Any

TaskType = Any  # Can be different types depending on env, e.g., int or dict


class TaskSettableEnv(gym.Env):
    """
    Extension of gym.Env to define a task-settable Env.

    Your env must implement this interface in order to be used with MAML.
    For curriculum learning, you can add this API to your env such that
    the `env_task_fn` can set the next task as needed.

    Supports:
    - Sampling from a distribution of tasks for meta-learning.
    - Setting the env to any task it supports.
    - Getting the current task this env has been set to.

    Examples:
        >>> from ray.rllib.env.apis.task_settable_env import TaskSettableEnv
        >>> env = TaskSettableEnv(...) # doctest: +SKIP
    """

    def sample_tasks(self, n_tasks: int) -> List[TaskType]:
        """Samples task of the meta-environment

        Args:
            n_tasks (int) : number of different meta-tasks needed

        Returns:
            tasks (list) : an (n_tasks) length list of tasks
        """
        raise NotImplementedError

    def set_task(self, task: TaskType) -> None:
        """Sets the specified task to the current environment

        Args:
            task: task of the meta-learning environment
        """
        raise NotImplementedError

    def get_task(self) -> TaskType:
        """Gets the task that the agent is performing in the current environment

        Returns:
            task: task of the meta-learning environment
        """
        raise NotImplementedError
