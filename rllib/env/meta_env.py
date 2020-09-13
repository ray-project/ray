import gym
from typing import List, Any

TaskType = Any  # Can be different types depending on env, e.g., int or dict


class MetaEnv(gym.Env):
    """
    Extension of gym.Env to define a distribution of tasks to meta-learn over.
    Your env must implement this interface in order to be used with MAML.
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
