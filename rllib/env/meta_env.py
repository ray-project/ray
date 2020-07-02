from gym.core import Env


class MetaEnv(Env):
    """Wrapper around OpenAI gym environments and meta-learning environments.
    
    Your env must implement this interface in order to be used with MAML.
    """

    def sample_tasks(self, n_tasks):
        """Samples task of the meta-environment

        Args:
            n_tasks (int) : number of different meta-tasks needed

        Returns:
            tasks (list) : an (n_tasks) length list of tasks
        """
        raise NotImplementedError

    def set_task(self, task):
        """Sets the specified task to the current environment

        Args:
            task: task of the meta-learning environment
        """
        raise NotImplementedError

    def get_task(self):
        """Gets the task that the agent is performing in the current environment

        Returns:
            task: task of the meta-learning environment
        """
        raise NotImplementedError
