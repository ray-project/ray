class Exploration:
    """
    An Exploration takes the predicted actions or action values from the agent,
    and selects the action to actually apply to the environment using some
    predefined exploration schema.
    """

    def __init__(self,
                 action_space=None,
                 worker_info=None,
                 framework="tf"):
        """
        Args:
            action_space (Optional[gym.spaces.Space]): The action space in
                which to explore.
            worker_info (Optional[dict]): A dict with information on the worker
                that is using this Exploration component. Should contain
                keys: `worker_index` and `num_workers`.
            framework (str): One of "tf" or "torch".
        """
        self.action_space = action_space
        self.worker_info = worker_info or {}
        self.framework = framework

    def get_exploration_action(self,
                               action,
                               model=None,
                               action_dist=None,
                               explore=True):
        """
        Given the Model's output and action distribution, return an exploration
        action.

        Args:
            action (any): The already sampled action (non-exploratory case).
            model (ModelV2): The Model object.
            action_dist: The ActionDistribution class.
            explore (bool): Whether to explore or not (this could be a tf
                placeholder).

        Returns:
            any: The chosen exploration action or a tf-op to fetch the
                exploration action from the graph.
        """
        pass

    def get_loss_exploration_term(self,
                                  model_output,
                                  model=None,
                                  action_dist=None,
                                  action_sample=None):
        """
        Given the Model's output and action distribution, returns an extra loss
        term to be added to the loss.

        Args:
            model_output (any): The Model's output Tensor(s).
            model (ModelV2): The Model object.
            action_dist: The ActionDistribution object resulting from
                `model_output`. TODO: Or the class?
            action_sample (any): An optional action sample.

        Returns:
            any: The extra loss term to add to the loss.
        """
        pass  # TODO(sven): implement for some example Exploration class.

    def get_info(self):
        """
        Returns:
            any: A description of the Exploration (not necessarily its state).
        """
        return None

    def get_state(self):
        """
        Returns:
            List[any]: The current state (or a tf-op thereof).
        """
        return []

    def set_state(self, state):
        """
        Sets the current state of the Exploration to the given value or
        returns a tf op that will do the set.
        
        Args:
            state (List[any]): The new state to set.

        Returns:
            Union[None,tf.op]: If framework=tf, the op that handles the update.
        """
        pass

    def reset_state(self):
        """
        Resets the exploration's state.

        Returns:
            Union[None,tf.op]: If framework=tf, the op that handles the reset.
        """
        pass

    @classmethod
    def merge_states(cls, exploration_objects):
        """
        Returns the merged states of all exploration_objects as a value
        or a tf.Tensor (whose execution will trigger the merge).
        
        Args:
            exploration_objects (List[Exploration]): All Exploration objects,
                whose states have to be merged somehow.

        Returns:
            The merged value or a tf.op to execute.
        """
        pass
