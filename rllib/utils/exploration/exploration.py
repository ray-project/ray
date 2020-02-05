from ray.rllib.utils.framework import check_framework


class Exploration:
    """Implements an env-exploration strategy for Policies.

    An Exploration takes the predicted actions or action values from the agent,
    and selects the action to actually apply to the environment using some
    predefined exploration schema.
    """

    def __init__(self,
                 action_space=None,
                 num_workers=None,
                 worker_index=None,
                 framework="tf"):
        """
        Args:
            action_space (Optional[gym.spaces.Space]): The action space in
                which to explore.
            num_workers (Optional[int]): The overall number of workers used.
            worker_index (Optional[int]): The index of the Worker using this
                Exploration.
            framework (str): One of "tf" or "torch".
        """
        self.action_space = action_space
        self.num_workers = num_workers
        self.worker_index = worker_index
        self.framework = check_framework(framework)

    def get_exploration_action(self,
                               action,
                               model=None,
                               action_dist=None,
                               explore=True,
                               timestep=None):
        """Returns an action for exploration purposes.

        Given the Model's output and action distribution, returns an
        exploration action (as opposed to the original model calculated
        action).

        Args:
            action (any): The already sampled action (non-exploratory case).
            model (ModelV2): The Model object.
            action_dist: The ActionDistribution class.
            explore (bool): Whether to explore or not (this could be a tf
                placeholder).
            timestep (int): The current sampling time step. If None, the
                component should try to use an internal counter, which it
                then increments by 1. If provided, will set the internal
                counter to the given value.

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
        """Returns an extra loss term to be added to a loss.

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
        """Returns a description of the current exploration state.

        This is not necessarily the state itself (and cannot be used in
        set_state!), but rather useful (e.g. debugging) information.

        Returns:
            any: A description of the Exploration (not necessarily its state).
        """
        return None

    def get_state(self):
        """Returns the current exploration state.

        Returns:
            List[any]: The current state (or a tf-op thereof).
        """
        return []

    def set_state(self, state):
        """Sets the current state of the Exploration to the given value.

        Or returns a tf op that will do the set.

        Args:
            state (List[any]): The new state to set.

        Returns:
            Union[None,tf.op]: If framework=tf, the op that handles the update.
        """
        pass

    def reset_state(self):
        """Resets the exploration's state.

        Returns:
            Union[None,tf.op]: If framework=tf, the op that handles the reset.
        """
        pass

    @classmethod
    def merge_states(cls, exploration_objects):
        """Returns the merged states of all exploration_objects as a value.

        Or a tf.Tensor (whose execution will trigger the merge).

        Args:
            exploration_objects (List[Exploration]): All Exploration objects,
                whose states have to be merged somehow.

        Returns:
            The merged value or a tf.op to execute.
        """
        pass
