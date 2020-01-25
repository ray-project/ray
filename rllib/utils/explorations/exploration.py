from abc import ABCMeta, abstractmethod

from ray.rllib.utils.framework import check_framework


class Exploration(metaclass=ABCMeta):
    """
    An Exploration takes the predicted actions or action values from the agent,
    and selects the action to actually apply to the environment using some
    predefined exploration schema.
    """
    def __init__(self, action_space=None, framework="tf"):
        """
        Args:
            action_space (Optional[gym.spaces.Space]): The action space in
                which to explore.
            framework (str): One of "tf" or "torch".
        """
        self.action_space = action_space
        self.framework = check_framework(framework)

    def get_exploration_action(self, action, model=None,
                               action_dist=None, is_exploring=None):
        """
        Given the Model's output and action distribution, return an exploration
        action.

        Args:
            action (any): The already sampled action (non-exploratory case).
            model (ModelV2): The Model object.
            action_dist: The ActionDistribution class.
            is_exploring (bool): Whether to explore or not (this could be a tf
                placeholder).

        Returns:
            any: The chosen exploration action or a tf-op to fetch the
                exploration action from the graph.
        """
        pass

    def get_loss_exploration_term(self, model_output, model=None,
                                  action_dist=None, action_sample=None):
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

    @abstractmethod
    def get_state(self):
        raise NotImplementedError

    @abstractmethod
    def set_state(self, exploration_state):
        raise NotImplementedError

    @abstractmethod
    def reset_state(self):
        """
        Used for resetting the exploration parameters when needed.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def merge_states(cls, exploration_states):
        raise NotImplementedError
