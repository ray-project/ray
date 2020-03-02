from gym.spaces import Space
from ray.rllib.utils.framework import check_framework, try_import_tf, \
    TensorType
from ray.rllib.models.modelv2 import ModelV2
from typing import Union

tf = try_import_tf()


class Exploration:
    """Implements an exploration strategy for Policies.

    An Exploration takes model outputs, a distribution, and a timestep from
    the agent and computes an action to apply to the environment using an
    implemented exploration schema.
    """

    def __init__(self,
                 *,
                 action_space: Space,
                 num_workers: int,
                 worker_index: int,
                 framework: str = "tf"):
        """
        Args:
            action_space (Space): The action space in which to explore.
            num_workers (int): The overall number of workers used.
            worker_index (int): The index of the worker using this class.
            framework (str): One of "tf" or "torch".
        """
        self.action_space = action_space
        self.num_workers = num_workers
        self.worker_index = worker_index
        self.framework = check_framework(framework)

    def get_exploration_action(self,
                               distribution_inputs: TensorType,
                               action_dist_class: type,
                               model: ModelV2,
                               timestep: Union[int, TensorType],
                               explore: bool = True):
        """Returns a (possibly) exploratory action and its log-likelihood.

        Given the Model's logits outputs and action distribution, returns an
        exploratory action.

        Args:
            distribution_inputs (TensorType): The output coming from the model,
                ready for parameterizing a distribution
                (e.g. q-values or PG-logits).
            action_dist_class (class): The action distribution class
                to use.
            model (ModelV2): The Model object.
            timestep (int|TensorType): The current sampling time step. It can
                be a tensor for TF graph mode, otherwise an integer.
            explore (bool): True: "Normal" exploration behavior.
                False: Suppress all exploratory behavior and return
                    a deterministic action.

        Returns:
            Tuple:
            - The chosen exploration action or a tf-op to fetch the exploration
              action from the graph.
            - The log-likelihood of the exploration action.
        """
        pass

    def get_loss_exploration_term(self,
                                  model_output: TensorType,
                                  model: ModelV2,
                                  action_dist: type,
                                  action_sample: TensorType = None):
        """Returns an extra loss term to be added to a loss.

        Args:
            model_output (TensorType): The Model's output Tensor(s).
            model (ModelV2): The Model object.
            action_dist: The ActionDistribution object resulting from
                `model_output`. TODO: Or the class?
            action_sample (TensorType): An optional action sample.

        Returns:
            TensorType: The extra loss term to add to the loss.
        """
        pass  # TODO(sven): implement for some example Exploration class.

    def get_info(self):
        """Returns a description of the current exploration state.

        This is not necessarily the state itself (and cannot be used in
        set_state!), but rather useful (e.g. debugging) information.

        Returns:
            dict: A description of the Exploration (not necessarily its state).
                This may include tf.ops as values in graph mode.
        """
        return {}
