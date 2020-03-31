from gym.spaces import Space
from typing import Union

from ray.rllib.utils.framework import check_framework, try_import_tf, \
    TensorType
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import DeveloperAPI

tf = try_import_tf()


@DeveloperAPI
class Exploration:
    """Implements an exploration strategy for Policies.

    An Exploration takes model outputs, a distribution, and a timestep from
    the agent and computes an action to apply to the environment using an
    implemented exploration schema.
    """

    def __init__(self,
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

    @DeveloperAPI
    def before_compute_actions(self,
                               *,
                               timestep=None,
                               explore=None,
                               tf_sess=None,
                               **kwargs):
        """Hook for preparations before policy.compute_actions() is called.

        Args:
            timestep (Optional[TensorType]): An optional timestep tensor.
            explore (Optional[TensorType]): An optional explore boolean flag.
            tf_sess (Optional[tf.Session]): The tf-session object to use.
            **kwargs: Forward compatibility kwargs.
        """
        pass

    @DeveloperAPI
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

    @DeveloperAPI
    def on_episode_start(self,
                         policy,
                         *,
                         environment=None,
                         episode=None,
                         tf_sess=None):
        """Handles necessary exploration logic at the beginning of an episode.

        Args:
            policy (Policy): The Policy object that holds this Exploration.
            environment (BaseEnv): The environment object we are acting in.
            episode (int): The number of the episode that is starting.
            tf_sess (Optional[tf.Session]): In case of tf, the session object.
        """
        pass

    @DeveloperAPI
    def on_episode_end(self,
                       policy,
                       *,
                       environment=None,
                       episode=None,
                       tf_sess=None):
        """Handles necessary exploration logic at the end of an episode.

        Args:
            policy (Policy): The Policy object that holds this Exploration.
            environment (BaseEnv): The environment object we are acting in.
            episode (int): The number of the episode that is starting.
            tf_sess (Optional[tf.Session]): In case of tf, the session object.
        """
        pass

    @DeveloperAPI
    def postprocess_trajectory(self, policy, sample_batch, tf_sess=None):
        """Handles post-processing of done episode trajectories.

        Changes the given batch in place. This callback is invoked by the
        sampler after policy.postprocess_trajectory() is called.

        Args:
            policy (Policy): The owning policy object.
            sample_batch (SampleBatch): The SampleBatch object to post-process.
            tf_sess (Optional[tf.Session]): An optional tf.Session object.
        """
        return sample_batch

    @DeveloperAPI
    def get_info(self):
        """Returns a description of the current exploration state.

        This is not necessarily the state itself (and cannot be used in
        set_state!), but rather useful (e.g. debugging) information.

        Returns:
            dict: A description of the Exploration (not necessarily its state).
                This may include tf.ops as values in graph mode.
        """
        return {}
