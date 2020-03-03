from abc import ABCMeta, abstractmethod
import gym
import numpy as np

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.exploration.exploration import Exploration
from ray.rllib.utils.from_config import from_config

# By convention, metrics from optimizing the loss can be reported in the
# `grad_info` dict returned by learn_on_batch() / compute_grads() via this key.
LEARNER_STATS_KEY = "learner_stats"

ACTION_PROB = "action_prob"
ACTION_LOGP = "action_logp"


@DeveloperAPI
class Policy(metaclass=ABCMeta):
    """An agent policy and loss, i.e., a TFPolicy or other subclass.

    This object defines how to act in the environment, and also losses used to
    improve the policy based on its experiences. Note that both policy and
    loss are defined together for convenience, though the policy itself is
    logically separate.

    All policies can directly extend Policy, however TensorFlow users may
    find TFPolicy simpler to implement. TFPolicy also enables RLlib
    to apply TensorFlow-specific optimizations such as fusing multiple policy
    graphs and multi-GPU support.

    Attributes:
        observation_space (gym.Space): Observation space of the policy.
        action_space (gym.Space): Action space of the policy.
    """

    @DeveloperAPI
    def __init__(self, observation_space, action_space, config):
        """Initialize the graph.

        This is the standard constructor for policies. The policy
        class you pass into RolloutWorker will be constructed with
        these arguments.

        Args:
            observation_space (gym.Space): Observation space of the policy.
            action_space (gym.Space): Action space of the policy.
            config (dict): Policy-specific configuration data.
            exploration (Exploration): The exploration object to use for
                computing actions.
        """
        self.observation_space = observation_space
        self.action_space = action_space
        self.config = config
        self.exploration = self._create_exploration(action_space, config)
        # The global timestep, broadcast down from time to time from the
        # driver.
        self.global_timestep = 0

    @abstractmethod
    @DeveloperAPI
    def compute_actions(self,
                        obs_batch,
                        state_batches=None,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        explore=None,
                        timestep=None,
                        **kwargs):
        """Computes actions for the current policy.

        Args:
            obs_batch (Union[List,np.ndarray]): Batch of observations.
            state_batches (Optional[list]): List of RNN state input batches,
                if any.
            prev_action_batch (Optional[List,np.ndarray]): Batch of previous
                action values.
            prev_reward_batch (Optional[List,np.ndarray]): Batch of previous
                rewards.
            info_batch (info): Batch of info objects.
            episodes (list): MultiAgentEpisode for each obs in obs_batch.
                This provides access to all of the internal episode state,
                which may be useful for model-based or multiagent algorithms.
            explore (bool): Whether to pick an exploitation or exploration
                action (default: None -> use self.config["explore"]).
            timestep (int): The current (sampling) time step.
            kwargs: forward compatibility placeholder

        Returns:
            actions (np.ndarray): batch of output actions, with shape like
                [BATCH_SIZE, ACTION_SHAPE].
            state_outs (list): list of RNN state output batches, if any, with
                shape like [STATE_SIZE, BATCH_SIZE].
            info (dict): dictionary of extra feature batches, if any, with
                shape like {"f1": [BATCH_SIZE, ...], "f2": [BATCH_SIZE, ...]}.
        """
        raise NotImplementedError

    @DeveloperAPI
    def compute_single_action(self,
                              obs,
                              state=None,
                              prev_action=None,
                              prev_reward=None,
                              info=None,
                              episode=None,
                              clip_actions=False,
                              explore=None,
                              timestep=None,
                              **kwargs):
        """Unbatched version of compute_actions.

        Arguments:
            obs (obj): Single observation.
            state (list): List of RNN state inputs, if any.
            prev_action (obj): Previous action value, if any.
            prev_reward (float): Previous reward, if any.
            info (dict): info object, if any
            episode (MultiAgentEpisode): this provides access to all of the
                internal episode state, which may be useful for model-based or
                multi-agent algorithms.
            clip_actions (bool): should the action be clipped
            explore (bool): Whether to pick an exploitation or exploration
                action (default: None -> use self.config["explore"]).
            timestep (int): The current (sampling) time step.
            kwargs: forward compatibility placeholder

        Returns:
            actions (obj): single action
            state_outs (list): list of RNN state outputs, if any
            info (dict): dictionary of extra features, if any
        """
        prev_action_batch = None
        prev_reward_batch = None
        info_batch = None
        episodes = None
        state_batch = None
        if prev_action is not None:
            prev_action_batch = [prev_action]
        if prev_reward is not None:
            prev_reward_batch = [prev_reward]
        if info is not None:
            info_batch = [info]
        if episode is not None:
            episodes = [episode]
        if state is not None:
            state_batch = [[s] for s in state]

        [action], state_out, info = self.compute_actions(
            [obs],
            state_batch,
            prev_action_batch=prev_action_batch,
            prev_reward_batch=prev_reward_batch,
            info_batch=info_batch,
            episodes=episodes,
            explore=explore,
            timestep=timestep)

        if clip_actions:
            action = clip_action(action, self.action_space)

        # Return action, internal state(s), infos.
        return action, [s[0] for s in state_out], \
            {k: v[0] for k, v in info.items()}

    @DeveloperAPI
    def compute_log_likelihoods(self,
                                actions,
                                obs_batch,
                                state_batches=None,
                                prev_action_batch=None,
                                prev_reward_batch=None):
        """Computes the log-prob/likelihood for a given action and observation.

        Args:
            actions (Union[List,np.ndarray]): Batch of actions, for which to
                retrieve the log-probs/likelihoods (given all other inputs:
                obs, states, ..).
            obs_batch (Union[List,np.ndarray]): Batch of observations.
            state_batches (Optional[list]): List of RNN state input batches,
                if any.
            prev_action_batch (Optional[List,np.ndarray]): Batch of previous
                action values.
            prev_reward_batch (Optional[List,np.ndarray]): Batch of previous
                rewards.

        Returns:
            log-likelihoods (np.ndarray): Batch of log probs/likelihoods, with
                shape: [BATCH_SIZE].
        """
        raise NotImplementedError

    @DeveloperAPI
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        """Implements algorithm-specific trajectory postprocessing.

        This will be called on each trajectory fragment computed during policy
        evaluation. Each fragment is guaranteed to be only from one episode.

        Arguments:
            sample_batch (SampleBatch): batch of experiences for the policy,
                which will contain at most one episode trajectory.
            other_agent_batches (dict): In a multi-agent env, this contains a
                mapping of agent ids to (policy, agent_batch) tuples
                containing the policy and experiences of the other agents.
            episode (MultiAgentEpisode): this provides access to all of the
                internal episode state, which may be useful for model-based or
                multi-agent algorithms.

        Returns:
            SampleBatch: Postprocessed sample batch.
        """
        return sample_batch

    @DeveloperAPI
    def learn_on_batch(self, samples):
        """Fused compute gradients and apply gradients call.

        Either this or the combination of compute/apply grads must be
        implemented by subclasses.

        Returns:
            grad_info: dictionary of extra metadata from compute_gradients().

        Examples:
            >>> batch = ev.sample()
            >>> ev.learn_on_batch(samples)
        """

        grads, grad_info = self.compute_gradients(samples)
        self.apply_gradients(grads)
        return grad_info

    @DeveloperAPI
    def compute_gradients(self, postprocessed_batch):
        """Computes gradients against a batch of experiences.

        Either this or learn_on_batch() must be implemented by subclasses.

        Returns:
            grads (list): List of gradient output values
            info (dict): Extra policy-specific values
        """
        raise NotImplementedError

    @DeveloperAPI
    def apply_gradients(self, gradients):
        """Applies previously computed gradients.

        Either this or learn_on_batch() must be implemented by subclasses.
        """
        raise NotImplementedError

    @DeveloperAPI
    def get_weights(self):
        """Returns model weights.

        Returns:
            weights (obj): Serializable copy or view of model weights
        """
        raise NotImplementedError

    @DeveloperAPI
    def set_weights(self, weights):
        """Sets model weights.

        Arguments:
            weights (obj): Serializable copy or view of model weights
        """
        raise NotImplementedError

    @DeveloperAPI
    def get_exploration_info(self):
        """Returns the current exploration information of this policy.

        This information depends on the policy's Exploration object.

        Returns:
            any: Serializable information on the `self.exploration` object.
        """
        return self.exploration.get_info()

    @DeveloperAPI
    def get_exploration_state(self):
        """Returns the current exploration state of this policy.

        This state depends on the policy's Exploration object.

        Returns:
            any: Serializable copy or view of the current exploration state.
        """
        raise NotImplementedError

    @DeveloperAPI
    def set_exploration_state(self, exploration_state):
        """Sets the current exploration state of this Policy.

        Arguments:
            exploration_state (any): Serializable copy or view of the new
                exploration state.
        """
        raise NotImplementedError

    @DeveloperAPI
    def is_recurrent(self):
        """Whether this Policy holds a recurrent Model.

        Returns:
            bool: True if this Policy has-a RNN-based Model.
        """
        return 0

    @DeveloperAPI
    def num_state_tensors(self):
        """The number of internal states needed by the RNN-Model of the Policy.

        Returns:
            int: The number of RNN internal states kept by this Policy's Model.
        """
        return 0

    @DeveloperAPI
    def get_initial_state(self):
        """Returns initial RNN state for the current policy."""
        return []

    @DeveloperAPI
    def get_state(self):
        """Saves all local state.

        Returns:
            state (obj): Serialized local state.
        """
        return self.get_weights()

    @DeveloperAPI
    def set_state(self, state):
        """Restores all local state.

        Arguments:
            state (obj): Serialized local state.
        """
        self.set_weights(state)

    @DeveloperAPI
    def on_global_var_update(self, global_vars):
        """Called on an update to global vars.

        Arguments:
            global_vars (dict): Global variables broadcast from the driver.
        """
        # Store the current global time step (sum over all policies' sample
        # steps).
        self.global_timestep = global_vars["timestep"]

    @DeveloperAPI
    def export_model(self, export_dir):
        """Export Policy to local directory for serving.

        Arguments:
            export_dir (str): Local writable directory.
        """
        raise NotImplementedError

    @DeveloperAPI
    def export_checkpoint(self, export_dir):
        """Export Policy checkpoint to local directory.

        Argument:
            export_dir (str): Local writable directory.
        """
        raise NotImplementedError

    def _create_exploration(self, action_space, config):
        """Creates the Policy's Exploration object.

        This method only exists b/c some Trainers do not use TfPolicy nor
        TorchPolicy, but inherit directly from Policy. Others inherit from
        TfPolicy w/o using DynamicTfPolicy.
        TODO(sven): unify these cases."""
        exploration = from_config(
            Exploration,
            config.get("exploration_config", {"type": "StochasticSampling"}),
            action_space=action_space,
            num_workers=config.get("num_workers"),
            worker_index=config.get("worker_index"),
            framework=getattr(self, "framework", "tf"))
        # If config is further passed around, it'll contain an already
        # instantiated object.
        config["exploration_config"] = exploration
        return exploration


def clip_action(action, space):
    """
    Called to clip actions to the specified range of this policy.

    Arguments:
        action: Single action.
        space: Action space the actions should be present in.

    Returns:
        Clipped batch of actions.
    """

    if isinstance(space, gym.spaces.Box):
        return np.clip(action, space.low, space.high)
    elif isinstance(space, gym.spaces.Tuple):
        if type(action) not in (tuple, list):
            raise ValueError("Expected tuple space for actions {}: {}".format(
                action, space))
        out = []
        for a, s in zip(action, space.spaces):
            out.append(clip_action(a, s))
        return out
    else:
        return action
