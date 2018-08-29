from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class PolicyGraph(object):
    """An agent policy and loss, i.e., a TFPolicyGraph or other subclass.

    This object defines how to act in the environment, and also losses used to
    improve the policy based on its experiences. Note that both policy and
    loss are defined together for convenience, though the policy itself is
    logically separate.

    All policies can directly extend PolicyGraph, however TensorFlow users may
    find TFPolicyGraph simpler to implement. TFPolicyGraph also enables RLlib
    to apply TensorFlow-specific optimizations such as fusing multiple policy
    graphs and multi-GPU support.

    Attributes:
        observation_space (gym.Space): Observation space of the policy.
        action_space (gym.Space): Action space of the policy.
    """

    def __init__(self, observation_space, action_space, config):
        """Initialize the graph.

        This is the standard constructor for policy graphs. The policy graph
        class you pass into PolicyEvaluator will be constructed with
        these arguments.

        Args:
            observation_space (gym.Space): Observation space of the policy.
            action_space (gym.Space): Action space of the policy.
            config (dict): Policy-specific configuration data.
        """

        self.observation_space = observation_space
        self.action_space = action_space

    def compute_actions(self,
                        obs_batch,
                        state_batches,
                        is_training=False,
                        episodes=None):
        """Compute actions for the current policy.

        Arguments:
            obs_batch (np.ndarray): batch of observations
            state_batches (list): list of RNN state input batches, if any
            is_training (bool): whether we are training the policy
            episodes (list): MultiAgentEpisode for each obs in obs_batch.
                This provides access to all of the internal episode state,
                which may be useful for model-based or multiagent algorithms.

        Returns:
            actions (np.ndarray): batch of output actions, with shape like
                [BATCH_SIZE, ACTION_SHAPE].
            state_outs (list): list of RNN state output batches, if any, with
                shape like [STATE_SIZE, BATCH_SIZE].
            info (dict): dictionary of extra feature batches, if any, with
                shape like {"f1": [BATCH_SIZE, ...], "f2": [BATCH_SIZE, ...]}.
        """
        raise NotImplementedError

    def compute_single_action(self,
                              obs,
                              state,
                              is_training=False,
                              episode=None):
        """Unbatched version of compute_actions.

        Arguments:
            obs (obj): single observation
            state_batches (list): list of RNN state inputs, if any
            is_training (bool): whether we are training the policy
            episode (MultiAgentEpisode): this provides access to all of the
                internal episode state, which may be useful for model-based or
                multiagent algorithms.

        Returns:
            actions (obj): single action
            state_outs (list): list of RNN state outputs, if any
            info (dict): dictionary of extra features, if any
        """

        [action], state_out, info = self.compute_actions(
            [obs], [[s] for s in state], is_training, episodes=[episode])
        return action, [s[0] for s in state_out], \
            {k: v[0] for k, v in info.items()}

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        """Implements algorithm-specific trajectory postprocessing.

        This will be called on each trajectory fragment computed during policy
        evaluation. Each fragment is guaranteed to be only from one episode.

        Arguments:
            sample_batch (SampleBatch): batch of experiences for the policy,
                which will contain at most one episode trajectory.
            other_agent_batches (dict): In a multi-agent env, this contains a
                mapping of agent ids to (policy_graph, agent_batch) tuples
                containing the policy graph and experiences of the other agent.

        Returns:
            SampleBatch: postprocessed sample batch.
        """
        return sample_batch

    def compute_gradients(self, postprocessed_batch):
        """Computes gradients against a batch of experiences.

        Returns:
            grads (list): List of gradient output values
            info (dict): Extra policy-specific values
        """
        raise NotImplementedError

    def apply_gradients(self, gradients):
        """Applies previously computed gradients.

        Returns:
            info (dict): Extra policy-specific values
        """
        raise NotImplementedError

    def compute_apply(self, samples):
        """Fused compute gradients and apply gradients call.

        Returns:
            grad_info: dictionary of extra metadata from compute_gradients().
            apply_info: dictionary of extra metadata from apply_gradients().

        Examples:
            >>> batch = ev.sample()
            >>> ev.compute_apply(samples)
        """

        grads, grad_info = self.compute_gradients(samples)
        apply_info = self.apply_gradients(grads)
        return grad_info, apply_info

    def get_weights(self):
        """Returns model weights.

        Returns:
            weights (obj): Serializable copy or view of model weights
        """
        raise NotImplementedError

    def set_weights(self, weights):
        """Sets model weights.

        Arguments:
            weights (obj): Serializable copy or view of model weights
        """
        raise NotImplementedError

    def get_initial_state(self):
        """Returns initial RNN state for the current policy."""
        return []

    def get_state(self):
        """Saves all local state.

        Returns:
            state (obj): Serialized local state.
        """
        return self.get_weights()

    def set_state(self, state):
        """Restores all local state.

        Arguments:
            state (obj): Serialized local state.
        """
        self.set_weights(state)

    def on_global_var_update(self, global_vars):
        """Called on an update to global vars.

        Arguments:
            global_vars (dict): Global variables broadcast from the driver.
        """
        pass
