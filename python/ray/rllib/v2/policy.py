class Policy(object):
    """An agent policy, i.e., a TFPolicy or ImperativePolicy subclass.
    
    The policy object defines how to act in the environment, and also losses
    used to improve the policy based on its experiences.

    All agents can be implemented as ImperativePolicy, however TensorFlow users
    may find TFPolicy simpler to implement. TFPolicy also enables RLlib to
    apply TensorFlow-specific optimizations such as fusing multiple policy
    graphs and multi-GPU support.
    """

    def __init__(self, observation_space, action_space):
        self.observation_space = observation_space
        self.action_space = action_space

    def compute_actions(self, obs_batch, state_batch, is_training=False):
        """Compute actions for the current policy.

        Arguments:
            obs_batch (np.ndarray): batch of observations
            state_batch (np.ndarray): batch of recurrent state inputs

        Returns:
            actions (np.ndarray): batch of output actions
            states (np.ndarray): batch of recurrent state outputs

        TODO(ekl): support tuple and dict inputs / outputs
        """
        raise NotImplementedError

    def compute_gradients(self, postprocessed_batch):
        raise NotImplementedError

    def apply_gradients(self, gradients):
        raise NotImplementedError

    def get_weights(self):
        raise NotImplementedError

    def set_weights(self, weights):
        raise NotImplementedError

    def postprocess_trajectory(self, sample_batch, other_agent_batches=None):
        """Implements algorithm-specific trajectory postprocessing.

        Arguments:
            sample_batch (SampleBatch): batch of experiences for the policy
            other_agent_batches (dict): In a multi-agent env, this contains the
                experience batches seen by other agents.
        
        Returns:
            SampleBatch: postprocessed sample batch.
        """
        return sample_batch
