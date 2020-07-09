from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
class ActionDistribution:
    """The policy action distribution of an agent.

    Attributes:
        inputs (Tensors): input vector to compute samples from.
        model (ModelV2): reference to model producing the inputs.
    """

    @DeveloperAPI
    def __init__(self, inputs, model):
        """Initialize the action dist.

        Arguments:
            inputs (Tensors): input vector to compute samples from.
            model (ModelV2): reference to model producing the inputs. This
                is mainly useful if you want to use model variables to compute
                action outputs (i.e., for auto-regressive action distributions,
                see examples/autoregressive_action_dist.py).
        """
        self.inputs = inputs
        self.model = model

    @DeveloperAPI
    def sample(self):
        """Draw a sample from the action distribution."""
        raise NotImplementedError

    @DeveloperAPI
    def deterministic_sample(self):
        """
        Get the deterministic "sampling" output from the distribution.
        This is usually the max likelihood output, i.e. mean for Normal, argmax
        for Categorical, etc..
        """
        raise NotImplementedError

    @DeveloperAPI
    def sampled_action_logp(self):
        """Returns the log probability of the last sampled action."""
        raise NotImplementedError

    @DeveloperAPI
    def logp(self, x):
        """The log-likelihood of the action distribution."""
        raise NotImplementedError

    @DeveloperAPI
    def kl(self, other):
        """The KL-divergence between two action distributions."""
        raise NotImplementedError

    @DeveloperAPI
    def entropy(self):
        """The entropy of the action distribution."""
        raise NotImplementedError

    def multi_kl(self, other):
        """The KL-divergence between two action distributions.

        This differs from kl() in that it can return an array for
        MultiDiscrete. TODO(ekl) consider removing this.
        """
        return self.kl(other)

    def multi_entropy(self):
        """The entropy of the action distribution.

        This differs from entropy() in that it can return an array for
        MultiDiscrete. TODO(ekl) consider removing this.
        """
        return self.entropy()

    @DeveloperAPI
    @staticmethod
    def required_model_output_shape(action_space, model_config):
        """Returns the required shape of an input parameter tensor for a
        particular action space and an optional dict of distribution-specific
        options.

        Args:
            action_space (gym.Space): The action space this distribution will
                be used for, whose shape attributes will be used to determine
                the required shape of the input parameter tensor.
            model_config (dict): Model's config dict (as defined in catalog.py)

        Returns:
            model_output_shape (int or np.ndarray of ints): size of the
                required input vector (minus leading batch dimension).
        """
        raise NotImplementedError
