from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from collections import OrderedDict

import gym

from ray.rllib.models.misc import linear, normc_initializer
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.utils.annotations import PublicAPI, DeveloperAPI
from ray.rllib.utils import try_import_tf

tf = try_import_tf()


# Deprecated: use TFModelV2 instead
class Model(object):
    """Defines an abstract network model for use with RLlib.

    This class is deprecated: please use TFModelV2 instead.

    Models convert input tensors to a number of output features. These features
    can then be interpreted by ActionDistribution classes to determine
    e.g. agent action values.

    The last layer of the network can also be retrieved if the algorithm
    needs to further post-processing (e.g. Actor and Critic networks in A3C).

    Attributes:
        input_dict (dict): Dictionary of input tensors, including "obs",
            "prev_action", "prev_reward", "is_training".
        outputs (Tensor): The output vector of this model, of shape
            [BATCH_SIZE, num_outputs].
        last_layer (Tensor): The feature layer right before the model output,
            of shape [BATCH_SIZE, f].
        state_init (list): List of initial recurrent state tensors (if any).
        state_in (list): List of input recurrent state tensors (if any).
        state_out (list): List of output recurrent state tensors (if any).
        seq_lens (Tensor): The tensor input for RNN sequence lengths. This
            defaults to a Tensor of [1] * len(batch) in the non-RNN case.

    If `options["free_log_std"]` is True, the last half of the
    output layer will be free variables that are not dependent on
    inputs. This is often used if the output of the network is used
    to parametrize a probability distribution. In this case, the
    first half of the parameters can be interpreted as a location
    parameter (like a mean) and the second half can be interpreted as
    a scale parameter (like a standard deviation).
    """

    def __init__(self,
                 input_dict,
                 obs_space,
                 action_space,
                 num_outputs,
                 options,
                 state_in=None,
                 seq_lens=None):
        assert isinstance(input_dict, dict), input_dict

        # Default attribute values for the non-RNN case
        self.state_init = []
        self.state_in = state_in or []
        self.state_out = []
        self.obs_space = obs_space
        self.action_space = action_space
        self.num_outputs = num_outputs
        self.options = options
        self.scope = tf.get_variable_scope()
        self.session = tf.get_default_session()
        self.input_dict = input_dict
        if seq_lens is not None:
            self.seq_lens = seq_lens
        else:
            self.seq_lens = tf.placeholder(
                dtype=tf.int32, shape=[None], name="seq_lens")

        self._num_outputs = num_outputs
        if options.get("free_log_std"):
            assert num_outputs % 2 == 0
            num_outputs = num_outputs // 2

        ok = True
        try:
            restored = input_dict.copy()
            restored["obs"] = restore_original_dimensions(
                input_dict["obs"], obs_space)
            self.outputs, self.last_layer = self._build_layers_v2(
                restored, num_outputs, options)
        except NotImplementedError:
            ok = False
        # In TF 1.14, you cannot construct variable scopes in exception
        # handlers so we have to set the OK flag and check it here:
        if not ok:
            self.outputs, self.last_layer = self._build_layers(
                input_dict["obs"], num_outputs, options)

        if options.get("free_log_std", False):
            log_std = tf.get_variable(
                name="log_std",
                shape=[num_outputs],
                initializer=tf.zeros_initializer)
            self.outputs = tf.concat(
                [self.outputs, 0.0 * self.outputs + log_std], 1)

    def _build_layers(self, inputs, num_outputs, options):
        """Builds and returns the output and last layer of the network.

        Deprecated: use _build_layers_v2 instead, which has better support
        for dict and tuple spaces.
        """
        raise NotImplementedError

    @PublicAPI
    def _build_layers_v2(self, input_dict, num_outputs, options):
        """Define the layers of a custom model.

        Arguments:
            input_dict (dict): Dictionary of input tensors, including "obs",
                "prev_action", "prev_reward", "is_training".
            num_outputs (int): Output tensor must be of size
                [BATCH_SIZE, num_outputs].
            options (dict): Model options.

        Returns:
            (outputs, feature_layer): Tensors of size [BATCH_SIZE, num_outputs]
                and [BATCH_SIZE, desired_feature_size].

        When using dict or tuple observation spaces, you can access
        the nested sub-observation batches here as well:

        Examples:
            >>> print(input_dict)
            {'prev_actions': <tf.Tensor shape=(?,) dtype=int64>,
             'prev_rewards': <tf.Tensor shape=(?,) dtype=float32>,
             'is_training': <tf.Tensor shape=(), dtype=bool>,
             'obs': OrderedDict([
                ('sensors', OrderedDict([
                    ('front_cam', [
                        <tf.Tensor shape=(?, 10, 10, 3) dtype=float32>,
                        <tf.Tensor shape=(?, 10, 10, 3) dtype=float32>]),
                    ('position', <tf.Tensor shape=(?, 3) dtype=float32>),
                    ('velocity', <tf.Tensor shape=(?, 3) dtype=float32>)]))])}
        """
        raise NotImplementedError

    @PublicAPI
    def value_function(self):
        """Builds the value function output.

        This method can be overridden to customize the implementation of the
        value function (e.g., not sharing hidden layers).

        Returns:
            Tensor of size [BATCH_SIZE] for the value function.
        """
        return tf.reshape(
            linear(self.last_layer, 1, "value", normc_initializer(1.0)), [-1])

    @PublicAPI
    def custom_loss(self, policy_loss, loss_inputs):
        """Override to customize the loss function used to optimize this model.

        This can be used to incorporate self-supervised losses (by defining
        a loss over existing input and output tensors of this model), and
        supervised losses (by defining losses over a variable-sharing copy of
        this model's layers).

        You can find an runnable example in examples/custom_loss.py.

        Arguments:
            policy_loss (Tensor): scalar policy loss from the policy.
            loss_inputs (dict): map of input placeholders for rollout data.

        Returns:
            Scalar tensor for the customized loss for this model.
        """
        if self.loss() is not None:
            raise DeprecationWarning(
                "self.loss() is deprecated, use self.custom_loss() instead.")
        return policy_loss

    @PublicAPI
    def custom_stats(self):
        """Override to return custom metrics from your model.

        The stats will be reported as part of the learner stats, i.e.,
            info:
                learner:
                    model:
                        key1: metric1
                        key2: metric2

        Returns:
            Dict of string keys to scalar tensors.
        """
        return {}

    def loss(self):
        """Deprecated: use self.custom_loss()."""
        return None

    @classmethod
    def get_initial_state(cls, obs_space, action_space, num_outputs, options):
        raise NotImplementedError(
            "In order to use recurrent models with ModelV2, you should define "
            "the get_initial_state @classmethod on your custom model class.")

    def _validate_output_shape(self):
        """Checks that the model has the correct number of outputs."""
        try:
            out = tf.convert_to_tensor(self.outputs)
            shape = out.shape.as_list()
        except Exception:
            raise ValueError("Output is not a tensor: {}".format(self.outputs))
        else:
            if len(shape) != 2 or shape[1] != self._num_outputs:
                raise ValueError(
                    "Expected output shape of [None, {}], got {}".format(
                        self._num_outputs, shape))


@DeveloperAPI
def restore_original_dimensions(obs, obs_space, tensorlib=tf):
    """Unpacks Dict and Tuple space observations into their original form.

    This is needed since we flatten Dict and Tuple observations in transit.
    Before sending them to the model though, we should unflatten them into
    Dicts or Tuples of tensors.

    Arguments:
        obs: The flattened observation tensor.
        obs_space: The flattened obs space. If this has the `original_space`
            attribute, we will unflatten the tensor to that shape.
        tensorlib: The library used to unflatten (reshape) the array/tensor.

    Returns:
        single tensor or dict / tuple of tensors matching the original
        observation space.
    """

    if hasattr(obs_space, "original_space"):
        if tensorlib == "tf":
            tensorlib = tf
        elif tensorlib == "torch":
            import torch
            tensorlib = torch
        return _unpack_obs(obs, obs_space.original_space, tensorlib=tensorlib)
    else:
        return obs


def _unpack_obs(obs, space, tensorlib=tf):
    """Unpack a flattened Dict or Tuple observation array/tensor.

    Arguments:
        obs: The flattened observation tensor
        space: The original space prior to flattening
        tensorlib: The library used to unflatten (reshape) the array/tensor
    """

    if (isinstance(space, gym.spaces.Dict)
            or isinstance(space, gym.spaces.Tuple)):
        prep = get_preprocessor(space)(space)
        if len(obs.shape) != 2 or obs.shape[1] != prep.shape[0]:
            raise ValueError(
                "Expected flattened obs shape of [None, {}], got {}".format(
                    prep.shape[0], obs.shape))
        assert len(prep.preprocessors) == len(space.spaces), \
            (len(prep.preprocessors) == len(space.spaces))
        offset = 0
        if isinstance(space, gym.spaces.Tuple):
            u = []
            for p, v in zip(prep.preprocessors, space.spaces):
                obs_slice = obs[:, offset:offset + p.size]
                offset += p.size
                u.append(
                    _unpack_obs(
                        tensorlib.reshape(obs_slice, [-1] + list(p.shape)),
                        v,
                        tensorlib=tensorlib))
        else:
            u = OrderedDict()
            for p, (k, v) in zip(prep.preprocessors, space.spaces.items()):
                obs_slice = obs[:, offset:offset + p.size]
                offset += p.size
                u[k] = _unpack_obs(
                    tensorlib.reshape(obs_slice, [-1] + list(p.shape)),
                    v,
                    tensorlib=tensorlib)
        return u
    else:
        return obs
