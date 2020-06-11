from collections import OrderedDict
import gym

from ray.rllib.models.preprocessors import get_preprocessor, \
    RepeatedValuesPreprocessor
from ray.rllib.models.repeated_values import RepeatedValues
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI, PublicAPI
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.spaces.repeated import Repeated

tf = try_import_tf()
torch, _ = try_import_torch()


@PublicAPI
class ModelV2:
    """Defines a Keras-style abstract network model for use with RLlib.

    Custom models should extend either TFModelV2 or TorchModelV2 instead of
    this class directly.

    Data flow:
        obs -> forward() -> model_out
               value_function() -> V(s)

    Attributes:
        obs_space (Space): observation space of the target gym env. This
            may have an `original_space` attribute that specifies how to
            unflatten the tensor into a ragged tensor.
        action_space (Space): action space of the target gym env
        num_outputs (int): number of output units of the model
        model_config (dict): config for the model, documented in ModelCatalog
        name (str): name (scope) for the model
        framework (str): either "tf" or "torch"
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name, framework):
        """Initialize the model.

        This method should create any variables used by the model.
        """

        self.obs_space = obs_space
        self.action_space = action_space
        self.num_outputs = num_outputs
        self.model_config = model_config
        self.name = name or "default_model"
        self.framework = framework
        self._last_output = None

    def get_initial_state(self):
        """Get the initial recurrent state values for the model.

        Returns:
            List[np.ndarray]: List of np.array objects containing the initial
                hidden state of an RNN, if applicable.

        Examples:
            >>> def get_initial_state(self):
            >>>    return [
            >>>        np.zeros(self.cell_size, np.float32),
            >>>        np.zeros(self.cell_size, np.float32),
            >>>    ]
        """
        return []

    def forward(self, input_dict, state, seq_lens):
        """Call the model with the given input tensors and state.

        Any complex observations (dicts, tuples, etc.) will be unpacked by
        __call__ before being passed to forward(). To access the flattened
        observation tensor, refer to input_dict["obs_flat"].

        This method can be called any number of times. In eager execution,
        each call to forward() will eagerly evaluate the model. In symbolic
        execution, each call to forward creates a computation graph that
        operates over the variables of this model (i.e., shares weights).

        Custom models should override this instead of __call__.

        Args:
            input_dict (dict): dictionary of input tensors, including "obs",
                "obs_flat", "prev_action", "prev_reward", "is_training"
            state (list): list of state tensors with sizes matching those
                returned by get_initial_state + the batch dimension
            seq_lens (Tensor): 1d tensor holding input sequence lengths

        Returns:
            (outputs, state): The model output tensor of size
                [BATCH, num_outputs]

        Examples:
            >>> def forward(self, input_dict, state, seq_lens):
            >>>     model_out, self._value_out = self.base_model(
            ...         input_dict["obs"])
            >>>     return model_out, state
        """
        raise NotImplementedError

    def value_function(self):
        """Returns the value function output for the most recent forward pass.

        Note that a `forward` call has to be performed first, before this
        methods can return anything and thus that calling this method does not
        cause an extra forward pass through the network.

        Returns:
            value estimate tensor of shape [BATCH].
        """
        raise NotImplementedError

    def custom_loss(self, policy_loss, loss_inputs):
        """Override to customize the loss function used to optimize this model.

        This can be used to incorporate self-supervised losses (by defining
        a loss over existing input and output tensors of this model), and
        supervised losses (by defining losses over a variable-sharing copy of
        this model's layers).

        You can find an runnable example in examples/custom_loss.py.

        Arguments:
            policy_loss (Union[List[Tensor],Tensor]): List of or single policy
                loss(es) from the policy.
            loss_inputs (dict): map of input placeholders for rollout data.

        Returns:
            Union[List[Tensor],Tensor]: List of or scalar tensor for the
                customized loss(es) for this model.
        """
        return policy_loss

    def metrics(self):
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

    def __call__(self, input_dict, state=None, seq_lens=None):
        """Call the model with the given input tensors and state.

        This is the method used by RLlib to execute the forward pass. It calls
        forward() internally after unpacking nested observation tensors.

        Custom models should override forward() instead of __call__.

        Arguments:
            input_dict (dict): dictionary of input tensors, including "obs",
                "prev_action", "prev_reward", "is_training"
            state (list): list of state tensors with sizes matching those
                returned by get_initial_state + the batch dimension
            seq_lens (Tensor): 1d tensor holding input sequence lengths

        Returns:
            (outputs, state): The model output tensor of size
                [BATCH, output_spec.size] or a list of tensors corresponding to
                output_spec.shape_list, and a list of state tensors of
                [BATCH, state_size_i].
        """

        restored = input_dict.copy()
        restored["obs"] = restore_original_dimensions(
            input_dict["obs"], self.obs_space, self.framework)
        if len(input_dict["obs"].shape) > 2:
            restored["obs_flat"] = flatten(input_dict["obs"], self.framework)
        else:
            restored["obs_flat"] = input_dict["obs"]
        with self.context():
            res = self.forward(restored, state or [], seq_lens)
        if ((not isinstance(res, list) and not isinstance(res, tuple))
                or len(res) != 2):
            raise ValueError(
                "forward() must return a tuple of (output, state) tensors, "
                "got {}".format(res))
        outputs, state = res

        try:
            shape = outputs.shape
        except AttributeError:
            raise ValueError("Output is not a tensor: {}".format(outputs))
        else:
            if len(shape) != 2 or int(shape[1]) != self.num_outputs:
                raise ValueError(
                    "Expected output shape of [None, {}], got {}".format(
                        self.num_outputs, shape))
        if not isinstance(state, list):
            raise ValueError("State output is not a list: {}".format(state))

        self._last_output = outputs
        return outputs, state

    def from_batch(self, train_batch, is_training=True):
        """Convenience function that calls this model with a tensor batch.

        All this does is unpack the tensor batch to call this model with the
        right input dict, state, and seq len arguments.
        """

        input_dict = {
            "obs": train_batch[SampleBatch.CUR_OBS],
            "is_training": is_training,
        }
        if SampleBatch.PREV_ACTIONS in train_batch:
            input_dict["prev_actions"] = train_batch[SampleBatch.PREV_ACTIONS]
        if SampleBatch.PREV_REWARDS in train_batch:
            input_dict["prev_rewards"] = train_batch[SampleBatch.PREV_REWARDS]
        states = []
        i = 0
        while "state_in_{}".format(i) in train_batch:
            states.append(train_batch["state_in_{}".format(i)])
            i += 1
        return self.__call__(input_dict, states, train_batch.get("seq_lens"))

    def import_from_h5(self, h5_file):
        """Imports weights from an h5 file.

        Args:
            h5_file (str): The h5 file name to import weights from.

        Example:
            >>> trainer = MyTrainer()
            >>> trainer.import_policy_model_from_h5("/tmp/weights.h5")
            >>> for _ in range(10):
            >>>     trainer.train()
        """
        raise NotImplementedError

    def last_output(self):
        """Returns the last output returned from calling the model."""
        return self._last_output

    def context(self):
        """Returns a contextmanager for the current forward pass."""
        return NullContextManager()

    def variables(self, as_dict=False):
        """Returns the list (or a dict) of variables for this model.

        Args:
            as_dict(bool): Whether variables should be returned as dict-values
                (using descriptive keys).

        Returns:
            Union[List[any],Dict[str,any]]: The list (or dict if `as_dict` is
                True) of all variables of this ModelV2.
        """
        raise NotImplementedError

    def trainable_variables(self, as_dict=False):
        """Returns the list of trainable variables for this model.

        Args:
            as_dict(bool): Whether variables should be returned as dict-values
                (using descriptive keys).

        Returns:
            Union[List[any],Dict[str,any]]: The list (or dict if `as_dict` is
                True) of all trainable (tf)/requires_grad (torch) variables
                of this ModelV2.
        """
        raise NotImplementedError


class NullContextManager:
    """No-op context manager"""

    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


@DeveloperAPI
def flatten(obs, framework):
    """Flatten the given tensor."""
    if framework == "tf":
        return tf.layers.flatten(obs)
    elif framework == "torch":
        assert torch is not None
        return torch.flatten(obs, start_dim=1)
    else:
        raise NotImplementedError("flatten", framework)


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
            assert torch is not None
            tensorlib = torch
        return _unpack_obs(obs, obs_space.original_space, tensorlib=tensorlib)
    else:
        return obs


# Cache of preprocessors, for if the user is calling unpack obs often.
_cache = {}


def _unpack_obs(obs, space, tensorlib=tf):
    """Unpack a flattened Dict or Tuple observation array/tensor.

    Arguments:
        obs: The flattened observation tensor, with last dimension equal to
            the flat size and any number of batch dimensions. For example, for
            Box(4,), the obs may have shape [B, 4], or [B, N, M, 4] in case
            the Box was nested under two Repeated spaces.
        space: The original space prior to flattening
        tensorlib: The library used to unflatten (reshape) the array/tensor
    """

    if (isinstance(space, gym.spaces.Dict)
            or isinstance(space, gym.spaces.Tuple)
            or isinstance(space, Repeated)):
        if id(space) in _cache:
            prep = _cache[id(space)]
        else:
            prep = get_preprocessor(space)(space)
            # Make an attempt to cache the result, if enough space left.
            if len(_cache) < 999:
                _cache[id(space)] = prep
        if len(obs.shape) < 2 or obs.shape[-1] != prep.shape[0]:
            raise ValueError(
                "Expected flattened obs shape of [..., {}], got {}".format(
                    prep.shape[0], obs.shape))
        offset = 0
        if tensorlib == tf:
            batch_dims = [v.value for v in obs.shape[:-1]]
            batch_dims = [-1 if v is None else v for v in batch_dims]
        else:
            batch_dims = list(obs.shape[:-1])
        if isinstance(space, gym.spaces.Tuple):
            assert len(prep.preprocessors) == len(space.spaces), \
                (len(prep.preprocessors) == len(space.spaces))
            u = []
            for p, v in zip(prep.preprocessors, space.spaces):
                obs_slice = obs[..., offset:offset + p.size]
                offset += p.size
                u.append(
                    _unpack_obs(
                        tensorlib.reshape(obs_slice,
                                          batch_dims + list(p.shape)),
                        v,
                        tensorlib=tensorlib))
        elif isinstance(space, gym.spaces.Dict):
            assert len(prep.preprocessors) == len(space.spaces), \
                (len(prep.preprocessors) == len(space.spaces))
            u = OrderedDict()
            for p, (k, v) in zip(prep.preprocessors, space.spaces.items()):
                obs_slice = obs[..., offset:offset + p.size]
                offset += p.size
                u[k] = _unpack_obs(
                    tensorlib.reshape(obs_slice, batch_dims + list(p.shape)),
                    v,
                    tensorlib=tensorlib)
        elif isinstance(space, Repeated):
            assert isinstance(prep, RepeatedValuesPreprocessor), prep
            child_size = prep.child_preprocessor.size
            # The list lengths are stored in the first slot of the flat obs.
            lengths = obs[..., 0]
            # [B, ..., 1 + max_len * child_sz] -> [B, ..., max_len, child_sz]
            with_repeat_dim = tensorlib.reshape(
                obs[..., 1:], batch_dims + [space.max_len, child_size])
            # Retry the unpack, dropping the List container space.
            u = _unpack_obs(
                with_repeat_dim, space.child_space, tensorlib=tensorlib)
            return RepeatedValues(
                u, lengths=lengths, max_len=prep._obs_space.max_len)
        else:
            assert False, space
        return u
    else:
        return obs
