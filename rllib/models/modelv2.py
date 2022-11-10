from collections import OrderedDict
import contextlib
import gym
from gym.spaces import Space
import numpy as np
from typing import Dict, List, Any, Union

from ray.rllib.models.preprocessors import get_preprocessor, RepeatedValuesPreprocessor
from ray.rllib.models.repeated_values import RepeatedValues
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils import NullContextManager
from ray.rllib.utils.annotations import DeveloperAPI, PublicAPI
from ray.rllib.utils.deprecation import Deprecated
from ray.rllib.utils.framework import try_import_tf, try_import_torch, TensorType
from ray.rllib.utils.spaces.repeated import Repeated
from ray.rllib.utils.typing import ModelConfigDict, ModelInputDict, TensorStructType

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


@PublicAPI
class ModelV2:
    r"""Defines an abstract neural network model for use with RLlib.

    Custom models should extend either TFModelV2 or TorchModelV2 instead of
    this class directly.

    Data flow:
        obs -> forward() -> model_out
            \-> value_function() -> V(s)
    """

    def __init__(
        self,
        obs_space: Space,
        action_space: Space,
        num_outputs: int,
        model_config: ModelConfigDict,
        name: str,
        framework: str,
    ):
        """Initializes a ModelV2 instance.

        This method should create any variables used by the model.

        Args:
            obs_space: Observation space of the target gym
                env. This may have an `original_space` attribute that
                specifies how to unflatten the tensor into a ragged tensor.
            action_space: Action space of the target gym
                env.
            num_outputs: Number of output units of the model.
            model_config: Config for the model, documented
                in ModelCatalog.
            name: Name (scope) for the model.
            framework: Either "tf" or "torch".
        """

        self.obs_space: Space = obs_space
        self.action_space: Space = action_space
        self.num_outputs: int = num_outputs
        self.model_config: ModelConfigDict = model_config
        self.name: str = name or "default_model"
        self.framework: str = framework
        self._last_output = None
        self.time_major = self.model_config.get("_time_major")
        # Basic view requirement for all models: Use the observation as input.
        self.view_requirements = {
            SampleBatch.OBS: ViewRequirement(shift=0, space=self.obs_space),
        }

    # TODO: (sven): Get rid of `get_initial_state` once Trajectory
    #  View API is supported across all of RLlib.
    @PublicAPI
    def get_initial_state(self) -> List[TensorType]:
        """Get the initial recurrent state values for the model.

        Returns:
            List of np.array (for tf) or Tensor (for torch) objects containing the
            initial hidden state of an RNN, if applicable.

        Examples:
            >>> import numpy as np
            >>> from ray.rllib.models.modelv2 import ModelV2
            >>> class MyModel(ModelV2): # doctest: +SKIP
            ...     # ...
            ...     def get_initial_state(self):
            ...         return [
            ...             np.zeros(self.cell_size, np.float32),
            ...             np.zeros(self.cell_size, np.float32),
            ...         ]
        """
        return []

    @PublicAPI
    def forward(
        self,
        input_dict: Dict[str, TensorType],
        state: List[TensorType],
        seq_lens: TensorType,
    ) -> (TensorType, List[TensorType]):
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
            input_dict: dictionary of input tensors, including "obs",
                "obs_flat", "prev_action", "prev_reward", "is_training",
                "eps_id", "agent_id", "infos", and "t".
            state: list of state tensors with sizes matching those
                returned by get_initial_state + the batch dimension
            seq_lens: 1d tensor holding input sequence lengths

        Returns:
            A tuple consisting of the model output tensor of size
            [BATCH, num_outputs] and the list of new RNN state(s) if any.

        Examples:
            >>> import numpy as np
            >>> from ray.rllib.models.modelv2 import ModelV2
            >>> class MyModel(ModelV2): # doctest: +SKIP
            ...     # ...
            >>>     def forward(self, input_dict, state, seq_lens):# doctest: +SKIP
            >>>         model_out, self._value_out = self.base_model(# doctest: +SKIP
            ...             input_dict["obs"])# doctest: +SKIP
            >>>         return model_out, state# doctest: +SKIP
        """
        raise NotImplementedError

    @PublicAPI
    def value_function(self) -> TensorType:
        """Returns the value function output for the most recent forward pass.

        Note that a `forward` call has to be performed first, before this
        methods can return anything and thus that calling this method does not
        cause an extra forward pass through the network.

        Returns:
            Value estimate tensor of shape [BATCH].
        """
        raise NotImplementedError

    @PublicAPI
    def custom_loss(
        self, policy_loss: TensorType, loss_inputs: Dict[str, TensorType]
    ) -> Union[List[TensorType], TensorType]:
        """Override to customize the loss function used to optimize this model.

        This can be used to incorporate self-supervised losses (by defining
        a loss over existing input and output tensors of this model), and
        supervised losses (by defining losses over a variable-sharing copy of
        this model's layers).

        You can find an runnable example in examples/custom_loss.py.

        Args:
            policy_loss: List of or single policy loss(es) from the policy.
            loss_inputs: map of input placeholders for rollout data.

        Returns:
            List of or scalar tensor for the customized loss(es) for this
            model.
        """
        return policy_loss

    @PublicAPI
    def metrics(self) -> Dict[str, TensorType]:
        """Override to return custom metrics from your model.

        The stats will be reported as part of the learner stats, i.e.,
        info.learner.[policy_id, e.g. "default_policy"].model.key1=metric1

        Returns:
            The custom metrics for this model.
        """
        return {}

    def __call__(
        self,
        input_dict: Union[SampleBatch, ModelInputDict],
        state: List[Any] = None,
        seq_lens: TensorType = None,
    ) -> (TensorType, List[TensorType]):
        """Call the model with the given input tensors and state.

        This is the method used by RLlib to execute the forward pass. It calls
        forward() internally after unpacking nested observation tensors.

        Custom models should override forward() instead of __call__.

        Args:
            input_dict: Dictionary of input tensors.
            state: list of state tensors with sizes matching those
                returned by get_initial_state + the batch dimension
            seq_lens: 1D tensor holding input sequence lengths.

        Returns:
            A tuple consisting of the model output tensor of size
                [BATCH, output_spec.size] or a list of tensors corresponding to
                output_spec.shape_list, and a list of state tensors of
                [BATCH, state_size_i] if any.
        """

        # Original observations will be stored in "obs".
        # Flattened (preprocessed) obs will be stored in "obs_flat".

        # SampleBatch case: Models can now be called directly with a
        # SampleBatch (which also includes tracking-dict case (deprecated now),
        # where tensors get automatically converted).
        if isinstance(input_dict, SampleBatch):
            restored = input_dict.copy(shallow=True)
        else:
            restored = input_dict.copy()

        # Backward compatibility.
        if not state:
            state = []
            i = 0
            while "state_in_{}".format(i) in input_dict:
                state.append(input_dict["state_in_{}".format(i)])
                i += 1
        if seq_lens is None:
            seq_lens = input_dict.get(SampleBatch.SEQ_LENS)

        # No Preprocessor used: `config._disable_preprocessor_api`=True.
        # TODO: This is unnecessary for when no preprocessor is used.
        #  Obs are not flat then anymore. However, we'll keep this
        #  here for backward-compatibility until Preprocessors have
        #  been fully deprecated.
        if self.model_config.get("_disable_preprocessor_api"):
            restored["obs_flat"] = input_dict["obs"]
        # Input to this Model went through a Preprocessor.
        # Generate extra keys: "obs_flat" (vs "obs", which will hold the
        # original obs).
        else:
            restored["obs"] = restore_original_dimensions(
                input_dict["obs"], self.obs_space, self.framework
            )
            try:
                if len(input_dict["obs"].shape) > 2:
                    restored["obs_flat"] = flatten(input_dict["obs"], self.framework)
                else:
                    restored["obs_flat"] = input_dict["obs"]
            except AttributeError:
                restored["obs_flat"] = input_dict["obs"]

        with self.context():
            res = self.forward(restored, state or [], seq_lens)

        if isinstance(input_dict, SampleBatch):
            input_dict.accessed_keys = restored.accessed_keys - {"obs_flat"}
            input_dict.deleted_keys = restored.deleted_keys
            input_dict.added_keys = restored.added_keys - {"obs_flat"}

        if (not isinstance(res, list) and not isinstance(res, tuple)) or len(res) != 2:
            raise ValueError(
                "forward() must return a tuple of (output, state) tensors, "
                "got {}".format(res)
            )
        outputs, state_out = res

        if not isinstance(state_out, list):
            raise ValueError("State output is not a list: {}".format(state_out))

        self._last_output = outputs
        return outputs, state_out if len(state_out) > 0 else (state or [])

    def import_from_h5(self, h5_file: str) -> None:
        """Imports weights from an h5 file.

        Args:
            h5_file: The h5 file name to import weights from.

        Example:
            >>> from ray.rllib.algorithms.ppo import PPO
            >>> trainer = PPO(...)  # doctest: +SKIP
            >>> trainer.import_policy_model_from_h5("/tmp/weights.h5") # doctest: +SKIP
            >>> for _ in range(10): # doctest: +SKIP
            >>>     trainer.train() # doctest: +SKIP
        """
        raise NotImplementedError

    @PublicAPI
    def last_output(self) -> TensorType:
        """Returns the last output returned from calling the model."""
        return self._last_output

    @PublicAPI
    def context(self) -> contextlib.AbstractContextManager:
        """Returns a contextmanager for the current forward pass."""
        return NullContextManager()

    @PublicAPI
    def variables(
        self, as_dict: bool = False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        """Returns the list (or a dict) of variables for this model.

        Args:
            as_dict: Whether variables should be returned as dict-values
                (using descriptive str keys).

        Returns:
            The list (or dict if `as_dict` is True) of all variables of this
            ModelV2.
        """
        raise NotImplementedError

    @PublicAPI
    def trainable_variables(
        self, as_dict: bool = False
    ) -> Union[List[TensorType], Dict[str, TensorType]]:
        """Returns the list of trainable variables for this model.

        Args:
            as_dict: Whether variables should be returned as dict-values
                (using descriptive keys).

        Returns:
            The list (or dict if `as_dict` is True) of all trainable
            (tf)/requires_grad (torch) variables of this ModelV2.
        """
        raise NotImplementedError

    @PublicAPI
    def is_time_major(self) -> bool:
        """If True, data for calling this ModelV2 must be in time-major format.

        Returns
            Whether this ModelV2 requires a time-major (TxBx...) data
            format.
        """
        return self.time_major is True

    @Deprecated(new="ModelV2.__call__()", error=True)
    def from_batch(
        self, train_batch: SampleBatch, is_training: bool = True
    ) -> (TensorType, List[TensorType]):
        """Convenience function that calls this model with a tensor batch.

        All this does is unpack the tensor batch to call this model with the
        right input dict, state, and seq len arguments.
        """

        input_dict = train_batch.copy()
        input_dict.set_training(is_training)
        states = []
        i = 0
        while "state_in_{}".format(i) in input_dict:
            states.append(input_dict["state_in_{}".format(i)])
            i += 1
        ret = self.__call__(input_dict, states, input_dict.get(SampleBatch.SEQ_LENS))
        return ret


@DeveloperAPI
def flatten(obs: TensorType, framework: str) -> TensorType:
    """Flatten the given tensor."""
    if framework in ["tf2", "tf"]:
        return tf1.keras.layers.Flatten()(obs)
    elif framework == "torch":
        assert torch is not None
        return torch.flatten(obs, start_dim=1)
    else:
        raise NotImplementedError("flatten", framework)


@DeveloperAPI
def restore_original_dimensions(
    obs: TensorType, obs_space: Space, tensorlib: Any = tf
) -> TensorStructType:
    """Unpacks Dict and Tuple space observations into their original form.

    This is needed since we flatten Dict and Tuple observations in transit
    within a SampleBatch. Before sending them to the model though, we should
    unflatten them into Dicts or Tuples of tensors.

    Args:
        obs: The flattened observation tensor.
        obs_space: The flattened obs space. If this has the
            `original_space` attribute, we will unflatten the tensor to that
            shape.
        tensorlib: The library used to unflatten (reshape) the array/tensor.

    Returns:
        single tensor or dict / tuple of tensors matching the original
        observation space.
    """

    if tensorlib in ["tf", "tf2"]:
        assert tf is not None
        tensorlib = tf
    elif tensorlib == "torch":
        assert torch is not None
        tensorlib = torch
    elif tensorlib == "numpy":
        assert np is not None
        tensorlib = np
    original_space = getattr(obs_space, "original_space", obs_space)
    return _unpack_obs(obs, original_space, tensorlib=tensorlib)


# Cache of preprocessors, for if the user is calling unpack obs often.
_cache = {}


def _unpack_obs(obs: TensorType, space: Space, tensorlib: Any = tf) -> TensorStructType:
    """Unpack a flattened Dict or Tuple observation array/tensor.

    Args:
        obs: The flattened observation tensor, with last dimension equal to
            the flat size and any number of batch dimensions. For example, for
            Box(4,), the obs may have shape [B, 4], or [B, N, M, 4] in case
            the Box was nested under two Repeated spaces.
        space: The original space prior to flattening
        tensorlib: The library used to unflatten (reshape) the array/tensor
    """

    if isinstance(space, (gym.spaces.Dict, gym.spaces.Tuple, Repeated)):
        # Already unpacked?
        if (isinstance(space, gym.spaces.Tuple) and isinstance(obs, (list, tuple))) or (
            isinstance(space, gym.spaces.Dict) and isinstance(obs, dict)
        ):
            return obs
        # Unpack using preprocessor
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
                    prep.shape[0], obs.shape
                )
            )
        offset = 0
        if tensorlib == tf:

            def get_value(v):
                if v is None:
                    return -1
                elif isinstance(v, int):
                    return v
                elif v.value is None:
                    return -1
                else:
                    return v.value

            batch_dims = [get_value(v) for v in obs.shape[:-1]]
        else:
            batch_dims = list(obs.shape[:-1])
        if isinstance(space, gym.spaces.Tuple):
            assert len(prep.preprocessors) == len(space.spaces), len(
                prep.preprocessors
            ) == len(space.spaces)
            u = []
            for p, v in zip(prep.preprocessors, space.spaces):
                obs_slice = obs[..., offset : offset + p.size]
                offset += p.size
                u.append(
                    _unpack_obs(
                        tensorlib.reshape(obs_slice, batch_dims + list(p.shape)),
                        v,
                        tensorlib=tensorlib,
                    )
                )
        elif isinstance(space, gym.spaces.Dict):
            assert len(prep.preprocessors) == len(space.spaces), len(
                prep.preprocessors
            ) == len(space.spaces)
            u = OrderedDict()
            for p, (k, v) in zip(prep.preprocessors, space.spaces.items()):
                obs_slice = obs[..., offset : offset + p.size]
                offset += p.size
                u[k] = _unpack_obs(
                    tensorlib.reshape(obs_slice, batch_dims + list(p.shape)),
                    v,
                    tensorlib=tensorlib,
                )
        # Repeated space.
        else:
            assert isinstance(prep, RepeatedValuesPreprocessor), prep
            child_size = prep.child_preprocessor.size
            # The list lengths are stored in the first slot of the flat obs.
            lengths = obs[..., 0]
            # [B, ..., 1 + max_len * child_sz] -> [B, ..., max_len, child_sz]
            with_repeat_dim = tensorlib.reshape(
                obs[..., 1:], batch_dims + [space.max_len, child_size]
            )
            # Retry the unpack, dropping the List container space.
            u = _unpack_obs(with_repeat_dim, space.child_space, tensorlib=tensorlib)
            return RepeatedValues(u, lengths=lengths, max_len=prep._obs_space.max_len)
        return u
    else:
        return obs
