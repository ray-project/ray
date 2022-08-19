# Copyright 2021 DeepMind Technologies Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Abstract classes for the RLlib components.

Components are the basic building blocks of the RLModule structure.
They are typically assembled in a sequence, using `SequentialComponent`.
Components implement an `unroll` function, which takes inputs from the current
rollout, and prev_state from the previous rollout. It returns outputs for the
current rollout, and an optional state to be passed to the next rollout.
Inputs and outputs are types.StructDict objects (nested dictionary with
chex.Array as leaves).
The `unroll` function takes two arguments:
  `inputs`: All the tensors corresponding to the current rollout. In a
    sequential component, this contains the observations for this rollout, and
    the outputs of all the previous components in the sequence. Since each input
    is a rollout, all tensors have a first dimension with size `unroll_len`.
  `prev_state`: The set of `state` tensors coming from the previous rollout
    (eg. LSTM state). They only contain the state of the timestep immediately
    before the first timestep of the current rollout (if there is no overlap
    between rollouts, that's the last step of the previous rollout), so unlike
    inputs, they do not have a first dimension with size `unroll_len`.
    Note that this contains the states produced by all the components in the
    previous rollout, including components which appear later in the sequence.
And the `unroll` function returns three arguments:
  `outputs`: The set of tensors this component computes for this rollout, they
    will be passed to the next components in the sequence and returned by the
    SequentialComponent `unroll` function.
  `next_state`: The set of tensors to pass to the next rollout components. All
    the states passed to the next rollouts are aggregated into a single
    types.TensorDict object before being passed to the next rollout.
  `log`: A log_utils.Log object (dictionary of dictionary) containing logs. The
    first level contains the name of the logs, the second contains the type of
    reduce function to apply to the logs (see log_utils).

To implement this function, a subclass must implement `_unroll`, since the
`unroll` function performs additional types checks.

By default, the `prev_state` of the very first rollout contains only zeros. It
can be changed by subclassing the `_initial_state` function.

Components have input and output static typing. This is enforced through 4
properties which must be implemented for each module, returning a types.SpecDict
object (nested dictionaries with specs.Array as leaves):
  `input_spec`: The spec of the `inputs` argument of the `unroll` function. This
    must be a subset of what is passed as `inputs`, and the shapes and data
    types must match. Only inputs specified in the `input_spec` will be visible
    in the `_unroll` function.
  `prev_state_spec`: The spec of the `prev_state` argument of the `unroll`
    function. This must be a subset of what is passed as `prev_state`, and the
    shapes and data types must match. Only inputs specified in the
    `prev_state_spec` will be visible in the `_unroll` function.
  `output_spec`: The spec of the `output` returned by the `_unroll` function.
    The leaves must match exactly, and the shapes and types must match as well.
  `next_state_spec`: The spec of the `next_state` returned by the `_unroll`
    function. The leaves must match exactly, and the shapes and types must match
    as well. Note that since `prev_state` contains the states passed by all the
    components from the previous rollout, `next_state_spec` has no reason to
    match `prev_state_spec`. For instance, one component could use the state
    passed by another component, or pass a state without using it (so that other
    components use it).

For convenience, a `BatchedComponent` class can be used instead of the base
`Component`. Instead of `_unroll`, the subclasses must implement a `_forward`
function. The difference is that the `_forward` function operates on a single
timestep instead of a rollout. This has two consequences:
  * These components do not use `prev_state` and `next_state`.
  * The tensors in the `inputs` argument (and returned in `outputs`) do not have
    the first dimension with size `rollout_len`.
"""

import abc
from typing import Optional, Tuple
import rllib2.models.types as types

ForwardOutputType = Tuple[types.TensorDict]
UnrollOutputType = Tuple[types.TensorDict, types.TensorDict]


class RecurrentModel(abc.ABC):
  """Basic component (see module docstring)."""

  def __init__(self, name: Optional[str] = None):
    self._name = name or "Component"

  @property
  def name(self) -> str:
    return self._name

  @property
  @abc.abstractmethod
  def input_spec(self) -> types.SpecDict:
    """Returns the spec of the input of this module."""

  @property
  @abc.abstractmethod
  def prev_state_spec(self) -> types.SpecDict:
    """Returns the spec of the prev_state of this module."""

  @property
  @abc.abstractmethod
  def output_spec(self) -> types.SpecDict:
    """Returns the spec of the output of this module."""

  @property
  @abc.abstractmethod
  def next_state_spec(self) -> types.SpecDict:
    """Returns the spec of the next_state of this module."""

  @abc.abstractmethod
  def _initial_state(self) -> types.TensorDict:
    """Initial state of the component.

    If this component returns a next_state in its unroll function, then
    this function provides the initial state. By default, we use zeros,
    but it can be overridden for custom initial state.

    Subclasses should override this function instead of `initial_state`, which
    adds additional checks.

    Returns:
      A Dict containing the state before the first step.
    """

  def initial_state(self) -> types.TensorDict:
    """Initial state of the component.

    If this component returns a next_state in its unroll function, then
    this function provides the initial state. By default, we use zeros,
    but it can be overridden for custom initial state.

    Returns:
      A Dict containing the state before the first step.
    """
    initial_state = self._initial_state()
    self.next_state_spec.validate(
        initial_state, error_prefix=f"{self.name} initial_state")
    return initial_state

  @abc.abstractmethod
  def _unroll(self,
              inputs: types.TensorDict,
              prev_state: types.TensorDict) -> UnrollOutputType:
    """Computes the output of the module over unroll_len timesteps.

    Call with a unroll_len=1 for a single step.

    Subclasses should override this function instead of `unroll`, which
    adds additional checks.

    Args:
      inputs: A TensorDict containing [unroll_len, ...] tensors.
      prev_state: A TensorDict containing [...] tensors, containing the
        next_state of the last timestep of the previous unroll.

    Returns:
      outputs: A TensorDict containing [unroll_len, ...] tensors.
      next_state: A dict containing [...] tensors representing the
        state to be passed as the first state of the next rollout.
        If overlap_len is 0, this is the last state of this rollout.
        More generally, this is the (unroll_len - overlap_len)-th state.
      logs: A dict containing [unroll_len] tensors to be logged.
    """

  def unroll(self,
             inputs: types.TensorDict,
             prev_state: types.TensorDict) -> UnrollOutputType:
    """Computes the output of the module over unroll_len timesteps.

    Call with a unroll_len=1 for a single step.

    Args:
      inputs: A TensorDict containing [unroll_len, ...] tensors.
      prev_state: A TensorDict containing [...] tensors, containing the
        next_state of the last timestep of the previous unroll.

    Returns:
      outputs: A TensorDict containing [unroll_len, ...] tensors.
      next_state: A dict containing [...] tensors representing the
        state to be passed as the first state of the next rollout.
        If overlap_len is 0, this is the last state of this rollout.
        More generally, this is the (unroll_len - overlap_len)-th state.
      logs: A dict containing [unroll_len] tensors to be logged.
    """
    # if inputs:
    #   try:
    #     chex.assert_equal_shape(jax.tree_leaves(inputs), dims=0)
    #   except AssertionError as e:
    #     raise AssertionError(f"{self.name}: {e}") from e
    self.input_spec.validate(inputs,
                             num_leading_dims_to_ignore=1,
                             error_prefix=f"{self.name} inputs")
    self.prev_state_spec.validate(prev_state,
                                  error_prefix=f"{self.name} prev_state")
    # We hide inputs not specified in input_spec to prevent accidental use.
    inputs = inputs.filter(self.input_spec)
    prev_state = prev_state.filter(self.prev_state_spec)
    # with hk.experimental.name_scope(self.name):
    outputs, next_state, logs = self._unroll(inputs, prev_state)
    self.output_spec.validate(outputs,
                              num_leading_dims_to_ignore=1,
                              error_prefix=f"{self.name} outputs")
    self.next_state_spec.validate(next_state,
                                  error_prefix=f"{self.name} next_state")
    return outputs, next_state, logs

class Model(RecurrentModel):
  """A Component which is not using the unroll dimension.

  This is a helper module to write simpler components.
  Such a component computes a function _forward such that
  unroll(x)[t] = _forward(x[t]) where t=0..unroll_len-1.

  Such a module must be stateless.
  """

  @property
  def prev_state_spec(self) -> types.SpecDict:
    return types.SpecDict()

  @property
  def next_state_spec(self) -> types.SpecDict:
    return types.SpecDict()

  def _unroll(self,
              inputs: types.TensorDict,
              prev_state: types.TensorDict) -> UnrollOutputType:
    del prev_state
    outputs, logs = self._forward(inputs)
    return outputs, types.TensorDict(), logs

  @abc.abstractmethod
  def _forward(self, inputs: types.TensorDict) -> ForwardOutputType:
    """Computes the output of this module for each timestep.

    Args:
      inputs: A TensorDict containing [...] tensors.

    Returns:
      outputs: A TensorDict containing [...] tensors.
      logs: A dict containing [...] tensors to be logged.
    """

