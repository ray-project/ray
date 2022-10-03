"""Model base classes.

RLlib models all inherit from the recurrent base class, which can be chained together
with other models.

The models input and output TensorDicts. Which keys the models read/write to
and the desired tensor shapes must be defined in input_spec, output_spec,
prev_state_spec, and next_state_spec.

The `unroll` function gets the model inputs and previous recurrent state, and outputs
the model outputs and next recurrent state. Note all ins/outs must match the specs. 
Users should override `_unroll` rather than `unroll`.

`initial_state` returns the "next" state for the first recurrent iteration. Again,
users should override `_initial_state` instead.

For non-recurrent models, users may instead override `_forward` which does not
make use of recurrent states.
"""


import abc
from typing import Optional, Tuple, Any


# TODO: Remove once TensorDict is in master
class TensorDict:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def filter(self, specs):
        return {k for k in self.kwargs if k in specs.kwargs}

    def __eq__(self, other: "TensorDict"):
        return True


# TODO: Remove once SpecDict is in master
class SpecDict:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        pass

    def validate(self, spec: Any) -> bool:
        return True


# TODO: Remove once ModelConfig is in master
class ModelConfig:
    pass


ForwardOutputType = TensorDict
# [Output, Recurrent State(s)]
UnrollOutputType = Tuple[TensorDict, TensorDict]


class RecurrentModel(abc.ABC):
    """The base model all other models are based on."""

    def __init__(self, name: Optional[str] = None):
        self._name = name or self.__class__.__name__

    @property
    def name(self) -> str:
        return self._name

    # TODO: Should these be properties or functions? How do we know the
    # input_spec before init? Won't our model change shapes depending
    # on arguments passed to init?
    @property
    @abc.abstractmethod
    def input_spec(self) -> SpecDict:
        """Returns the spec of the input of this module."""

    @property
    @abc.abstractmethod
    def prev_state_spec(self) -> SpecDict:
        """Returns the spec of the prev_state of this module."""

    @property
    @abc.abstractmethod
    def output_spec(self) -> SpecDict:
        """Returns the spec of the output of this module."""

    @property
    @abc.abstractmethod
    def next_state_spec(self) -> SpecDict:
        """Returns the spec of the next_state of this module."""

    @abc.abstractmethod
    def _initial_state(self) -> TensorDict:
        """Initial state of the component.
        If this component returns a next_state in its unroll function, then
        this function provides the initial state.
        Subclasses should override this function instead of `initial_state`, which
        adds additional checks.

        Returns:
          A TensorDict containing the state before the first step.
        """

    def initial_state(self) -> TensorDict:
        """Initial state of the component.
        If this component returns a next_state in its unroll function, then
        this function provides the initial state.

        Returns:
          A TensorDict containing the state before the first step.
        """
        initial_state = self._initial_state()
        self.next_state_spec.validate(initial_state)
        return initial_state

    @abc.abstractmethod
    def _unroll(
        self, inputs: TensorDict, prev_state: TensorDict, **kwargs
    ) -> UnrollOutputType:
        """Computes the output of the module over unroll_len timesteps.
        Subclasses should override this function instead of `unroll`, which
        adds additional checks.

        Args:
          inputs: A TensorDict of inputs
          prev_state: A TensorDict containing the next_state of the last
            timestep of the previous unroll.
        Returns:
          outputs: A TensorDict of outputs
          next_state: A dict containing the state to be passed
            as the first state of the next rollout.
        """

    def unroll(
        self, inputs: TensorDict, prev_state: TensorDict, **kwargs
    ) -> UnrollOutputType:
        """Computes the output of the module over unroll_len timesteps.

        Args:
          inputs: A TensorDict containing inputs to the model
          prev_state: A TensorDict containing containing the
            next_state of the last timestep of the previous unroll.
        Returns:
          outputs: A TensorDict containing model outputs
          next_state: A TensorDict containing the
            state to be passed as the first state of the next rollout.
        """
        self.input_spec.validate(inputs)
        self.prev_state_spec.validate(prev_state)
        # We hide inputs not specified in input_spec to prevent accidental use.
        inputs = inputs.filter(self.input_spec)
        prev_state = prev_state.filter(self.prev_state_spec)
        inputs, prev_state = self._check_inputs_and_prev_state(inputs, prev_state)
        outputs, next_state = self._unroll(inputs, prev_state, **kwargs)
        self.output_spec.validate(outputs)
        self.next_state_spec.validate(next_state)
        outputs, next_state = self._check_outputs_and_next_state(outputs, next_state)
        return outputs, next_state

    def _check_inputs_and_prev_state(
        self, inputs: TensorDict, prev_state: TensorDict
    ) -> Tuple[TensorDict, TensorDict]:
        """Override this function to add additional checks on inputs."""
        return inputs, prev_state

    def _check_outputs_and_next_state(
        self, outputs: TensorDict, next_state: TensorDict
    ) -> Tuple[TensorDict, TensorDict]:
        """Override this function to add additional checks on outputs."""
        return outputs, next_state


class Model(RecurrentModel):
    """A Component which is not using the unroll dimension.
    This is a helper module to write simpler components.
    Such a component computes a function _forward such that
    unroll(x)[t] = _forward(x[t]) where t=0..unroll_len-1.
    Such a module must be stateless.
    """

    @property
    def prev_state_spec(self) -> SpecDict:
        return SpecDict()

    @property
    def next_state_spec(self) -> SpecDict:
        return SpecDict()

    def _initial_state(self) -> TensorDict:
        return TensorDict()

    def _check_inputs_and_prev_state(
        self, inputs: TensorDict, prev_state: TensorDict
    ) -> Tuple[TensorDict, TensorDict]:
        inputs = self._check_inputs(inputs)
        return inputs, prev_state

    def _check_inputs(self, inputs: TensorDict) -> TensorDict:
        """Override this function to add additional checks on inputs."""
        return inputs

    def _check_outputs_and_next_state(
        self, outputs: TensorDict, next_state: TensorDict
    ) -> Tuple[TensorDict, TensorDict]:
        outputs = self._check_outputs(outputs)
        return outputs, next_state

    def _check_outputs(self, outputs: TensorDict) -> TensorDict:
        """Override this function to add additional checks on outputs."""
        return outputs

    def _unroll(
        self, inputs: TensorDict, prev_state: TensorDict, **kwargs
    ) -> UnrollOutputType:
        del prev_state
        outputs = self._forward(inputs, **kwargs)
        return outputs, TensorDict()

    @abc.abstractmethod
    def _forward(self, inputs: TensorDict, **kwargs) -> ForwardOutputType:
        """Computes the output of this module for each timestep.
        Args:
          inputs: A TensorDict.
        Returns:
          outputs: A TensorDict.
        """


class ModelIO(abc.ABC):
    def __init__(self, config: ModelConfig) -> None:
        self._config = config

    @property
    def config(self) -> ModelConfig:
        return self._config

    @abc.abstractmethod
    def save(self, path: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def load(self, path: str) -> RecurrentModel:
        raise NotImplementedError
