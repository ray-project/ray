import abc
from typing import Optional, Any, Mapping
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.models.temp_spec_classes import SpecDict, TensorDict
from ray.rllib.models.specs.specs_dict import ModelSpecDict


@ExperimentalAPI
class Module(abc.ABC):
    def __init__(self, name: Optional[str] = None):
        self._name = name or self.__class__.__name__

    @property
    def name(self) -> str:
        """Returns the name of this module."""
        return self._name

    @property
    @abc.abstractmethod
    def fwd_in_spec(self) -> SpecDict:
        """Returns the spec of the input of this module."""

    @property
    @abc.abstractmethod
    def fwd_out_spec(self) -> SpecDict:
        """Returns the spec of the output of this module."""

    def fwd(self, fwd_in: Mapping[str, Any], **kwargs) -> Mapping[str, Any]:
        """Forward pass of this module."""
        if not isinstance(fwd_in, NestedDict):
            fwd_in = NestedDict(fwd_in)
        self.fwd_in_spec.validate(fwd_in)
        # We hide inputs not specified in input_spec to prevent accidental use.
        inputs = fwd_in.filter(self.input_spec)

        # Call the actual forward pass.
        fwd_out = self._fwd(inputs, **kwargs)

        # Validate the output.
        self.fwd_out_spec.validate(fwd_out)
        return fwd_out

    @abc.abstractmethod
    def _fwd(self, fwd_in: NestedDict[Any], **kwargs) -> NestedDict[Any]:
        """The actual forward pass of this module."""


@ExperimentalAPI
class RecurrentModule(Module):
    @property
    def fwd_in_spec(self) -> SpecDict:
        """Returns the spec of the input of this module."""
        return ModelSpecDict(
            {
                "input_dict": self.input_spec,
                "state_dict": self.prev_state_spec,
            }
        )

    @property
    def fwd_out_spec(self) -> SpecDict:
        """Returns the spec of the output of this module."""
        return ModelSpecDict(
            {
                "output_dict": self.output_spec,
                "state_dict": self.next_state_spec,
            }
        )

    @property
    @abc.abstractmethod
    def prev_state_spec(self) -> SpecDict:
        """Returns the spec of the previous state of this module."""

    @property
    @abc.abstractmethod
    def input_spec(self) -> SpecDict:
        """Returns the spec of the input of this module."""

    @property
    @abc.abstractmethod
    def output_spec(self) -> SpecDict:
        """Returns the spec of the output of this module."""

    @property
    @abc.abstractmethod
    def next_state_spec(self) -> SpecDict:
        """Returns the spec of the next state of this module."""

    def initial_state(self) -> TensorDict:
        """Initial state of the component.
        If this component returns a next_state in its unroll function, then
        this function provides the initial state.

        Returns:
            A TensorDict containing the state before the first step.

        Examples:
            >>> state = model.initial_state()
            >>> state # TensorDict(...)
        """
        initial_state = self._initial_state()
        self.next_state_spec.validate(initial_state)
        return initial_state

    @abc.abstractmethod
    def _initial_state(self) -> TensorDict:
        """The initial state of this module."""
