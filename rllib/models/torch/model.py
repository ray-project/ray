import torch
from torch import nn
import tree

from ray.rllib.utils.annotations import (
    DeveloperAPI,
    override,
)
from ray.rllib.models.temp_spec_classes import TensorDict, ModelConfig
from ray.rllib.models.base_model import RecurrentModel, Model, ModelIO
from ray.rllib.utils.deprecation import Deprecated


@Deprecated(error=False)
class TorchModelIO(ModelIO):
    """Save/Load mixin for torch models

    Examples:
        >>> model.save("/tmp/model_weights.cpt")
        >>> model.load("/tmp/model_weights.cpt")
    """

    @DeveloperAPI
    @override(ModelIO)
    def save(self, path: str) -> None:
        """Saves the state dict to the specified path

        Args:
            path: Path on disk the checkpoint is saved to

        """
        torch.save(self.state_dict(), path)

    @DeveloperAPI
    @override(ModelIO)
    def load(self, path: str) -> RecurrentModel:
        """Loads the state dict from the specified path

        Args:
            path: Path on disk to load the checkpoint from
        """
        self.load_state_dict(torch.load(path))


@Deprecated(error=False)
class TorchRecurrentModel(RecurrentModel, nn.Module, TorchModelIO):
    """The base class for recurrent pytorch models.

    If implementing a custom recurrent model, you likely want to inherit
    from this model. You should make sure to call super().__init__(config)
    in your __init__.

    Args:
        config: The config used to construct the model

    Required Attributes:
        input_specs: SpecDict: Denotes the input keys and shapes passed to `unroll`
        output_specs: SpecDict: Denotes the output keys and shapes returned from
            `unroll`
        prev_state_spec: SpecDict: Denotes the keys and shapes for the input
            recurrent states to the model
        next_state_spec: SpecDict: Denotes the keys and shapes for the
            recurrent states output by the model

    Required Overrides:
        # Define unrolling (forward pass) over a sequence of inputs
        _unroll(self, inputs: TensorDict, prev_state: TensorDict, **kwargs)
            -> Tuple[TensorDict, TensorDict]

    Optional Overrides:
        # Define the initial state, if a zero tensor is insufficient
        # the returned TensorDict must match the prev_state_spec
        _initial_state(self) -> TensorDict

        # Additional checks on the input and recurrent state before `_unroll`
        _update_inputs_and_prev_state(inputs: TensorDict, prev_state: TensorDict)
            -> Tuple[TensorDict, TensorDict]

        # Additional checks on the output and the output recurrent state
        # after `_unroll`
        _update_outputs_and_next_state(outputs: TensorDict, next_state: TensorDict)
            -> Tuple[TensorDict, TensorDict]

        # Save model weights to path
        save(self, path: str) -> None

        # Load model weights from path
        load(self, path: str) -> None

    Examples:
        >>> class MyCustomModel(TorchRecurrentModel):
        ...     def __init__(self, config):
        ...         super().__init__(config)
        ...
        ...         self.lstm = nn.LSTM(
        ...             input_size, recurrent_size, batch_first=True
        ...         )
        ...         self.project = nn.Linear(recurrent_size, output_size)
        ...
        ...     @property
        ...     def input_specs(self):
        ...         return SpecDict(
        ...             {"obs": "batch time hidden"}, hidden=self.config.input_size
        ...         )
        ...
        ...     @property
        ...     def output_specs(self):
        ...         return SpecDict(
        ...             {"logits": "batch time logits"}, logits=self.config.output_size
        ...         )
        ...
        ...     @property
        ...     def prev_state_spec(self):
        ...         return SpecDict(
        ...             {"input_state": "batch recur"}, recur=self.config.recurrent_size
        ...         )
        ...
        ...     @property
        ...     def next_state_spec(self):
        ...         return SpecDict(
        ...             {"output_state": "batch recur"},
        ...             recur=self.config.recurrent_size
        ...         )
        ...
        ...     def _unroll(self, inputs, prev_state, **kwargs):
        ...         output, state = self.lstm(inputs["obs"], prev_state["input_state"])
        ...         output = self.project(output)
        ...         return TensorDict(
        ...             {"logits": output}), TensorDict({"output_state": state}
        ...         )

    """

    def __init__(self, config: ModelConfig) -> None:
        RecurrentModel.__init__(self)
        nn.Module.__init__(self)
        TorchModelIO.__init__(self, config)

    @override(RecurrentModel)
    def _initial_state(self) -> TensorDict:
        """Returns the initial recurrent state

        This defaults to all zeros and can be overidden to return
        nonzero tensors.

        Returns:
            A TensorDict that matches the initial_state_spec
        """
        return TensorDict(
            tree.map_structure(
                lambda spec: torch.zeros(spec.shape, dtype=spec.dtype),
                self.initial_state_spec,
            )
        )


@Deprecated(error=False)
class TorchModel(Model, nn.Module, TorchModelIO):
    """The base class for non-recurrent pytorch models.

    If implementing a custom pytorch model, you likely want to
    inherit from this class. You should make sure to call super().__init__(config)
    in your __init__.

    Args:
        config: The config used to construct the model

    Required Attributes:
        input_specs: SpecDict: Denotes the input keys and shapes passed to `_forward`
        output_specs: SpecDict: Denotes the output keys and shapes returned from
            `_forward`

    Required Overrides:
        # Define unrolling (forward pass) over a sequence of inputs
        _forward(self, inputs: TensorDict, **kwargs)
            -> TensorDict

    Optional Overrides:
        # Additional checks on the input before `_forward`
        _update_inputs(inputs: TensorDict) -> TensorDict

        # Additional checks on the output after `_forward`
        _update_outputs(outputs: TensorDict) -> TensorDict

        # Save model weights to path
        save(self, path: str) -> None

        # Load model weights from path
        load(self, path: str) -> None

    Examples:
        >>> class MyCustomModel(TorchModel):
        ...     def __init__(self, config):
        ...         super().__init__(config)
        ...         self.mlp = nn.Sequential(
        ...             nn.Linear(input_size, hidden_size),
        ...             nn.ReLU(),
        ...             nn.Linear(hidden_size, hidden_size),
        ...             nn.ReLU(),
        ...             nn.Linear(hidden_size, output_size)
        ...         )
        ...
        ...     @property
        ...     def input_specs(self):
        ...         return SpecDict(
        ...             {"obs": "batch time hidden"}, hidden=self.config.input_size
        ...         )
        ...
        ...     @property
        ...     def output_specs(self):
        ...         return SpecDict(
        ...             {"logits": "batch time logits"}, logits=self.config.output_size
        ...         )
        ...
        ...     def _forward(self, inputs, **kwargs):
        ...         output = self.mlp(inputs["obs"])
        ...         return TensorDict({"logits": output})

    """

    def __init__(self, config: ModelConfig) -> None:
        Model.__init__(self)
        nn.Module.__init__(self)
        TorchModelIO.__init__(self, config)
