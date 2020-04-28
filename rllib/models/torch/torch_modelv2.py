from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils import try_import_torch

_, nn = try_import_torch()


@PublicAPI
class TorchModelV2(ModelV2):
    """Torch version of ModelV2.

    Note that this class by itself is not a valid model unless you
    inherit from nn.Module and implement forward() in a subclass."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        """Initialize a TorchModel.

        Here is an example implementation for a subclass
        ``MyModelClass(TorchModel, nn.Module)``::

            def __init__(self, *args, **kwargs):
                TorchModel.__init__(self, *args, **kwargs)
                nn.Module.__init__(self)
                self._hidden_layers = nn.Sequential(...)
                self._logits = ...
                self._value_branch = ...
        """

        if not isinstance(self, nn.Module):
            raise ValueError("Subclasses of TorchModel must also inherit from "
                             "nn.Module, e.g., MyModel(TorchModel, nn.Module)")

        ModelV2.__init__(
            self,
            obs_space,
            action_space,
            num_outputs,
            model_config,
            name,
            framework="torch")

    @override(ModelV2)
    def variables(self, as_dict=False):
        if as_dict:
            return self.state_dict()
        return list(self.parameters())

    @override(ModelV2)
    def trainable_variables(self, as_dict=False):
        if as_dict:
            return {
                k: v
                for k, v in self.variables(as_dict=True).items()
                if v.requires_grad
            }
        return [v for v in self.variables() if v.requires_grad]
