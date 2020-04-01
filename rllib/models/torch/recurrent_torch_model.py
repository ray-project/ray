import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


@DeveloperAPI
class RecurrentTorchModel(TorchModelV2, nn.Module):
    """Helper class to simplify implementing RNN models with TFModelV2.

    Instead of implementing forward(), you can implement forward_rnn() which
    takes batches with the time dimension added already.

    Here is an example implementation for a subclass
    ``MyRNNClass(nn.Module, RecurrentTorchModel)``::

        def __init__(self, obs_space, num_outputs):
            self.obs_size = _get_size(obs_space)
            self.rnn_hidden_dim = model_config["lstm_cell_size"]
            self.fc1 = nn.Linear(self.obs_size, self.rnn_hidden_dim)
            self.rnn = nn.GRUCell(self.rnn_hidden_dim, self.rnn_hidden_dim)
            self.fc2 = nn.Linear(self.rnn_hidden_dim, num_outputs)

            self.value_branch = nn.Linear(self.rnn_hidden_dim, 1)
            self._cur_value = None

        @override(ModelV2)
        def get_initial_state(self):
            # make hidden states on same device as model
            h = [self.fc1.weight.new(
                1, self.rnn_hidden_dim).zero_().squeeze(0)]
            return h

        @override(ModelV2)
        def value_function(self):
            assert self._cur_value is not None, "must call forward() first"
            return self._cur_value

        @override(RecurrentTorchModel)
        def forward_rnn(self, input_dict, state, seq_lens):
            x = nn.functional.relu(self.fc1(input_dict["obs_flat"].float()))
            h_in = state[0].reshape(-1, self.rnn_hidden_dim)
            h = self.rnn(x, h_in)
            q = self.fc2(h)
            self._cur_value = self.value_branch(h).squeeze(1)
            return q, [h]
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

    @override(ModelV2)
    def forward(self, input_dict, state, seq_lens):
        """Adds time dimension to batch before sending inputs to forward_rnn().

        You should implement forward_rnn() in your subclass."""
        if isinstance(seq_lens, np.ndarray):
            seq_lens = torch.Tensor(seq_lens).int()
        output, new_state = self.forward_rnn(
            add_time_dimension(
                input_dict["obs_flat"].float(), seq_lens, framework="torch"),
            state, seq_lens)
        return torch.reshape(output, [-1, self.num_outputs]), new_state

    def forward_rnn(self, inputs, state, seq_lens):
        """Call the model with the given input tensors and state.

        Args:
            inputs (dict): Observation tensor with shape [B, T, obs_size].
            state (list): List of state tensors, each with shape [B, size].
            seq_lens (Tensor): 1D tensor holding input sequence lengths.
                Note: len(seq_lens) == B.

        Returns:
            (outputs, new_state): The model output tensor of shape
                [B, T, num_outputs] and the list of new state tensors each with
                shape [B, size].

        Examples:
            def forward_rnn(self, inputs, state, seq_lens):
                model_out, h, c = self.rnn_model([inputs, seq_lens] + state)
                return model_out, [h, c]
        """
        raise NotImplementedError("You must implement this for an RNN model")
