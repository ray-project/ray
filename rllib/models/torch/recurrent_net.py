from gym.spaces import Box
import numpy as np

from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.models.torch.misc import SlimFC
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.rnn_sequencing import add_time_dimension
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.trajectory_view import ViewRequirement
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


@DeveloperAPI
class RecurrentNetwork(TorchModelV2):
    """Helper class to simplify implementing RNN models with TorchModelV2.

    Instead of implementing forward(), you can implement forward_rnn() which
    takes batches with the time dimension added already.

    Here is an example implementation for a subclass
    ``MyRNNClass(RecurrentNetwork, nn.Module)``::

        def __init__(self, obs_space, num_outputs):
            nn.Module.__init__(self)
            super().__init__(obs_space, action_space, num_outputs,
                             model_config, name)
            self.obs_size = _get_size(obs_space)
            self.rnn_hidden_dim = model_config["lstm_cell_size"]
            self.fc1 = nn.Linear(self.obs_size, self.rnn_hidden_dim)
            self.rnn = nn.GRUCell(self.rnn_hidden_dim, self.rnn_hidden_dim)
            self.fc2 = nn.Linear(self.rnn_hidden_dim, num_outputs)

            self.value_branch = nn.Linear(self.rnn_hidden_dim, 1)
            self._cur_value = None

        @override(ModelV2)
        def get_initial_state(self):
            # Place hidden states on same device as model.
            h = [self.fc1.weight.new(
                1, self.rnn_hidden_dim).zero_().squeeze(0)]
            return h

        @override(ModelV2)
        def value_function(self):
            assert self._cur_value is not None, "must call forward() first"
            return self._cur_value

        @override(RecurrentNetwork)
        def forward_rnn(self, input_dict, state, seq_lens):
            x = nn.functional.relu(self.fc1(input_dict["obs_flat"].float()))
            h_in = state[0].reshape(-1, self.rnn_hidden_dim)
            h = self.rnn(x, h_in)
            q = self.fc2(h)
            self._cur_value = self.value_branch(h).squeeze(1)
            return q, [h]
    """

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


class LSTMWrapper(RecurrentNetwork, nn.Module):
    """An LSTM wrapper serving as an interface for ModelV2s that set use_lstm.
    """

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):

        nn.Module.__init__(self)
        super().__init__(obs_space, action_space, None, model_config, name)

        self.cell_size = model_config["lstm_cell_size"]
        self.use_prev_action_reward = model_config[
            "lstm_use_prev_action_reward"]
        self.action_dim = int(np.product(action_space.shape))
        # Add prev-action/reward nodes to input to LSTM.
        if self.use_prev_action_reward:
            self.num_outputs += 1 + self.action_dim
        self.lstm = nn.LSTM(self.num_outputs, self.cell_size, batch_first=True)

        self.num_outputs = num_outputs

        # Postprocess LSTM output with another hidden layer and compute values.
        self._logits_branch = SlimFC(
            in_size=self.cell_size,
            out_size=self.num_outputs,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_)
        self._value_branch = SlimFC(
            in_size=self.cell_size,
            out_size=1,
            activation_fn=None,
            initializer=torch.nn.init.xavier_uniform_)

        initial_state = self.get_initial_state()
        self._trajectory_view = {
            SampleBatch.OBS: ViewRequirement(timesteps=-1),
            "state_in_0": ViewRequirement("state_out_0", fill="zeros", space=Box(-1.0, 1.0, shape=(self.cell_size, )), timesteps=-1),
            "state_in_1": ViewRequirement("state_out_1", fill="zeros", space=Box(-1.0, 1.0, shape=(self.cell_size, )), timesteps=-1),
            #"seq_lens": ViewRequirement(fill="zeros",
            #    space=Box(-1.0, 1.0, shape=(self.cell_size,)), timesteps=-1),
            SampleBatch.PREV_ACTIONS: ViewRequirement(SampleBatch.ACTIONS, timesteps=-1),
            SampleBatch.PREV_REWARDS: ViewRequirement(SampleBatch.REWARDS, timesteps=-1),
        }
        """
        1) Normal case:
        - sample_collection_mode: T-shuffled
            SampleBatch.CUR_OBS:
                postprocessing: false
            SampleBatch.NEXT_OBS:
                data_col: SampleBatch.CUR_OBS
                shift: 1
                sampling: false  # next obs only needed for postprocessing (at least in PPO)
                training: false
        Implementation details:
            - Trajectory.get_sample_batch: Produce SampleBatch with special NEXT_OBS view using same buffer (not part of "regular" SampleBatch's dict) as CUR_OBS.
            - PolicyTrajectories.add_sample_batch: Copy NEXT_OBS data from SampleBatch CUR_OBS (if required for training, otherwise: no)
            - PolicyTrajectories.get_sample_batch: Produce SampleBatch from columns required for training (here: only OBS), shuffle.

        2) Prev-a/r case:
        - sample_collection_mode: T-shuffled
            [same as above] ... +
            SampleBatch.PREV_ACTIONS:
                data_col: SampleBatch.ACTIONS
                shift: -1
            SampleBatch.PREV_REWARDS:
                data_col: SampleBatch.REWARDS
                shift: -1
        Implementation details:
            - Trajectory.get_sample_batch: Produce SampleBatch same way as above + special PREV_ACTION/REWARD view using same buffer as ACTIONS/REWARDS. 
            - PolicyTrajectories.add_sample_batch: same as above (copy iff PREV_ACTIONS/REWARDS needed for training (usually yes), otherwise: no).
            - PolicyTrajectories.get_sample_batch: Produce SampleBatch from columns required for training, shuffle.

        3) LSTM case (iff use_lstm=True -> our default LSTM wrapper):
        - sample_collection_mode: TxB  # <- switch on time-major collection mode (if model is batch major: BxT)
            [same as above] ... +
            state_in_0:
                data_col: state_out_0
                shift: -1
                postprocessing: false
                training: first  # only include first state (time=0, no time dim)
            state_in_1:
                data_col: state_out_1
                shift: -1
                postprocessing: false
                training: first  # only include first state (time=0, no time dim)
        Implementation details:
            - Trajectory.get_sample_batch: Produce SampleBatch same way as above (states are not included in postprocessing so we don't worry about them here).
            - PolicyTrajectories.add_sample_batch: same as above (copy iff PREV_ACTIONS/REWARDS needed for training (usually yes), otherwise: no).
            - PolicyTrajectories.get_sample_batch: Produce SampleBatch from columns required for training, shuffle.

        4) Atari frame-stacking case (no LSTM, no prev-a/r):
        - sample_collection_mode: T-shuffled
            SampleBatch.CUR_OBS:
                shift: [-3, -2, -1, 0]  # always pack the last 4 obs into one obs
                postprocessing: false
            SampleBatch.NEXT_OBS:
                data_col: SampleBatch.CUR_OBS
                shift: [-2, -1, 0, 1]
                sampling: false  # next obs only needed for postprocessing (at least in PPO)
                training: false
                postprocessing: true
        Implementation details:
            - Trajectory.get_sample_batch: Gathers single 84x84 frames into its "obs" special buffer and creates a packs the last 4 under the view key CUR_OBS/NEXT_OBS for the view (see below on how to do this for np and torch).
            - PolicyTrajectories.add_sample_batch: Again, will have to copy NEXT_OBS if required from here on (training). Otherwise, keep working with simple obs views to get the 4x-stacking effect.
            - PolicyTrajectories.get_sample_batch: 
            >>> a = np.array([0, 1, 2, 3, 4, 5])
            >>> a
            array([0, 1, 2, 3, 4, 5])
            >>> b = np.lib.stride_tricks.as_strided(a, (4, 3), (a.itemsize, a.itemsize))  # NOTE: torch.as_strided works similarly
            >>> b
            array([[0, 1, 2],
                   [1, 2, 3],
                   [2, 3, 4],
                   [3, 4, 5]])
            >>> a[2] = 555
            >>> b
            array([[  0,   1, 555],
                   [  1, 555,   3],
                   [555,   3,   4],
                   [  3,   4,   5]])

        5) Attention Nets:
        - sample_collection_mode: BxT
            SampleBatch.CUR_OBS:
                postprocessing: false
            SampleBatch.NEXT_OBS:
                data_col: SampleBatch.CUR_OBS
                shift: 1
                sampling: false  # next obs only needed for postprocessing (at least in PPO)
                training: false
            state_in_0:  # <- memory states (as many as transformer units)
                data_col: state_out_0
                shift: -1
                postprocessing: false
            state_in_...:
                data_col: state_out_1
                shift: -1
                postprocessing: false
        Implementation details:
            - Change attention net to cache previous obs mappings (e.g. first Dense layer' (or even other succeeding ones) output can be cached), such that only the current observation has to be passed in each time step during sampling.
            - Unlike for RNNs, all reviously calculated states/memory values are fed back in for training (as BxT).
            - Trajectory.get_sample_batch: Produce SampleBatch same way as above (states are not included in postprocessing so we don't worry about them here).
                Also collect all intermediary memory (state) outputs. Because we do have time-slicing, we don't have to copy into new state_in fields here, but can use views.
            - PolicyTrajectories.add_sample_batch: same as RNN (copy iff PREV_ACTIONS/REWARDS needed for training (usually yes), otherwise: no).
            - PolicyTrajectories.get_sample_batch: Produce SampleBatch from columns required for training.

        6) Multi-agent communication.

        Changes required:
        Trajectory class:
        - needs to check "max-shift" and allocate respective slots at beginning/end of lists to put zero-padding/state-init/prev-action/etc.. in there.
        - these extra slots will then also move into the SampleBatch when constructed (as larger-than-count np.arrays) and used for the different views (e.g. PREV_ACTION, NEXT_OBS).
        SampleBatch:
        - needs to be able to take extra slots at beginning and end of some buffer col and create views in the "main" dict into these (e.g. NEXT_OBS, PREV_ACTION).
        - NOTE: this is only possible for single-trajectory SampleBatches (create new flag: is_single_trajectory). Good for postprocessing on a single trajectory.
        PolicyTrajectories:
        - Will already build the buffers in a way that serves the final SampleBatch for training:
            - already zero-pad for LSTMs (max_seq_len chunks)
            - already pre-allocate everything in time-major fashion for LSTMs
            - already determine seq_lens and collect initial state inputs (for each seq chunk)

        All options explained:
        ----------------------
        sample_collection_mode:
            T: Time axis only (current behavior of SampleBatchBuilder/SampleBatch)
            T-shuffle: Time axis only; final SampleBatch is shuffled (order on time axis does not matter anymore -> basically a batch)
            TxB: Time-major (max_seq_len) + batch dimension used for assembling batches for LSTMs/RNNs.
            BxT: Batch-major + time dimension (max_seq_len) used for assembling batches for LSTMs/RNNs that don't support time-major.

        data_col:
        """

    @override(RecurrentNetwork)
    def forward(self, input_dict, state=None, seq_lens=None):
        # Push obs through "unwrapped" net's `forward()` first.
        wrapped_out, _ = self._wrapped_forward(input_dict, [], None)

        # Concat. prev-action/reward if required.
        if self.model_config["lstm_use_prev_action_reward"]:
            #bla
            print(end="")
            wrapped_out = torch.cat(
                [
                    wrapped_out,
                    torch.reshape(input_dict[SampleBatch.PREV_ACTIONS].float(),
                                  [-1, self.action_dim]),
                    torch.reshape(input_dict[SampleBatch.PREV_REWARDS],
                                  [-1, 1]),
                ],
                dim=1)

        # Then through our LSTM.
        input_dict["obs_flat"] = wrapped_out
        return super().forward(input_dict, state, seq_lens)

    @override(RecurrentNetwork)
    def forward_rnn(self, inputs, state, seq_lens):
        self._features, [h, c] = self.lstm(
            inputs,
            [torch.unsqueeze(state[0], 0),
             torch.unsqueeze(state[1], 0)])
        model_out = self._logits_branch(self._features)
        return model_out, [torch.squeeze(h, 0), torch.squeeze(c, 0)]

    @override(ModelV2)
    def get_initial_state(self):
        # Place hidden states on same device as model.
        linear = next(self._logits_branch._model.children())
        h = [
            linear.weight.new(1, self.cell_size).zero_().squeeze(0),
            linear.weight.new(1, self.cell_size).zero_().squeeze(0)
        ]
        return h

    @override(ModelV2)
    def value_function(self):
        assert self._features is not None, "must call forward() first"
        return torch.reshape(self._value_branch(self._features), [-1])
