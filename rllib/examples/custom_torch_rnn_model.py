import argparse

import ray
from ray.rllib.examples.cartpole_lstm import CartPoleStatelessEnv
from ray.rllib.examples.custom_keras_rnn_model import RepeatInitialEnv, \
    RepeatAfterMeEnv
from ray.rllib.models.preprocessors import get_preprocessor
from ray.rllib.models.torch.recurrent_torch_model import RecurrentTorchModel
from ray.rllib.models.modelv2 import ModelV2
from ray.rllib.utils.annotations import override
from ray.rllib.utils import try_import_torch
from ray.rllib.models import ModelCatalog
import ray.tune as tune

torch, nn = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--env", type=str, default="repeat_initial")
parser.add_argument("--stop", type=int, default=90)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument("--fc-size", type=int, default=64)
parser.add_argument("--lstm-cell-size", type=int, default=256)


class RNNModel(RecurrentTorchModel):
    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 fc_size=64,
                 lstm_state_size=256):
        super().__init__(obs_space, action_space, num_outputs, model_config,
                         name)

        self.obs_size = get_preprocessor(obs_space)(obs_space).size
        self.fc_size = fc_size
        self.lstm_state_size = lstm_state_size

        # Build the Module from fc + LSTM + 2xfc (action + value outs).
        self.fc1 = nn.Linear(self.obs_size, self.fc_size)
        self.lstm = nn.LSTM(
            self.fc_size, self.lstm_state_size, batch_first=True)
        self.action_branch = nn.Linear(self.lstm_state_size, num_outputs)
        self.value_branch = nn.Linear(self.lstm_state_size, 1)
        # Store the value output to save an extra forward pass.
        self._cur_value = None

    @override(ModelV2)
    def get_initial_state(self):
        # make hidden states on same device as model
        h = [
            self.fc1.weight.new(1, self.lstm_state_size).zero_().squeeze(0),
            self.fc1.weight.new(1, self.lstm_state_size).zero_().squeeze(0)
        ]
        return h

    @override(ModelV2)
    def value_function(self):
        assert self._cur_value is not None, "must call forward() first"
        return self._cur_value

    @override(RecurrentTorchModel)
    def forward_rnn(self, inputs, state, seq_lens):
        """Feeds `inputs` (B x T x ..) through the Gru Unit.

        Returns the resulting outputs as a sequence (B x T x ...).
        Values are stored in self._cur_value in simple (B) shape (where B
        contains both the B and T dims!).

        Returns:
            NN Outputs (B x T x ...) as sequence.
            The state batches as a List of two items (c- and h-states).
        """
        x = nn.functional.relu(self.fc1(inputs))
        lstm_out = self.lstm(
            x, [torch.unsqueeze(state[0], 0),
                torch.unsqueeze(state[1], 0)])
        action_out = self.action_branch(lstm_out[0])
        self._cur_value = torch.reshape(self.value_branch(lstm_out[0]), [-1])
        return action_out, [
            torch.squeeze(lstm_out[1][0], 0),
            torch.squeeze(lstm_out[1][1], 0)
        ]


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None)
    ModelCatalog.register_custom_model("rnn", RNNModel)
    tune.register_env(
        "repeat_initial", lambda _: RepeatInitialEnv(episode_len=100))
    tune.register_env(
        "repeat_after_me", lambda _: RepeatAfterMeEnv({"repeat_delay": 1}))
    tune.register_env("cartpole_stateless", lambda _: CartPoleStatelessEnv())

    config = {
        "env": args.env,
        "use_pytorch": True,
        "num_workers": 0,
        "num_envs_per_worker": 20,
        "gamma": 0.9,
        "entropy_coeff": 0.0001,
        "model": {
            "custom_model": "rnn",
            "max_seq_len": 20,
            "lstm_use_prev_action_reward": "store_true",
            "custom_options": {
                "fc_size": args.fc_size,
                "lstm_state_size": args.lstm_cell_size,
            }
        },
        "lr": 3e-4,
        "num_sgd_iter": 5,
        "vf_loss_coeff": 0.0003,
    }

    tune.run(
        args.run,
        stop={
            "episode_reward_mean": args.stop,
            "timesteps_total": 100000
        },
        config=config,
    )
